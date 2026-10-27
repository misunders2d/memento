//! Legacy control versus the production Cold-storage lifecycle; no capacity claim.
use super::*;
use qdrant_edge::{PayloadConfig, SegmentState};

const POINTS: u64 = 4096;
const FIELDS: [(&str, PayloadSchemaType); 11] = [
    ("kind", PayloadSchemaType::Keyword), ("type", PayloadSchemaType::Keyword),
    ("current", PayloadSchemaType::Bool), ("ordinary", PayloadSchemaType::Bool),
    ("automatic", PayloadSchemaType::Bool), ("historical", PayloadSchemaType::Bool),
    ("raw_fallback", PayloadSchemaType::Bool), ("parked_candidate", PayloadSchemaType::Bool),
    ("entity", PayloadSchemaType::Bool), ("personal", PayloadSchemaType::Bool),
    ("professional", PayloadSchemaType::Bool),
];

fn legacy_config() -> EdgeConfig {
    EdgeConfigBuilder::new()
        .vector(DEFAULT_VECTOR_NAME, EdgeVectorParamsBuilder::new(768, Distance::Cosine)
            .memory(Memory::Cold).datatype(VectorStorageDatatype::Float32).build())
        .payload_memory(Memory::Cold).id_tracker_memory(Memory::Cold)
        .hnsw_config(HnswIndexConfig {
            m:0,payload_m:Some(0),ef_construct:200,full_scan_threshold:10000,
            max_indexing_threads:1,memory:Some(Memory::Cold),..Default::default()
        })
        .optimizers(EdgeOptimizersConfig {
            indexing_threshold:Some(0),default_segment_number:Some(2),
            prevent_unoptimized:Some(false),..Default::default()
        })
        .max_search_threads(1).wal_enabled(true)
        .wal_options(WalOptions {segment_capacity:1024*1024,..Default::default()}).build()
}

fn field_indices(shard: &EdgeShard, cold: bool) {
    if cold { indices(shard).unwrap(); return; }
    // Freeze legacy FieldType schemas; production now requests explicit Cold.
    for (name, kind) in FIELDS {
        shard.update(UpdateOperation::FieldIndexOperation(FieldIndexOperations::CreateIndex(CreateIndex {
            field_name:name.parse().unwrap(),field_schema:Some(PayloadFieldSchema::FieldType(kind)),
        }))).unwrap();
    }
}

fn point(id: u64, updated: bool) -> PointStructPersisted {
    let cosine = if updated && id == POINTS - 1 { 0.99 } else { 0.1 + id as f32 * 0.0002 };
    let mut vector = vec![0.; 768];
    vector[0] = cosine; vector[1] = (1. - cosine * cosine).sqrt();
    let payload = json!({"target":format!("fixture-{id}"),"version":if updated {2}else{1},
        "kind":if id%8==3 {"candidate"}else{"node"},"type":"knowledge",
        "current":id%8!=0 && !(updated && id==POINTS-2),
        // A false ordinary flag represents an excluded/forgotten pool member here;
        // the existing SQL eligibility tests, not this Edge test, prove its derivation.
        "ordinary":id%8!=1,"automatic":id%8!=4,"personal":id%8!=2,
        "professional":id%8==2,"historical":false,"raw_fallback":false,
        "parked_candidate":false,"entity":false});
    PointStructPersisted {id:PointId::NumId(id), vector:VectorStructPersisted::Single(vector),
        payload:Some(Payload::from(payload.as_object().unwrap().clone()))}
}

fn put(shard: &EdgeShard, points: Vec<PointStructPersisted>) {
    shard.update(UpdateOperation::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperations::PointsList(points)))).unwrap();
}

fn answers(shard: &EdgeShard, through: u64, updated: bool) -> Vec<Vec<(i64,u32)>> {
    let view = SearchView {hot:Some(shard),cold:None,deadline:Instant::now()+Duration::from_secs(20)};
    let mut query=[0.;768]; query[0]=1.;
    [(Pool::Ordinary,"personal"),(Pool::Ordinary,"professional"),(Pool::Automatic,"personal")]
        .into_iter().map(|(pool,scope)| {
            let hits=view.search(Tier::Hot,&query,16,"node",pool,Some(scope),Some("knowledge")).unwrap();
            let mut expected:Vec<_>=(1..=through).filter(|id| {
                id%8!=0 && id%8!=3 && !(updated && *id==POINTS-2)
                    && (if matches!(pool,Pool::Ordinary) {id%8!=1}else{id%8!=4})
                    && (if scope=="personal" {id%8!=2}else{id%8==2})
            }).collect();
            expected.sort_by_key(|id| std::cmp::Reverse(if updated && *id==POINTS-1 {POINTS+1}else{*id}));
            expected.truncate(16);
            assert_eq!(hits.iter().map(|h|h.seq as u64).collect::<Vec<_>>(),expected,
                "filtered exact IDs changed, scope={scope}");
            for hit in &hits {
                let expected=if updated && hit.seq as u64==POINTS-1 {0.99}else{0.1+hit.seq as f32*0.0002};
                assert!((hit.cosine-expected).abs()<1e-6,"FP32 cosine changed");
            }
            hits.into_iter().map(|h|(h.seq,h.cosine.to_bits())).collect()
        }).collect()
}

fn storage_evidence(path: &Path, shard: &EdgeShard, candidate: bool) -> usize {
    shard.flush().unwrap();
    let info=shard.info().unwrap();
    let mut segment_count=0; let mut converted=0;
    for entry in std::fs::read_dir(path.join("segments")).unwrap() {
        let path=entry.unwrap().path();
        let name=path.file_name().unwrap().to_string_lossy();
        if !path.is_dir() || name.starts_with('.') || name.ends_with(".deleted") || !path.join("version.info").is_file() {continue;}
        segment_count+=1;
        let state:SegmentState=serde_json::from_slice(&std::fs::read(path.join("segment.json")).unwrap()).unwrap();
        let vector=&state.config.vector_data[DEFAULT_VECTOR_NAME];
        let value=serde_json::to_value(vector).unwrap();
        assert_eq!(value["datatype"],"float32");
        assert!(vector.quantization_config.is_none());
        let fields=PayloadConfig::load(&path.join("payload_index/config.json")).unwrap();
        for (_,schema) in fields.indices.to_schemas() {
            assert_eq!(schema.expand().memory_placement(),if candidate {Memory::Cold}else{Memory::Pinned});
        }
        if state.config.is_appendable() {
            assert_eq!(value["index"]["type"],"plain");
            assert_eq!(value["storage_type"],"ChunkedMmap");
        } else {
            assert!(candidate,"threshold0 control unexpectedly became nonappendable");
            converted+=1;
            assert_eq!(value["storage_type"],"Mmap","actual Cold single-file conversion missing");
            assert_eq!(value["index"]["type"],"hnsw");
            assert_eq!(value["index"]["options"]["m"],0);
            assert_eq!(value["index"]["options"]["payload_m"],0);
            // Read the built index's persisted graph parameters, not just requested config.
            let graph:serde_json::Value=serde_json::from_slice(&std::fs::read(path.join("vector_index/hnsw_config.json")).unwrap()).unwrap();
            for field in ["m","m0","payload_m","payload_m0"] {assert_eq!(graph[field],0,"graph condition {field}");}
            let actual=serde_json::to_value(&fields).unwrap();
            // Pinned vendor Bool uses RoaringFlags<MmapFile> but ignores requested
            // Cold placement: persisted Heap layout resolves to Pinned. Assert that
            // limitation explicitly; this is not proof of bounded total RAM.
            for (field,kind,on_disk,memory) in [
                ("kind","keyword_index",true,Memory::Cold),
                ("current","bool_index",false,Memory::Pinned),
            ] {
                assert!(actual["indexed_types"][field].as_array().unwrap().iter().any(|v|
                    v["index_type"]==kind && v["mutability"]=="immutable"
                    && v["storage_type"]["mmap"]["is_on_disk"]==on_disk),
                    "actual immutable field representation changed for {field}");
                let key:qdrant_edge::JsonPath=field.parse().unwrap();
                assert!(fields.indices.get(&key).unwrap().types.iter().any(|index|
                    serde_json::to_value(index).unwrap()["index_type"]==kind
                    && index.storage_type.immutable_memory(Memory::Cold)==Some(memory)),
                    "actual field placement changed for {field}");
            }
        }
    }
    assert_eq!(segment_count,info.segments_count,"metadata must describe exactly live segments");
    if !candidate {assert_eq!(converted,0);}
    converted
}

#[test]
fn exact_m0_vendor_cold_conversion_preserves_filtered_updates_and_reopen() {
    let dir=tempfile::tempdir().unwrap();
    let control_path=dir.path().join("control"); let candidate_path=dir.path().join("candidate");
    let control=EdgeShard::new(&control_path,legacy_config()).unwrap();
    let candidate_config=config();
    let optimizers=candidate_config.optimizers.as_ref().unwrap();
    assert_eq!(optimizers.indexing_threshold,Some(10_000));
    assert_eq!(optimizers.max_segment_size,Some(256_000));
    assert_eq!(optimizers.get_indexing_threshold_kb(),10_000);
    assert_eq!(optimizers.get_max_segment_size_kb(1),256_000);
    let candidate=EdgeShard::new(&candidate_path,candidate_config).unwrap();
    field_indices(&control,false); field_indices(&candidate,true);
    assert!(POINTS*768*4>10_000*1024,"fixture must cross real vendor threshold");
    let mut did_optimize=false;
    for start in (1..=POINTS).step_by(BATCH) {
        let through=(start+BATCH as u64-1).min(POINTS);
        for shard in [&control,&candidate] {put(shard,(start..=through).map(|id|point(id,false)).collect());}
        let before=answers(&control,through,false);
        assert_eq!(answers(&candidate,through,false),before);
        control.optimize_with_budget(ResourceBudget::new(1,1)).unwrap();
        did_optimize |= candidate.optimize_with_budget(ResourceBudget::new(1,1)).unwrap();
        assert_eq!(answers(&control,through,false),before);
        assert_eq!(answers(&candidate,through,false),before);
    }
    assert!(did_optimize,"public optimizer never converted the candidate");
    storage_evidence(&control_path,&control,false);
    assert!(storage_evidence(&candidate_path,&candidate,true)>0,"no actual nonappendable Cold segment");
    for shard in [&control,&candidate] {put(shard,vec![point(POINTS-1,true),point(POINTS-2,true)]);}
    let updated=answers(&control,POINTS,true);
    assert_eq!(answers(&candidate,POINTS,true),updated);
    for shard in [&control,&candidate] {shard.optimize_with_budget(ResourceBudget::new(1,1)).unwrap();shard.flush().unwrap();}
    assert_eq!(answers(&candidate,POINTS,true),updated);
    drop(control);drop(candidate);
    let control=EdgeShard::load(&control_path,None).unwrap();
    let candidate=EdgeShard::load(&candidate_path,None).unwrap();
    assert_eq!(answers(&control,POINTS,true),updated);
    assert_eq!(answers(&candidate,POINTS,true),updated);
    assert_eq!(control.info().unwrap().points_count,POINTS as usize);
    assert_eq!(candidate.info().unwrap().points_count,POINTS as usize);
    storage_evidence(&control_path,&control,false);
    assert!(storage_evidence(&candidate_path,&candidate,true)>0);
    eprintln!("{{\"vendor_lifecycle\":\"verified\",\"points_per_arm\":4096,\"vector_nonappendable_cold_mmap\":true,\"keyword_immutable_cold_mmap\":true,\"bool_cold_requested\":true,\"bool_cold_provided\":false,\"bool_actual\":\"RoaringFlags<MmapFile>/Heap/Pinned\",\"capacity_proven\":false}}");
}
