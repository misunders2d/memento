struct RawPoint {
    episode: String,
    chunk: String,
}
fn raw_point(
    f: &Fixture,
    event: &str,
    origin: (&str, &str, &str),
    metadata: Value,
    cosine: f32,
    seq: i64,
) -> RawPoint {
    let (source, kind, text) = origin;
    let episode = f.store.capture(serde_json::from_value(json!({"source":source,"event_id":event,"conversation_id":event,"kind":kind,"text":text,"metadata":metadata})).unwrap()).unwrap();
    let chunk = format!("chunk-{event}");
    let mut vector = [0.;768]; vector[0]=cosine; vector[1]=(1.-cosine*cosine).sqrt();
    f.store.write(|tx| {
        tx.sql().execute("INSERT INTO episode_chunks VALUES(?,?,0,?)",params![chunk,episode.id,text.chars().count() as i64])?;
        tx.put_vector(&VectorWrite{seq,target:chunk.clone(),kind:VectorKind::Episode,version:1,current:true,tier:Tier::Cold,embedding:F32Blob::from_values(&vector)?})?;
        Ok(())
    }).unwrap();
    RawPoint { episode:episode.id, chunk }
}
fn raw_current(f: &Fixture, automatic: bool, deep: bool, scope: Option<&str>, archive: bool) -> (Vec<Value>, bool) {
    let request = RecallRequest {
        query:"unseen paraphrase".into(), automatic, effort:if deep {"deep"}else{"normal"}.into(),
        limit:if automatic {6}else{10}, scope:scope.map(str::to_owned), ..Default::default()
    };
    let mut vector=[0.;768];vector[0]=1.;
    let deadline=Instant::now()+std::time::Duration::from_secs(5);
    f.projection.snapshot_until(true,archive,deadline,|tx,view|Retrieval::current(tx,view,&request,&vector,archive,deadline)).unwrap()
}
#[test]
fn raw_pool_fresh_fallback_prefilters_forbidden_sources_and_labels_actual_parking() {
    let f=Fixture::new();
    let fresh=raw_point(&f,"fresh",("synthetic","user","Fresh salinity evidence remains unprocessed"),json!({}),0.9,-1);
    let legacy="# AGENTS.md instructions for /fixture\n\n<INSTRUCTIONS>\nRetained developer salinity instruction body\n</INSTRUCTIONS>";
    // More forbidden higher-scoring points than either fallback fetch limit:
    // filtering after top K would lose the eligible fresh passage.
    for n in 0..48 {
        let (source,kind,text,metadata)=match n%8 {
            0=>("synthetic","developer","Excluded developer evidence",json!({})),
            1=>("synthetic","native_record","Excluded native source record",json!({})),
            2=>("synthetic","native_live_record","Excluded live source record",json!({})),
            3=>("synthetic","native_hook","Excluded hook source record",json!({})),
            4=>("synthetic","user","Excluded skipped source evidence",json!({"extraction_mode":"skip"})),
            5=>("synthetic","user","Excluded ineligible source evidence",json!({"evidence_eligible":false})),
            6=>("native:fixture:codex","user",legacy,json!({})),
            _=>("synthetic","user","Excluded forgotten source evidence",json!({})),
        };
        let event=format!("forbidden-{n}");
        let point=raw_point(&f,&event,(source,kind,text),metadata,0.99,-2-n);
        if n%8==7 {
            f.store.write(|tx| {tx.sql().execute("INSERT INTO forgotten_sources VALUES(?,?,?, 'fixture')",params![source,event,point.chunk])?;tx.touch_source(source,&event)?;Ok(())}).unwrap();
        }
    }
    f.projection.rebuild(Tier::Hot,&AtomicBool::new(false)).unwrap();
    f.projection.rebuild(Tier::Cold,&AtomicBool::new(false)).unwrap();
    for automatic in [false,true] {
        let rows=raw_current(&f,automatic,false,None,true).0;
        assert_eq!(rows.len(),1);
        assert_eq!(rows[0]["id"],fresh.chunk);
        assert_eq!(rows[0]["episode_id"],fresh.episode);
        assert_eq!(rows[0]["tier"],"raw");
        assert_eq!(rows[0]["confirmed"],false);
        assert_eq!(rows[0]["historical"],false);
        assert_ne!(rows[0]["category"],"historical_raw");
        if automatic {assert!(rows[0]["text"].as_str().unwrap().starts_with("Unverified raw passage"));}
        assert!(raw_current(&f,automatic,false,Some("personal"),true).0.is_empty());
    }
    let mut query=[0.;768];query[0]=1.;
    let keyword=f.projection.snapshot(true,true,|tx,view|Retrieval::raw_fallback(tx,view,"salinity",&query,10,true,Utc::now())).unwrap();
    assert_eq!(keyword.len(),1);
    assert_eq!(keyword[0]["id"],fresh.chunk);
    assert_eq!(keyword[0]["historical"],false);
    // Deep Ordinary remains broader than automatic/normal raw fallback.
    let deep=raw_current(&f,false,true,None,true).0;
    assert!(deep.iter().any(|r|r["id"]!=fresh.chunk));
    assert!(raw_current(&f,false,true,Some("personal"),true).0.is_empty());
    let citation=f.graph.cite(memento_domain::dto::Citation{event_id:"fresh-citation".into(),ids:vec![fresh.chunk.clone()],query:None,shown:vec![]}).unwrap();
    assert!(citation["demand_jobs"].as_array().unwrap().is_empty());
    f.store.write(|tx| {
        tx.sql().execute("INSERT INTO historical_parking(source,conversation_id,parked_through,live_seq,live_offset) SELECT source,conversation_id,seq,seq,0 FROM episodes WHERE id=?",[&fresh.episode])?;
        tx.touch_conversation("synthetic","fresh")?;Ok(())
    }).unwrap();
    let parked=raw_current(&f,true,false,None,true).0;
    assert_eq!(parked[0]["id"],fresh.chunk);
    assert_eq!(parked[0]["historical"],true);
    assert_eq!(parked[0]["category"],"historical_raw");
    assert!(parked[0]["text"].as_str().unwrap().starts_with("Unverified historical raw passage"));
    let citation=f.graph.cite(memento_domain::dto::Citation{event_id:"parked-citation".into(),ids:vec![fresh.chunk],query:None,shown:vec![]}).unwrap();
    assert_eq!(citation["demand_jobs"].as_array().unwrap().len(),1);
}
#[test]
fn raw_pool_curated_answer_does_not_wait_for_cold_or_mix_unscoped_raw() {
    let mut f=Fixture::new();
    let curated=f.add("Supported curated salinity fact","knowledge","2026-10-26T00:00:00Z",0.5,0.95,"personal");
    raw_point(&f,"new-raw",("synthetic","user","Fresh unprocessed raw source evidence"),json!({}),0.99,-1);
    f.projection.rebuild(Tier::Hot,&AtomicBool::new(false)).unwrap();
    assert!(!f.projection.ready(Tier::Cold));
    for automatic in [false,true] {
        let (rows,needs_cold)=raw_current(&f,automatic,false,None,false);
        assert!(!needs_cold);
        assert_eq!(rows[0]["id"],curated);
        assert!(rows.iter().all(|r|r["tier"]!="raw"));
        assert!(!f.projection.ready(Tier::Cold));
    }
}
