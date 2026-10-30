// Included in the existing retrieval fixtures: real SQLite+Edge, paused scorer only.
#[test]
fn owned_hot_paused_scorer_allows_capture_query_and_keeps_old_current_and_forgetting_cut() {
    use std::sync::{mpsc, atomic::Ordering};
    use std::time::Duration;
    let mut f=Fixture::new();
    let changed=f.add("Copper kettle stored above the workshop shelf","knowledge","2026-10-25T00:00:00Z",0.5,1.,"personal");
    let forgotten=f.add("Olive orchard harvest begins after the autumn rain","knowledge","2026-10-25T00:00:00Z",0.5,0.9,"personal");
    for tier in [Tier::Hot,Tier::Cold] {f.projection.rebuild(tier,&AtomicBool::new(false)).unwrap();}
    let (entered_tx,entered_rx)=mpsc::sync_channel(0);let (resume_tx,resume_rx)=mpsc::sync_channel(0);let (done_tx,done_rx)=mpsc::sync_channel(0);
    let mut vector=[0.;768];vector[0]=1.;
    std::thread::scope(|scope| {
        let projection=f.projection.clone();let old_changed=changed.clone();let old_forgotten=forgotten.clone();
        let reader=scope.spawn(move|| {
            let request=RecallRequest{query:"plausible contextual probe".into(),effort:"deep".into(),limit:6,..Default::default()};
            let first=AtomicBool::new(true);
            let score=|documents:&[&str],_:Instant| {
                if first.swap(false,Ordering::SeqCst) {
                    entered_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(10)).map_err(|_|Error::Unavailable("Paused scorer test expired"))?;
                }
                Ok(vec![0.;documents.len()])
            };
            let deadline=Instant::now()+Duration::from_secs(20);
            projection.snapshot_hot_until(true,deadline,
                |_,view|Retrieval::hot_preview(view,&request,&vector,true,None),
                |tx,hot,cold| {
                    let cut=tx.source_cut().clone();
                    let rows=Retrieval::current_owned_scored(tx,hot,|n,pool|cold.search(&vector,n,"episode",pool,None,None),&request,&vector,true,deadline,Some(&score))?.0;
                    assert_eq!(tx.source_cut(),&cut);
                    assert_eq!(tx.sql().query_row("SELECT max(version) FROM node_versions WHERE id=?",[&old_changed],|r|r.get::<_,i64>(0))?,1);
                    assert_eq!(tx.sql().query_row("SELECT count(*) FROM forgotten_targets WHERE target=?",[&old_forgotten],|r|r.get::<_,i64>(0))?,0);
                    assert_eq!(tx.sql().query_row("SELECT count(*) FROM episodes WHERE source='concurrent-proof'",[],|r|r.get::<_,i64>(0))?,0);
                    let bytes:Vec<u8>=tx.sql().query_row("SELECT embedding FROM vector_blobs WHERE seq=1",[],|r|r.get(0))?;
                    assert_eq!(F32Blob::from_bytes(bytes)?.values()[0],1.);
                    Ok(rows)
                }).unwrap()
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).expect("scorer did not pause");
        let store=f.store.clone();let projection=f.projection.clone();let writer_changed=changed.clone();let writer_forgotten=forgotten.clone();
        let writer=scope.spawn(move|| {
            let captured=store.capture(serde_json::from_value(json!({"source":"concurrent-proof","event_id":"new","conversation_id":"new","kind":"user","text":"Exact new capture while scorer is paused","metadata":{}})).unwrap()).unwrap();
            store.write(|tx| {
                tx.sql().execute("INSERT INTO node_versions SELECT id,2,commit_id,type,'Updated assertion after the pinned cut',state,valid_from,valid_to,superseded_by,pinned,scope FROM node_versions WHERE id=? AND version=1",[&writer_changed])?;
                let mut v=[0.;768];v[0]=0.8;v[1]=0.6;
                tx.put_vector(&VectorWrite{seq:1,target:writer_changed.clone(),kind:VectorKind::Node,version:2,current:true,tier:Tier::Hot,embedding:F32Blob::from_values(&v)?})?;
                tx.sql().execute("INSERT INTO forgotten_targets SELECT ?1,commit_id,'node' FROM node_versions WHERE id=?1 AND version=1",[&writer_forgotten])?;
                tx.touch_target(&writer_forgotten)?;
                Ok(())
            }).unwrap();
            let hits=projection.snapshot_until(true,false,Instant::now()+Duration::from_secs(5),|tx,view|{
                assert!(tx.episode(&captured.id)?.is_some());
                view.search(Tier::Hot,&vector,8,"node",Pool::Ordinary,None,None)
            }).unwrap();
            done_tx.send(hits).unwrap();
        });
        let concurrent=done_rx.recv_timeout(Duration::from_secs(5));
        // Always release the paused reader before reporting a concurrency failure.
        resume_tx.send(()).unwrap();
        let new_hits=concurrent.expect("Hot query/capture blocked behind paused scorer");
        assert!(new_hits.iter().any(|h|h.target==changed&&h.version==2));
        assert!(new_hits.iter().all(|h|h.target!=forgotten));
        writer.join().unwrap();let old_rows=reader.join().unwrap();
        assert!(old_rows.iter().any(|r|r["id"]==changed&&r["version"]==1));
        assert!(old_rows.iter().any(|r|r["id"]==forgotten));
    });
}

#[test]
fn optional_parked_preview_error_is_only_observed_when_pool_is_used() {
    let f=Fixture::new();let mut vector=[0.;768];vector[0]=1.;
    let preview=||HotPreview{at:Utc::now(),curated:Vec::new(),deep_raw:Vec::new(),parked:Some(Err(Error::Unavailable("optional preview sentinel"))),period:None};
    f.store.read(|tx| {
        let scoped=RecallRequest{query:"unrelated probe".into(),scope:Some("personal".into()),..Default::default()};
        let deadline=Instant::now()+std::time::Duration::from_secs(5);
        let value=Retrieval::current_owned_scored(tx,preview(),|_,_|panic!("Scoped request must not use Cold"),&scoped,&vector,true,deadline,None)?;
        assert!(value.0.is_empty());
        let automatic=RecallRequest{query:"unrelated probe".into(),automatic:true,..Default::default()};
        assert!(matches!(Retrieval::current_owned_scored(tx,preview(),|_,_|Ok(Vec::new()),&automatic,&vector,true,deadline,None),Err(Error::Unavailable("optional preview sentinel"))));
        Ok(())
    }).unwrap();
}
