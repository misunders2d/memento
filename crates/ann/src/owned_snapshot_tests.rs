use super::*;
use memento_core::{VectorKind, VectorWrite};

#[test]
fn owned_hot_releases_guard_and_cold_query_releases_before_return() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data")).unwrap();
    let index=Projection::open(store.clone(),ModelSignature::pinned()).unwrap();
    store.write(|tx| {
        tx.sql().execute("INSERT INTO graph_commits VALUES('test','test','{}','{}','owner','Owned phase fixture','2026-10-27')", [])?;
        tx.sql().execute("INSERT INTO node_versions VALUES('node',1,'test','knowledge','Exact original assertion','active',NULL,NULL,NULL,0,'[\"personal\"]')", [])?;
        let mut v=[0.;768];v[0]=1.;
        tx.put_vector(&VectorWrite{seq:1,target:"node".into(),kind:VectorKind::Node,version:1,current:true,tier:Tier::Hot,embedding:F32Blob::from_values(&v)?})?;
        Ok(())
    }).unwrap();
    for tier in [Tier::Hot,Tier::Cold] {index.rebuild(tier,&AtomicBool::new(false)).unwrap();}
    let mut q=[0.;768];q[0]=1.;let deadline=Instant::now()+Duration::from_secs(5);
    let hits=index.snapshot_hot_until(true,deadline,
        |tx,view|Ok((tx.source_cut().clone(),view.search(Tier::Hot,&q,1,"node",Pool::Ordinary,Some("personal"),None)?)),
        |tx,(cut,hits),cold| {
            assert_eq!(tx.source_cut(),&cut);
            assert!(index.hot.state.try_lock().is_ok(),"Hot guard survived owned preview");
            assert!(index.cold.state.try_lock().is_err(),"Cold guard released before its query");
            assert!(cold.search(&q,1,"episode",Pool::Ordinary,None,None)?.is_empty());
            assert!(index.cold.state.try_lock().is_ok(),"Cold guard survived owned result return");
            Ok(hits)
        }).unwrap();
    assert_eq!(hits.len(),1);assert_eq!(hits[0].target,"node");assert_eq!(hits[0].version,1);
}
