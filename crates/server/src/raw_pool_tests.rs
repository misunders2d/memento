struct RawPoint {
    episode: String,
    chunk: String,
}
fn ranking_media_point(f: &Fixture, modality: &str, cosine: f32) -> String {
    let id = format!("ranking-{modality}");
    let segment = format!("{id}-segment");
    let text = "Synthetic attachment provenance";
    let source = f.store.capture(serde_json::from_value(json!({"source":"synthetic:media","event_id":id,"conversation_id":id,"kind":"user","text":text,"occurred_at":"2020-01-01T00:00:00Z","metadata":{}})).unwrap()).unwrap();
    let context = memento_core::CommandContext {
        command_id: id.clone(), actor: "fixture".into(), why: "Exact attachment evidence".into(),
    };
    let mut vector = [0.;768];
    vector[0] = cosine;
    vector[1] = (1.-cosine*cosine).sqrt();
    let sha = "a".repeat(64);
    f.store.command(&context,&json!({"op":"fixture_media"}),|tx,commit|{
        tx.touch_target(&id)?;
        tx.sql().execute("INSERT INTO node_versions(id,version,commit_id,type,text,state,pinned,scope) VALUES(?,1,?,'media',?,'active',0,'[\"personal\"]')",params![id,commit,text])?;
        tx.sql().execute("INSERT INTO media_files VALUES(?,?,'synthetic.bin',?,?)",params![id,sha,modality,commit])?;
        tx.sql().execute("INSERT INTO graph_evidence VALUES(?,1,?,0,?,?)",params![id,source.id,text.chars().count() as i64,text])?;
        tx.sql().execute("INSERT INTO segments(id,node_id,modality,text,artifact_sha256) VALUES(?,?,?,'',?)",params![segment,id,modality,sha])?;
        tx.put_vector(&VectorWrite{seq:9000,target:segment.clone(),kind:VectorKind::Segment,version:1,current:true,tier:Tier::Hot,embedding:F32Blob::from_values(&vector)?})?;
        Ok(json!({"id":id}))
    }).unwrap();
    id
}

#[test]
fn relevance_first_all_discovered_raw_lexical_rows_are_scored_beyond_semantic_shortlist() {
    let f = Fixture::new();
    let mut found = BTreeMap::new();
    for n in 0..21 {
        let id = format!("lexical-{n:02}");
        let text = if n == 20 { "late raw positive".to_owned() } else { format!("raw lexical negative {n}") };
        found.insert(id.clone(),json!({"id":id,"type":"raw_episode","tier":"raw","match":"keyword","text":text,"score":TEXT_FLOOR+0.25,"cosine":null}));
    }
    for n in 0..12 {
        let id = format!("semantic-{n:02}");
        found.insert(id.clone(),json!({"id":id,"type":"knowledge","match":"semantic","text":format!("semantic negative {n}"),"score":0.9,"cosine":0.9}));
    }
    let expected = found["lexical-20"].clone();
    let calls = std::cell::RefCell::new(Vec::<Vec<String>>::new());
    let score = |documents: &[&str], _: Instant| {
        calls.borrow_mut().push(documents.iter().map(|text|text.to_string()).collect());
        Ok(documents.iter().map(|text|if *text=="late raw positive"{1.}else{-10.}).collect())
    };
    f.store.read(|tx|filter_text_passages(tx.sql(),&mut found,10,Instant::now()+std::time::Duration::from_secs(5),Some(&score),&HashSet::new())).unwrap();
    assert_eq!(found.len(),1);
    assert_eq!(found["lexical-20"],expected);
    let calls = calls.borrow();
    assert!(calls.iter().all(|batch|batch.len()<=MAX_CANDIDATES));
    assert!(calls[0].iter().all(|text|text.as_str()!="late raw positive"));
    assert_eq!(calls.iter().flatten().filter(|text|text.starts_with("semantic negative")).count(),10);
    assert_eq!(calls.iter().flatten().filter(|text|text.starts_with("raw lexical")||text.as_str()=="late raw positive").count(),21);
    assert_eq!(calls.last().unwrap().last().unwrap(),"late raw positive");
}

#[test]
fn relevance_first_raw_lexical_passages_are_validated_and_positive_history_stays_available() {
    let f = Fixture::new();
    let positive = raw_point(&f,"positive-lexical",("synthetic","user","alpha omega exact supporting recollection"),json!({}),0.1,-1);
    let wrong_excerpt = format!("alpha unrelated distraction {} omega", "padding ".repeat(80));
    raw_point(&f,"wrong-lexical",("synthetic","user",&wrong_excerpt),json!({}),0.1,-2);
    f.store.write(|tx|{
        tx.sql().execute("INSERT INTO historical_parking(source,conversation_id,parked_through,live_seq,live_offset) SELECT source,conversation_id,seq,seq,0 FROM episodes WHERE id=?",[&positive.episode])?;
        tx.touch_conversation("synthetic","positive-lexical")?;Ok(())
    }).unwrap();
    f.projection.rebuild(Tier::Hot,&AtomicBool::new(false)).unwrap();
    f.projection.rebuild(Tier::Cold,&AtomicBool::new(false)).unwrap();
    for effort in ["normal","deep"] {
        let calls = std::cell::RefCell::new(Vec::<Vec<String>>::new());
        let score = |documents: &[&str], _: Instant| {
            calls.borrow_mut().push(documents.iter().map(|s|s.to_string()).collect());
            Ok(documents.iter().map(|text|if text.contains("exact supporting recollection"){1.}else{-10.}).collect())
        };
        let request = RecallRequest{query:"alpha omega".into(),effort:effort.into(),limit:10,..Default::default()};
        let mut vector=[0.;768];vector[0]=1.;
        let deadline=Instant::now()+std::time::Duration::from_secs(5);
        let rows=f.projection.snapshot_until(true,true,deadline,|tx,view|Retrieval::current_scored(tx,view,&request,&vector,true,deadline,Some(&score))).unwrap().0;
        assert_eq!(rows.len(),1);
        assert_eq!(rows[0]["id"],positive.chunk);
        assert_eq!(rows[0]["episode_id"],positive.episode);
        assert_eq!(rows[0]["match"],"keyword");
        assert!(rows[0]["cosine"].is_null());
        assert!((number(&rows[0],"score",0.)-(TEXT_FLOOR+0.25)).abs()<1e-12);
        if effort=="normal" {assert_eq!(rows[0]["historical"],true);}
        let calls=calls.borrow();
        assert_eq!(calls.len(),1);
        assert_eq!(calls[0].len(),2);
        assert!(calls[0].iter().any(|text|text.contains("unrelated distraction")&&!text.contains("omega")));
        assert!(calls[0].iter().any(|text|text.contains("exact supporting recollection")));
    }
}

#[test]
fn relevance_first_raw_flood_cannot_preempt_stronger_media_but_stronger_raw_can() {
    for (modality,cosine) in [("image",0.699),("audio",0.85)] {
        let f=Fixture::new();
        let media=ranking_media_point(&f,modality,cosine);
        for n in 0..12 {
            raw_point(&f,&format!("literal-{n}"),("synthetic","user",&format!("quartz exact supporting source {n}")),json!({}),0.1,-1-n);
        }
        f.projection.rebuild(Tier::Hot,&AtomicBool::new(false)).unwrap();
        f.projection.rebuild(Tier::Cold,&AtomicBool::new(false)).unwrap();
        let calls=std::cell::RefCell::new(Vec::new());
        let recall=|effort:&str|{
            let score=|documents:&[&str],_:Instant|{calls.borrow_mut().push(documents.len());Ok(vec![1.;documents.len()])};
            let request=RecallRequest{query:"quartz".into(),effort:effort.into(),limit:2,..Default::default()};
            let mut vector=[0.;768];vector[0]=1.;
            let deadline=Instant::now()+std::time::Duration::from_secs(5);
            f.projection.snapshot_until(true,true,deadline,|tx,view|Retrieval::current_scored(tx,view,&request,&vector,true,deadline,Some(&score))).unwrap().0
        };
        for effort in ["normal","deep"] {
            let rows=recall(effort);
            assert_eq!(rows.len(),2);
            assert_eq!(rows[0]["id"],media);
            assert_eq!(rows[0]["modality"],modality);
            assert!((number(&rows[0],"score",0.)-cosine as f64).abs()<1e-6);
            assert_eq!(rows[1]["tier"],"raw");
            assert_eq!(rows[1]["match"],"keyword");
            assert!(rows[1]["cosine"].is_null());
        }
        let strong=raw_point(&f,"strong-raw",("synthetic","user","Different wording with the strongest semantic source"),json!({}),0.98,-100);
        let next=raw_point(&f,"next-raw",("synthetic","user","Other wording with another strong semantic source"),json!({}),0.97,-101);
        f.projection.rebuild(Tier::Cold,&AtomicBool::new(false)).unwrap();
        for effort in ["normal","deep"] {
            let rows=recall(effort);
            assert_eq!(rows.len(),2);
            assert_eq!(rows[0]["id"],strong.chunk);
            assert_eq!(rows[1]["id"],next.chunk);
            assert!(rows.iter().all(|row|row["type"]=="raw_episode"));
        }
        assert!(!calls.borrow().is_empty());
        assert!(calls.borrow().iter().all(|count|*count<=MAX_CANDIDATES));
    }
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
