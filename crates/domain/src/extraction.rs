//! Source-validated tentative candidate batches; no model client/credentials.
use crate::pipeline_schema::{self, WorkResult, reject};
use crate::util::*;
use crate::{Graph, context, jobs::Job, loop_due, loops};
use memento_core::{Error, ReadTxn, Result, WriteTxn};
use rusqlite::params;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

pub const VERSION: &str = "extract-v3";
#[derive(Clone)]
pub struct Extractor {
    pub graph: Graph,
}
impl Extractor {
    pub fn new(graph: Graph) -> Self {
        Self { graph }
    }
    pub fn prepare(&self, job: &Job) -> Result<Value> {
        self.graph.store.read(|tx| {
            let mut windows = vec![];
            for item in job.payload["windows"]
                .as_array()
                .ok_or(Error::Validation("Invalid extraction windows"))?
            {
                let id = get_str(item, "episode_id")?;
                let row = one(tx.sql(), "SELECT * FROM episodes WHERE id=?", [id])?
                    .ok_or(Error::Validation("extraction_source_missing"))?;
                let text = tx
                    .original_text(id)?
                    .ok_or(Error::Validation("extraction_source_missing"))?;
                let metadata = decode_json(get_str(&row, "metadata")?)?;
                let kind = get_str(&row, "kind")?;
                let source = get_str(&row, "source")?;
                if metadata["extraction_mode"] == "skip"
                    || one(
                        tx.sql(),
                        "SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=? LIMIT 1",
                        params![source, row["event_id"].as_str()],
                    )?
                    .is_some()
                    || memento_core::raw::legacy_native_context(
                        source,
                        kind,
                        Some(&text),
                        Some(&metadata),
                    )
                {
                    continue;
                }
                let start = item["start"]
                    .as_u64()
                    .ok_or(Error::Validation("Invalid extraction window"))?
                    as usize;
                let end = item["end"]
                    .as_u64()
                    .ok_or(Error::Validation("Invalid extraction window"))?
                    as usize;
                let end = if metadata["extraction_mode"] == "tool_output" || kind == "tool" {
                    end.min(start + 2000)
                } else {
                    end
                };
                windows.push(json!({
                    "id":item["id"],
                    "episode_id":id,
                    "source":source,
                    "kind":kind,
                    "start":start,
                    "end":end,
                    "text":substring(&text,start,end),
                    "occurred_at":row["occurred_at"]
                }));
            }
            Ok(json!({"windows":windows,"window_view":"handles-v1","policy_version":VERSION}))
        })
    }
    pub fn model_input(context: &Value) -> Value {
        // local_time (with weekday) resolves an explicit relative due date only.
        let zone = context["timezone"].as_str();
        let windows = context["windows"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
            .map(|(index, item)| {
                json!({
                    "id":format!("w{index}"),
                    "kind":item["kind"],
                    "local_time":loop_due::local_time(item["occurred_at"].as_str(),zone),
                    "text":item["text"]
                })
            })
            .collect::<Vec<_>>();
        let mut view = json!({"timezone":zone,"windows":windows});
        if let Some(rejection) = context.get("previous_rejection") {
            view["previous_rejection"] = rejection.clone();
        }
        view
    }
    pub fn apply(
        &self,
        job: &Job,
        leased: &Value,
        output: &Value,
        agent: &str,
        model: Option<&str>,
        usage: Option<&Value>,
    ) -> WorkResult<Value> {
        if leased["policy_version"] != VERSION {
            return Err(Error::Conflict("Extraction policy changed; derive a fresh plan").into());
        }
        if !pipeline_schema::validate(&pipeline_schema::EXTRACTION, output) {
            return Err(reject("candidate_schema_invalid", vec![]));
        }
        let windows = leased_windows(leased)?;
        let batches = output["windows"].as_array().unwrap();
        let expected = windows.keys().map(String::as_str).collect::<HashSet<_>>();
        let supplied = batches
            .iter()
            .filter_map(|batch| batch["id"].as_str())
            .collect::<HashSet<_>>();
        if batches.len() != windows.len() || supplied != expected {
            return Err(coverage_rejection(leased, batches, &windows));
        }
        let dues = resolve_dues(batches, &windows, leased)?;
        let prepared = self
            .graph
            .store
            .read(|tx| {
                let mut prepared = Vec::new();
                for batch in batches {
                    prepared.push(prepare_window(tx, batch, &windows, &dues)?);
                }
                Ok(prepared)
            })
            .map_err(|error| quote_rejection(error, batches, &windows))?;
        let request = json!({
            "op":"extract",
            "job_id":job.id,
            "version":VERSION,
            "output":output,
            "agent":agent,
            "reported_model":model,
            "usage":usage
        });
        let ctx = context(
            format!("extract/{}", job.id),
            format!("extractor/{agent}"),
            "Candidate derivation from immutable raw evidence using the connected harness".into(),
        );
        let completion = Completion { job, model, usage };
        Ok(self.graph.store.command(&ctx, &request, |tx, commit| {
            job.check_in(tx.sql())?;
            let mut count = 0;
            let mut loop_responsibilities = serde_json::Map::new();
            for (handle, items) in &prepared {
                commit_window(
                    tx,
                    commit,
                    &completion,
                    &windows[handle],
                    items,
                    &mut count,
                    &mut loop_responsibilities,
                )?;
            }
            advance_cursor(tx, job)?;
            Ok(json!({
                "candidate_count":count,
                "window_count":prepared.len(),
                "job_id":job.id,
                "loop_responsibilities":loop_responsibilities
            }))
        })?)
    }
}
// The handle the model was given for the leased window at `index`.
fn leased_handle(leased: &Value, index: usize, item: &Value) -> Result<String> {
    Ok(if leased["window_view"] == "handles-v1" {
        format!("w{index}")
    } else {
        get_str(item, "id")?.into()
    })
}
// Leased windows keyed by the handle the model was given.
fn leased_windows(leased: &Value) -> Result<HashMap<String, Value>> {
    let mut windows = HashMap::new();
    for (index, item) in leased["windows"]
        .as_array()
        .ok_or(Error::Validation("Invalid leased extraction context"))?
        .iter()
        .enumerate()
    {
        windows.insert(leased_handle(leased, index, item)?, item.clone());
    }
    Ok(windows)
}
// Explains an output already refused for coverage: at most three offending handles, each
// problem present named before a second of its kind, and their counts for the timing line.
// A missing window is never read as empty: that would move the cursor past unread evidence.
fn coverage_rejection(
    leased: &Value,
    batches: &[Value],
    windows: &HashMap<String, Value>,
) -> pipeline_schema::WorkError {
    let mut returned: Vec<(&str, usize)> = vec![];
    for id in batches.iter().filter_map(|batch| batch["id"].as_str()) {
        match returned.iter_mut().find(|(known, _)| *known == id) {
            Some((_, times)) => *times += 1,
            None => returned.push((id, 1)),
        }
    }
    let missing = leased["windows"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .filter_map(|(index, item)| leased_handle(leased, index, item).ok())
        .filter(|handle| !returned.iter().any(|(id, _)| id == handle))
        .map(|handle| json!({"window_id":handle,"problem":"missing"}))
        .collect::<Vec<_>>();
    let unexpected = returned
        .iter()
        .filter(|(id, _)| !windows.contains_key(*id))
        .map(|(id, _)| json!({"window_id":id,"problem":"unexpected"}))
        .collect::<Vec<_>>();
    let duplicate = returned
        .iter()
        .filter(|(id, times)| *times > 1 && windows.contains_key(*id))
        .map(|(id, _)| json!({"window_id":id,"problem":"duplicate"}))
        .collect::<Vec<_>>();
    let problems = [
        ("apply_windows_missing", missing),
        ("apply_windows_unexpected", unexpected),
        ("apply_windows_duplicate", duplicate),
    ];
    for (name, found) in &problems {
        crate::work_timing::count(name, found.len() as u64);
    }
    let mut references = vec![];
    for rank in 0..3 {
        for (_, found) in &problems {
            references.extend(found.get(rank).cloned());
        }
    }
    reject("candidate_window_coverage_invalid", references)
}
fn resolve_dues(
    batches: &[Value],
    windows: &HashMap<String, Value>,
    leased: &Value,
) -> WorkResult<HashMap<(String, String), String>> {
    // An explicit due date resolves against its window's original time; keyed by (window, key).
    let mut dues = HashMap::new();
    for batch in batches {
        let handle = batch["id"].as_str().unwrap();
        for draft in batch["candidates"].as_array().unwrap() {
            if draft["due"].is_null() {
                continue;
            }
            let resolved = if draft["open_loop"] == true {
                loop_due::resolve(
                    &draft["due"],
                    draft["quote"].as_str().unwrap(),
                    windows[handle]["occurred_at"].as_str(),
                    leased["timezone"].as_str(),
                )
            } else {
                Err(loop_due::Invalid::Format)
            };
            let at = resolved.map_err(|invalid| {
                let code = match invalid {
                    loop_due::Invalid::Format => "candidate_due_invalid",
                    loop_due::Invalid::Quote => "candidate_due_quote_invalid",
                    loop_due::Invalid::BeforeSource => "candidate_due_before_source",
                };
                reject(code, vec![json!({"window_id":handle,"key":draft["key"],"quote":draft["due"]["quote"]})])
            })?;
            dues.insert(
                (handle.to_owned(), draft["key"].as_str().unwrap().to_owned()),
                at,
            );
        }
    }
    Ok(dues)
}
// One leased window's source text and identity while its candidates are validated.
struct WindowSource<'a> {
    handle: &'a str,
    episode_id: &'a str,
    raw: &'a str,
    begin: usize,
}
// Validates one window's drafts against its source and derives their candidate items.
fn prepare_window(
    tx: &ReadTxn,
    batch: &Value,
    windows: &HashMap<String, Value>,
    dues: &HashMap<(String, String), String>,
) -> Result<(String, Vec<Value>)> {
    let handle = get_str(batch, "id")?;
    let window = &windows[handle];
    let episode_id = get_str(window, "episode_id")?;
    let episode = one(tx.sql(), "SELECT * FROM episodes WHERE id=?", [episode_id])?
        .ok_or(Error::Validation("extraction_source_missing"))?;
    let text = tx
        .original_text(episode_id)?
        .ok_or(Error::Validation("extraction_source_missing"))?;
    let metadata = decode_json(get_str(&episode, "metadata")?)?;
    if memento_core::raw::legacy_native_context(
        get_str(&episode, "source")?,
        get_str(&episode, "kind")?,
        Some(&text),
        Some(&metadata),
    ) {
        return Ok((handle.to_owned(), vec![]));
    }
    let begin = window["start"].as_u64().unwrap() as usize;
    let raw = substring(&text, begin, window["end"].as_u64().unwrap() as usize);
    let mut keys = HashMap::new();
    let mut local = vec![];
    let source = WindowSource {
        handle,
        episode_id,
        raw: &raw,
        begin,
    };
    for draft in batch["candidates"].as_array().unwrap() {
        let key = get_str(draft, "key")?;
        if keys.contains_key(key) {
            return Err(Error::Validation("candidate_duplicate_key"));
        }
        let item = candidate_item(tx, draft, key, &source, dues)?;
        keys.insert(key.to_owned(), local.len());
        local.push((draft.clone(), item));
    }
    link_relation_endpoints(&mut local, &keys)?;
    Ok((
        handle.to_owned(),
        local.into_iter().map(|(_, item)| item).collect::<Vec<_>>(),
    ))
}
fn candidate_item(
    tx: &ReadTxn,
    draft: &Value,
    key: &str,
    source: &WindowSource,
    dues: &HashMap<(String, String), String>,
) -> Result<Value> {
    let WindowSource {
        handle,
        episode_id,
        raw,
        begin,
    } = *source;
    let kind = get_str(draft, "kind")?;
    let typ = get_str(draft, "type")?;
    if kind == "memory" && !MEMORIES.contains(&typ) {
        return Err(Error::Validation("candidate_memory_type_invalid"));
    }
    if kind == "entity" && !ENTITIES.contains(&typ) {
        return Err(Error::Validation("candidate_entity_type_invalid"));
    }
    if kind == "relation" {
        if !valid_relation(typ)
            || draft["src_key"].as_str().is_none_or(str::is_empty)
            || draft["dst_key"].as_str().is_none_or(str::is_empty)
        {
            return Err(Error::Validation("candidate_relation_invalid"));
        }
    } else if !draft["src_key"].is_null() || !draft["dst_key"].is_null() {
        return Err(Error::Validation("candidate_unexpected_relation_endpoints"));
    }
    // Store reference details separately on quote rejection; no model
    // output or source text is placed in HTTP diagnostics.
    let quote = get_str(draft, "quote")?;
    let occurrence = draft["occurrence"].as_u64().map(|value| value as usize);
    let (start, end) = source_span(raw, quote, occurrence)?;
    let from = time(draft["valid_from"].as_str())?;
    let to = time(draft["valid_to"].as_str())?;
    if let (Some(left), Some(right)) = (&from, &to) {
        if parse_time(left)? >= parse_time(right)? {
            return Err(Error::Validation("candidate_world_interval_invalid"));
        }
    }
    let labels = draft["scope"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    let labels = scope(tx.sql(), typ, &labels)?;
    let mut item = json!({
        "kind":kind,
        "type":typ,
        "text":draft["text"],
        "why":draft["why"],
        "scope":labels,
        "open_loop":draft["open_loop"],
        "responsibility":draft["responsibility"],
        "start":begin+start,
        "end":begin+end,
        "quote":quote,
        "confidence":draft["confidence"],
        "valid_from":from,
        "valid_to":to,
        "due_at":dues.get(&(handle.to_owned(),key.to_owned()))
    });
    let fingerprint = json!({
        "episode_id":episode_id,
        "kind":item["kind"],
        "type":item["type"],
        "text":item["text"],
        "start":item["start"],
        "end":item["end"],
        "valid_from":item["valid_from"],
        "valid_to":item["valid_to"]
    });
    item["id"] = json!(format!("c_{}", &hash_json(&fingerprint)?[..32]));
    Ok(item)
}
// Second pass over one window: relation candidates take their endpoints' candidate IDs.
fn link_relation_endpoints(
    local: &mut [(Value, Value)],
    keys: &HashMap<String, usize>,
) -> Result<()> {
    for index in 0..local.len() {
        let draft = &local[index].0;
        let (src, dst) = if draft["kind"] == "relation" {
            let a = keys.get(draft["src_key"].as_str().unwrap());
            let b = keys.get(draft["dst_key"].as_str().unwrap());
            let (Some(&a), Some(&b)) = (a, b) else {
                return Err(Error::Validation("candidate_relation_endpoint_missing"));
            };
            if !matches!(local[a].1["kind"].as_str(), Some("entity" | "memory"))
                || !matches!(local[b].1["kind"].as_str(), Some("entity" | "memory"))
            {
                return Err(Error::Validation("candidate_relation_endpoint_missing"));
            }
            let src = get_str(&local[a].1, "id")?.to_owned();
            let dst = get_str(&local[b].1, "id")?.to_owned();
            let material = format!("{}|{src}|{dst}", get_str(&local[index].1, "id")?);
            local[index].1["id"] = json!(format!(
                "c_{}",
                &memento_core::raw::sha256(material.as_bytes())[..32]
            ));
            (Some(src), Some(dst))
        } else {
            (None, None)
        };
        local[index].1["src"] = json!(src);
        local[index].1["dst"] = json!(dst);
    }
    Ok(())
}
// Quote failures become a rejection carrying at most three offending references.
fn quote_rejection(
    error: Error,
    batches: &[Value],
    windows: &HashMap<String, Value>,
) -> pipeline_schema::WorkError {
    let code = match &error {
        Error::Validation(code)
            if [
                "candidate_quote_not_in_source",
                "candidate_quote_ambiguous",
                "candidate_quote_occurrence_invalid",
            ]
            .contains(code) =>
        {
            Some(*code)
        }
        _ => None,
    };
    if let Some(code) = code {
        let mut references = vec![];
        for batch in batches {
            let window = &windows[batch["id"].as_str().unwrap()];
            for draft in batch["candidates"].as_array().unwrap() {
                if source_span(
                    window["text"].as_str().unwrap_or(""),
                    draft["quote"].as_str().unwrap_or(""),
                    draft["occurrence"].as_u64().map(|value| value as usize),
                )
                .is_err()
                {
                    references.push(json!({
                        "window_id":batch["id"],
                        "key":draft["key"],
                        "quote":draft["quote"],
                        "occurrence":draft["occurrence"]
                    }));
                    if references.len() == 3 {
                        break;
                    }
                }
            }
            if references.len() == 3 {
                break;
            }
        }
        reject(code, references)
    } else {
        error.into()
    }
}
// The leased job and the harness-reported model and usage recorded on each extraction run.
struct Completion<'a> {
    job: &'a Job,
    model: Option<&'a str>,
    usage: Option<&'a Value>,
}
fn commit_window(
    tx: &WriteTxn,
    commit: &str,
    completion: &Completion,
    window: &Value,
    items: &[Value],
    count: &mut i32,
    loop_responsibilities: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    let Completion { job, model, usage } = *completion;
    let episode_id = get_str(window, "episode_id")?;
    let episode = one(tx.sql(), "SELECT * FROM episodes WHERE id=?", [episode_id])?
        .ok_or(Error::Validation("extraction_source_missing"))?;
    let text = tx
        .original_text(episode_id)?
        .ok_or(Error::Validation("extraction_source_missing"))?;
    let metadata = decode_json(get_str(&episode, "metadata")?)?;
    if one(
        tx.sql(),
        "SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=? LIMIT 1",
        params![episode["source"].as_str(), episode["event_id"].as_str()],
    )?
    .is_some()
        || memento_core::raw::legacy_native_context(
            get_str(&episode, "source")?,
            get_str(&episode, "kind")?,
            Some(&text),
            Some(&metadata),
        )
    {
        return Ok(());
    }
    let run = format!("{}/{}", job.id, get_str(window, "id")?);
    tx.sql().execute(
        "INSERT INTO extraction_runs VALUES (?,?,?,?,?,?,?,?)",
        params![
            run,
            commit,
            episode_id,
            VERSION,
            "connected-harness-default",
            encode_json(&json!(
                model
                    .filter(|value| !value.is_empty())
                    .into_iter()
                    .collect::<Vec<_>>()
            ))?,
            encode_json(&usage.cloned().unwrap_or(Value::Null))?,
            now()
        ],
    )?;
    for item in items {
        insert_candidate(
            tx,
            commit,
            &run,
            episode_id,
            item,
            count,
            loop_responsibilities,
        )?;
    }
    tx.touch_run(&run)?;
    Ok(())
}
fn insert_candidate(
    tx: &WriteTxn,
    commit: &str,
    run: &str,
    episode_id: &str,
    item: &Value,
    count: &mut i32,
    loop_responsibilities: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    let inserted = tx.sql().execute(
        "INSERT INTO \
            candidates(id,run_id,kind,type,text,why,src,dst,confidence,valid_from,valid_to,scope) \
            SELECT ?,?,?,?,?,?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM candidates WHERE id=?)",
        params![
            item["id"].as_str(),
            run,
            item["kind"].as_str(),
            item["type"].as_str(),
            item["text"].as_str(),
            item["why"].as_str(),
            item["src"].as_str(),
            item["dst"].as_str(),
            item["confidence"].as_f64(),
            item["valid_from"].as_str(),
            item["valid_to"].as_str(),
            encode_json(&item["scope"])?,
            item["id"].as_str()
        ],
    )?;
    if inserted > 0 {
        let target = get_str(item, "id")?;
        tx.sql().execute(
            "INSERT INTO graph_evidence VALUES (?,?,?,?,?,?)",
            params![
                target,
                1,
                episode_id,
                item["start"].as_i64(),
                item["end"].as_i64(),
                item["quote"].as_str()
            ],
        )?;
        loops::mark(
            tx,
            commit,
            target,
            item["open_loop"].as_bool(),
            None,
            None,
            item["due_at"].as_str(),
        )?;
        if item["open_loop"] == true {
            if let Some(value) = item["responsibility"].as_str() {
                let role = loops::assign_automatic(
                    tx,
                    commit,
                    target,
                    value,
                    &json!([
                        {
                            "episode_id":episode_id,
                            "quote":item["quote"],
                            "start":item["start"],
                            "end":item["end"]
                        }
                    ]),
                )?;
                if let Some(map) = role["loop_responsibilities"].as_object() {
                    loop_responsibilities.extend(map.clone());
                }
            }
        }
        tx.touch_target(target)?;
        *count += 1;
    }
    Ok(())
}
// Demand jobs never move a conversation cursor.
fn advance_cursor(tx: &WriteTxn, job: &Job) -> Result<()> {
    if job.payload["mode"] == "demand" {
    } else if job.payload["cursor"] == "live" {
        tx.sql().execute(
            "UPDATE historical_parking SET live_seq=?,live_offset=? WHERE source=? AND conversation_id=?",
            params![
                job.payload["next_seq"].as_i64(),
                job.payload["next_offset"].as_i64(),
                job.payload["source"].as_str(),
                job.payload["conversation_id"].as_str()
            ],
        )?;
    } else {
        tx.sql().execute(
            "UPDATE processing_progress SET seq=?,offset=? WHERE source=? AND conversation_id=?",
            params![
                job.payload["next_seq"].as_i64(),
                job.payload["next_offset"].as_i64(),
                job.payload["source"].as_str(),
                job.payload["conversation_id"].as_str()
            ],
        )?;
    }
    Ok(())
}
pub fn source_span(text: &str, quote: &str, occurrence: Option<usize>) -> Result<(usize, usize)> {
    let matches = text
        .char_indices()
        .enumerate()
        .filter_map(|(character, (byte, _))| text[byte..].starts_with(quote).then_some(character))
        .collect::<Vec<_>>();
    if matches.is_empty() {
        return Err(Error::Validation("candidate_quote_not_in_source"));
    }
    if occurrence.is_none() && matches.len() > 1 {
        return Err(Error::Validation("candidate_quote_ambiguous"));
    }
    let start = *matches
        .get(occurrence.unwrap_or(0))
        .ok_or(Error::Validation("candidate_quote_occurrence_invalid"))?;
    Ok((start, start + quote.chars().count()))
}
