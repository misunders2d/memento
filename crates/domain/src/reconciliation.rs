//! Explicit duplicate reconciliation reuses ordinary coalescence, never owner approval.
use crate::*;
use std::collections::BTreeSet;

impl Graph {
    pub fn reconcile(&self, command: dto::Reconcile) -> Result<Value> {
        validate_common(
            &command.command_id,
            &command.actor,
            &command.why,
            &command.evidence,
            command.source.as_deref(),
            command.conversation_id.as_deref(),
        )?;
        let members = std::iter::once(&command.survivor)
            .chain(command.absorbed.iter())
            .collect::<BTreeSet<_>>();
        if command.absorbed.is_empty()
            || command.absorbed.len() > 8
            || members.len() != command.absorbed.len() + 1
            || members != command.expected_versions.keys().collect()
            || command
                .expected_versions
                .values()
                .any(|version| *version < 1)
        {
            return Err(Error::Validation(
                "Reconcile requires distinct members and their exact versions",
            ));
        }
        for id in &members {
            required(id, 64, "Invalid reconciliation member identifier")?;
        }
        let request = json!({"op":"reconcile", "survivor":command.survivor,
            "absorbed":command.absorbed, "expected_versions":command.expected_versions,
            "evidence":command.evidence, "source":command.source,
            "conversation_id":command.conversation_id});
        let absorbed = command.absorbed;
        let context = context(command.command_id, command.actor, command.why);
        self.store.command(&context, &request, |tx, commit| {
            let request = TxGraph::resolve_evidence(tx, &request)?;
            let spans = TxGraph::evidence_spans(tx, &request["evidence"])?;
            for (id, version) in request["expected_versions"].as_object().unwrap() {
                let current = node(tx.sql(), id)?;
                if current["version"] != *version {
                    return Err(Error::Conflict("Reconciliation members changed; read history again"));
                }
                let mut grounded = false;
                for (episode, start, end, _) in &spans {
                    if one(tx.sql(), "SELECT 1 FROM graph_evidence WHERE target=? AND version=? AND episode_id=? AND start<=? AND end>=? LIMIT 1",
                        params![id, version.as_i64(), episode, start, end])?.is_some() {
                        grounded = true;
                        break;
                    }
                }
                if !grounded {
                    return Err(Error::Validation("Reconcile must cite original evidence for every member"));
                }
            }
            curation::absorb(tx, commit, get_str(&request, "survivor")?,
                &absorbed, Some(&request["evidence"]), false, None)
        })
    }
}
