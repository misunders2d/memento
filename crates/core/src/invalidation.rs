//! Explicit equivalents of the former ann.py dependency selectors. No invalidation triggers.
//! Call before destructive joins vanish and after moved/replaced associations as applicable.
use crate::{EventFence, Result, WriteTxn};
fn targets(ids: &str) -> String {
    format!(
        "SELECT i.seq FROM vector_items i WHERE i.target IN({ids}) UNION SELECT i.seq FROM vector_items i JOIN segments s ON s.id=i.target WHERE i.kind='segment' AND s.node_id IN({ids})"
    )
}
fn sha(expression: &str) -> String {
    targets(&format!(
        "SELECT node_id FROM captured_media WHERE sha256={expression} UNION SELECT node_id FROM media_files WHERE sha256={expression}"
    ))
}
fn episodes(ids: &str) -> String {
    format!(
        "SELECT i.seq FROM vector_items i JOIN episode_chunks x ON x.id=i.target WHERE i.kind='episode' AND x.episode_id IN({ids}) UNION {} UNION SELECT i.seq FROM vector_items i JOIN candidates c ON c.id=i.target JOIN extraction_runs r ON r.id=c.run_id WHERE i.kind='candidate' AND r.episode_id IN({ids}) UNION {}",
        targets(&format!(
            "SELECT target FROM graph_evidence WHERE episode_id IN({ids})"
        )),
        targets(&format!(
            "SELECT cm.node_id FROM captured_media cm JOIN captured_media_refs r ON r.sha256=cm.sha256 WHERE r.episode_id IN({ids})"
        ))
    )
}
impl WriteTxn<'_, '_> {
    pub fn touch_target(&self, id: &str) -> Result<Vec<EventFence>> {
        self.touch_selected(&targets("SELECT ?1"), [id])
    }
    pub fn touch_episode(&self, id: &str) -> Result<Vec<EventFence>> {
        self.touch_selected(&episodes("SELECT ?1"), [id])
    }
    pub fn touch_source(&self, source: &str, event_id: &str) -> Result<Vec<EventFence>> {
        self.touch_selected(
            &episodes("SELECT id FROM episodes WHERE source=?1 AND event_id=?2"),
            [source, event_id],
        )
    }
    pub fn touch_media(&self, sha256: &str) -> Result<Vec<EventFence>> {
        self.touch_selected(&sha("?1"), [sha256])
    }
    pub fn touch_type(&self, name: &str) -> Result<Vec<EventFence>> {
        self.touch_selected(&targets("SELECT id FROM node_versions WHERE type=?1 UNION SELECT id FROM candidates WHERE type=?1"),[name])
    }
    pub fn touch_conversation(
        &self,
        source: &str,
        conversation_id: &str,
    ) -> Result<Vec<EventFence>> {
        // Parking moves only raw history physically. Curated/candidate/segment kinds
        // remain hot regardless of age; both-tier events below remove the old point.
        self.sql().execute("UPDATE vector_blobs SET tier=CASE WHEN EXISTS(SELECT 1 FROM vector_items i JOIN episode_chunks x ON x.id=i.target JOIN episodes e ON e.id=x.episode_id JOIN historical_parking h ON h.source=e.source AND h.conversation_id=e.conversation_id WHERE i.seq=vector_blobs.seq AND i.kind='episode' AND e.seq<=h.parked_through) THEN 'cold' ELSE 'hot' END WHERE seq IN(SELECT i.seq FROM vector_items i JOIN episode_chunks x ON x.id=i.target JOIN episodes e ON e.id=x.episode_id WHERE i.kind='episode' AND e.source=?1 AND e.conversation_id=?2)",[source,conversation_id])?;
        self.touch_selected(
            &episodes("SELECT id FROM episodes WHERE source=?1 AND conversation_id=?2"),
            [source, conversation_id],
        )
    }
    pub fn touch_run(&self, id: &str) -> Result<Vec<EventFence>> {
        self.touch_selected(&targets("SELECT id FROM candidates WHERE run_id=?1"), [id])
    }
    pub fn touch_segment(&self, id: &str) -> Result<Vec<EventFence>> {
        self.touch_selected(
            &targets("SELECT ?1 UNION SELECT node_id FROM segments WHERE id=?1"),
            [id],
        )
    }
    pub fn touch_forgotten_target(&self, id: &str) -> Result<Vec<EventFence>> {
        self.touch_selected(&format!("{} UNION {}",targets("SELECT ?1"),targets("SELECT node_id FROM captured_media WHERE sha256 IN(SELECT sha256 FROM media_files WHERE node_id=?1)")),[id])
    }
}
