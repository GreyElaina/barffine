pub fn doc_channel_key(workspace_id: &str, doc_id: &str) -> String {
    format!("{workspace_id}:{doc_id}")
}

pub fn comment_attachment_blob_key(doc_id: &str, key: &str) -> String {
    format!("comment-attachments/{doc_id}/{key}")
}
