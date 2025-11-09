pub fn public_doc_share_path(workspace_id: &str, doc_id: &str) -> String {
    format!("/workspace/{workspace_id}/{doc_id}")
}
