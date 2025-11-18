use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rocksdb::{Direction, IteratorMode, WriteBatch};

use crate::{
    doc_roles::{DocumentRoleCursor, DocumentRoleRecord, backend::DocRoleBackend},
    ids::{DocId, UserId, WorkspaceId},
};

use super::doc_data::DocDataStore;

pub struct RocksDocRoleBackend {
    store: Arc<DocDataStore>,
}

impl RocksDocRoleBackend {
    pub fn new(store: Arc<DocDataStore>) -> Self {
        Self { store }
    }

    fn role_key(workspace_id: &str, doc_id: &str, user_id: &str) -> String {
        format!("role:{workspace_id}:{doc_id}:{user_id}")
    }

    fn doc_prefix(workspace_id: &str, doc_id: &str) -> String {
        format!("role:{workspace_id}:{doc_id}:")
    }

    fn load_doc_roles(&self, workspace_id: &str, doc_id: &str) -> Result<Vec<DocumentRoleRecord>> {
        let prefix = Self::doc_prefix(workspace_id, doc_id);
        let cf = self.store.doc_roles_cf();
        let db = self.store.db();
        let mut roles = Vec::new();
        let iter = db.iterator_cf(
            cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            let record: DocumentRoleRecord =
                serde_json::from_slice(&value).context("failed to decode Rocks doc role record")?;
            roles.push(record);
        }
        Ok(roles)
    }

    fn encode_record(record: &DocumentRoleRecord) -> Result<Vec<u8>> {
        serde_json::to_vec(record).context("failed to encode Rocks doc role record")
    }

    fn put_role(&self, record: &DocumentRoleRecord, batch: Option<&mut WriteBatch>) -> Result<()> {
        let key = Self::role_key(
            record.workspace_id.as_str(),
            record.doc_id.as_str(),
            record.user_id.as_str(),
        );
        let value = Self::encode_record(record)?;
        if let Some(batch) = batch {
            batch.put_cf(self.store.doc_roles_cf(), key.as_bytes(), &value);
            Ok(())
        } else {
            self.store
                .db()
                .put_cf(self.store.doc_roles_cf(), key.as_bytes(), &value)
                .context("failed to write Rocks doc role record")
        }
    }

    fn delete_role_key(&self, workspace_id: &str, doc_id: &str, user_id: &str) -> Result<()> {
        let key = Self::role_key(workspace_id, doc_id, user_id);
        self.store
            .db()
            .delete_cf(self.store.doc_roles_cf(), key.as_bytes())
            .context("failed to delete Rocks doc role record")
    }
}

#[async_trait]
impl DocRoleBackend for RocksDocRoleBackend {
    async fn list_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>> {
        let mut roles = self.load_doc_roles(workspace_id, doc_id)?;
        roles.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.user_id.cmp(&b.user_id))
        });
        Ok(roles)
    }

    async fn find_for_user(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
    ) -> Result<Option<DocumentRoleRecord>> {
        let key = Self::role_key(workspace_id, doc_id, user_id);
        let value = self
            .store
            .db()
            .get_cf(self.store.doc_roles_cf(), key.as_bytes())
            .context("failed to read Rocks doc role record")?;
        Ok(match value {
            Some(bytes) => Some(
                serde_json::from_slice(&bytes).context("failed to decode Rocks doc role record")?,
            ),
            None => None,
        })
    }

    async fn paginate_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentRoleCursor>,
    ) -> Result<Vec<DocumentRoleRecord>> {
        let mut roles = self.load_doc_roles(workspace_id, doc_id)?;
        roles.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.user_id.cmp(&b.user_id))
        });

        let mut filtered: Vec<DocumentRoleRecord> = if let Some(cursor) = cursor {
            roles
                .into_iter()
                .filter(|role| {
                    role.created_at > cursor.created_at
                        || (role.created_at == cursor.created_at && role.user_id > cursor.user_id)
                })
                .collect()
        } else if offset > 0 {
            roles.into_iter().skip(offset as usize).collect()
        } else {
            roles
        };

        if filtered.len() > limit as usize {
            filtered.truncate(limit as usize);
        }

        Ok(filtered)
    }

    async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64> {
        let roles = self.load_doc_roles(workspace_id, doc_id)?;
        Ok(roles.len() as i64)
    }

    async fn owners_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>> {
        let mut owners: Vec<_> = self
            .load_doc_roles(workspace_id, doc_id)?
            .into_iter()
            .filter(|role| role.role.eq_ignore_ascii_case("owner"))
            .collect();
        owners.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.user_id.cmp(&b.user_id))
        });
        Ok(owners)
    }

    async fn upsert_roles(&self, roles: &[DocumentRoleRecord]) -> Result<()> {
        if roles.is_empty() {
            return Ok(());
        }

        let mut batch = WriteBatch::default();
        for role in roles {
            let key =
                Self::role_key(role.workspace_id.as_str(), role.doc_id.as_str(), role.user_id.as_str());
            let value = Self::encode_record(role)?;
            batch.put_cf(self.store.doc_roles_cf(), key.as_bytes(), &value);
        }

        self.store
            .db()
            .write(batch)
            .context("failed to batch write Rocks doc roles")
    }

    async fn upsert_role(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
        role: &str,
        created_at: i64,
    ) -> Result<()> {
        let record = DocumentRoleRecord {
            workspace_id: WorkspaceId::from(workspace_id.to_owned()),
            doc_id: DocId::from(doc_id.to_owned()),
            user_id: UserId::from(user_id.to_owned()),
            role: role.to_owned(),
            created_at,
        };
        self.put_role(&record, None)
    }

    async fn remove_role(&self, workspace_id: &str, doc_id: &str, user_id: &str) -> Result<()> {
        self.delete_role_key(workspace_id, doc_id, user_id)
    }

    async fn remove_doc_roles(&self, workspace_id: &str, doc_id: &str) -> Result<()> {
        let prefix = Self::doc_prefix(workspace_id, doc_id);
        let cf = self.store.doc_roles_cf();
        let db = self.store.db();
        let mut batch = WriteBatch::default();
        let iter = db.iterator_cf(
            cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            batch.delete_cf(cf, &key);
        }

        db.write(batch)
            .context("failed to delete Rocks doc roles for document")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_backend() -> (TempDir, RocksDocRoleBackend) {
        let dir = TempDir::new().expect("create temp dir");
        let store = Arc::new(DocDataStore::open(dir.path().join("doc-kv")).unwrap());
        (dir, RocksDocRoleBackend::new(store))
    }

    #[tokio::test]
    async fn upsert_and_list_roles() {
        let (_dir, backend) = create_backend();
        let records = vec![
            DocumentRoleRecord {
                workspace_id: WorkspaceId::from("ws"),
                doc_id: DocId::from("doc"),
                user_id: UserId::from("user-a"),
                role: "editor".into(),
                created_at: 3,
            },
            DocumentRoleRecord {
                workspace_id: WorkspaceId::from("ws"),
                doc_id: DocId::from("doc"),
                user_id: UserId::from("user-b"),
                role: "owner".into(),
                created_at: 5,
            },
        ];
        backend.upsert_roles(&records).await.unwrap();

        let listed = backend.list_for_doc("ws", "doc").await.unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].user_id, UserId::from("user-b"));
        assert_eq!(listed[1].user_id, UserId::from("user-a"));

        let owners = backend.owners_for_doc("ws", "doc").await.unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].user_id, UserId::from("user-b"));
    }

    #[tokio::test]
    async fn pagination_respects_cursor() {
        let (_dir, backend) = create_backend();
        for (idx, user) in ["alice", "bob", "carol"].iter().enumerate() {
            backend
                .upsert_role("ws", "doc", user, "editor", (idx + 1) as i64)
                .await
                .unwrap();
        }

        let page = backend
            .paginate_for_doc("ws", "doc", 2, 0, None)
            .await
            .unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].user_id, UserId::from("alice"));
        assert_eq!(page[1].user_id, UserId::from("bob"));

        let cursor = DocumentRoleCursor {
            created_at: page[1].created_at,
            user_id: page[1].user_id.clone(),
        };
        let next = backend
            .paginate_for_doc("ws", "doc", 2, 0, Some(&cursor))
            .await
            .unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].user_id, UserId::from("carol"));
    }

    #[tokio::test]
    async fn remove_doc_roles_clears_entries() {
        let (_dir, backend) = create_backend();
        backend
            .upsert_role("ws", "doc", "user-a", "editor", 1)
            .await
            .unwrap();
        backend
            .upsert_role("ws", "doc", "user-b", "editor", 2)
            .await
            .unwrap();

        backend.remove_doc_roles("ws", "doc").await.unwrap();
        let listed = backend.list_for_doc("ws", "doc").await.unwrap();
        assert!(listed.is_empty());
    }
}
