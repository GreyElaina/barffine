pub mod backend;

use anyhow::{Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Sleep, sleep},
};
use tracing::{error, warn};

use crate::{
    config::DocStoreBackend,
    db::{Database, rocks::doc_role_backend::RocksDocRoleBackend},
};
use backend::{DocRoleBackendRef, SqlDocRoleBackend};

const ROLE_UPSERT_CHANNEL_CAPACITY: usize = 512;
const ROLE_UPSERT_BATCH_SIZE: usize = 32;
const ROLE_UPSERT_FLUSH_DELAY_MS: u64 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentRoleRecord {
    pub workspace_id: String,
    pub doc_id: String,
    pub user_id: String,
    pub role: String,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct DocumentRoleCursor {
    pub created_at: i64,
    pub user_id: String,
}

#[derive(Clone)]
pub struct DocumentRoleStore {
    backend: DocRoleBackendRef,
    upsert_tx: mpsc::Sender<QueuedRoleUpsert>,
}

impl DocumentRoleStore {
    pub fn new(database: &Database) -> Self {
        let backend = match database.doc_store_backend() {
            DocStoreBackend::Sql => {
                let repo = database.repositories().doc_role_repo();
                let sql_backend = SqlDocRoleBackend::new(repo);
                Arc::new(sql_backend) as DocRoleBackendRef
            }
            DocStoreBackend::RocksDb => {
                let store = database
                    .doc_data_store()
                    .expect("Rocks doc store requires Rocks doc data backend");
                Arc::new(RocksDocRoleBackend::new(store)) as DocRoleBackendRef
            }
        };
        Self::with_backend(backend)
    }

    pub(crate) fn with_backend(backend: DocRoleBackendRef) -> Self {
        let upsert_tx = spawn_upsert_worker(backend.clone());
        Self { backend, upsert_tx }
    }

    pub async fn list_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>> {
        self.backend.list_for_doc(workspace_id, doc_id).await
    }

    pub async fn find_for_user(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
    ) -> Result<Option<DocumentRoleRecord>> {
        self.backend
            .find_for_user(workspace_id, doc_id, user_id)
            .await
    }

    pub async fn paginate_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentRoleCursor>,
    ) -> Result<Vec<DocumentRoleRecord>> {
        self.backend
            .paginate_for_doc(workspace_id, doc_id, limit, offset, cursor)
            .await
    }

    pub async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64> {
        self.backend.count_for_doc(workspace_id, doc_id).await
    }

    pub async fn owners_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>> {
        self.backend.owners_for_doc(workspace_id, doc_id).await
    }

    pub async fn upsert(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
        role: &str,
    ) -> Result<()> {
        let created_at = Utc::now().timestamp();
        let record = DocumentRoleRecord {
            workspace_id: workspace_id.to_owned(),
            doc_id: doc_id.to_owned(),
            user_id: user_id.to_owned(),
            role: role.to_owned(),
            created_at,
        };
        let fallback_record = record.clone();
        let (tx, rx) = oneshot::channel();
        let queued = QueuedRoleUpsert {
            record,
            responder: tx,
        };

        match self.upsert_tx.send(queued).await {
            Ok(()) => match rx.await {
                Ok(result) => result,
                Err(_) => {
                    warn!(
                        "doc role upsert worker dropped before ack; falling back to direct write"
                    );
                    self.backend
                        .upsert_roles(std::slice::from_ref(&fallback_record))
                        .await
                }
            },
            Err(err) => {
                warn!("doc role upsert queue closed; falling back to direct write");
                let mut single = Vec::with_capacity(1);
                single.push(err.0.record);
                self.backend.upsert_roles(&single).await
            }
        }
    }

    pub async fn remove(&self, workspace_id: &str, doc_id: &str, user_id: &str) -> Result<()> {
        self.backend
            .remove_role(workspace_id, doc_id, user_id)
            .await
    }

    pub async fn remove_all_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<()> {
        self.backend.remove_doc_roles(workspace_id, doc_id).await
    }
}

struct QueuedRoleUpsert {
    record: DocumentRoleRecord,
    responder: oneshot::Sender<Result<()>>,
}

fn spawn_upsert_worker(doc_role_repo: DocRoleBackendRef) -> mpsc::Sender<QueuedRoleUpsert> {
    let (tx, rx) = mpsc::channel(ROLE_UPSERT_CHANNEL_CAPACITY);
    tokio::spawn(run_upsert_worker(doc_role_repo, rx));
    tx
}

async fn run_upsert_worker(
    doc_role_repo: DocRoleBackendRef,
    mut rx: mpsc::Receiver<QueuedRoleUpsert>,
) {
    let mut pending: Vec<QueuedRoleUpsert> = Vec::with_capacity(ROLE_UPSERT_BATCH_SIZE);
    let mut flush_deadline: Option<Pin<Box<Sleep>>> = None;

    loop {
        tokio::select! {
            maybe_job = rx.recv() => {
                match maybe_job {
                    Some(job) => {
                        pending.push(job);
                        if pending.len() >= ROLE_UPSERT_BATCH_SIZE {
                            flush_pending(&doc_role_repo, &mut pending).await;
                            flush_deadline = None;
                        } else if flush_deadline.is_none() {
                            flush_deadline = Some(new_flush_deadline());
                        }
                    }
                    None => break,
                }
            }
            _ = async {
                if let Some(deadline) = &mut flush_deadline {
                    deadline.as_mut().await;
                }
            }, if flush_deadline.is_some() => {
                flush_pending(&doc_role_repo, &mut pending).await;
                flush_deadline = None;
            }
        }
    }

    if !pending.is_empty() {
        flush_pending(&doc_role_repo, &mut pending).await;
    }
}

async fn flush_pending(doc_role_repo: &DocRoleBackendRef, pending: &mut Vec<QueuedRoleUpsert>) {
    if pending.is_empty() {
        return;
    }

    let mut batch = Vec::with_capacity(pending.len());
    let mut responders = Vec::with_capacity(pending.len());
    for job in pending.drain(..) {
        batch.push(job.record);
        responders.push(job.responder);
    }

    match doc_role_repo.upsert_roles(&batch).await {
        Ok(_) => {
            for responder in responders {
                let _ = responder.send(Ok(()));
            }
        }
        Err(err) => {
            error!(error = ?err, batch_size = batch.len(), "doc role batch upsert failed");
            let err_msg = err.to_string();
            for responder in responders {
                let _ = responder.send(Err(anyhow!(err_msg.clone())));
            }
        }
    }
}

fn new_flush_deadline() -> Pin<Box<Sleep>> {
    Box::pin(sleep(Duration::from_millis(ROLE_UPSERT_FLUSH_DELAY_MS)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::future::join_all;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use crate::doc_roles::backend::{DocRoleBackend, DocRoleBackendRef};

    #[derive(Default)]
    struct MockDocRoleRepo {
        calls: Mutex<Vec<Vec<DocumentRoleRecord>>>,
    }

    impl MockDocRoleRepo {
        async fn calls(&self) -> Vec<Vec<DocumentRoleRecord>> {
            self.calls.lock().await.clone()
        }
    }

    #[async_trait]
    #[async_trait]
    impl DocRoleBackend for MockDocRoleRepo {
        async fn list_for_doc(
            &self,
            _workspace_id: &str,
            _doc_id: &str,
        ) -> Result<Vec<DocumentRoleRecord>> {
            Ok(Vec::new())
        }

        async fn find_for_user(
            &self,
            _workspace_id: &str,
            _doc_id: &str,
            _user_id: &str,
        ) -> Result<Option<DocumentRoleRecord>> {
            Ok(None)
        }

        async fn paginate_for_doc(
            &self,
            _workspace_id: &str,
            _doc_id: &str,
            _limit: i64,
            _offset: i64,
            _cursor: Option<&DocumentRoleCursor>,
        ) -> Result<Vec<DocumentRoleRecord>> {
            Ok(Vec::new())
        }

        async fn count_for_doc(&self, _workspace_id: &str, _doc_id: &str) -> Result<i64> {
            Ok(0)
        }

        async fn owners_for_doc(
            &self,
            _workspace_id: &str,
            _doc_id: &str,
        ) -> Result<Vec<DocumentRoleRecord>> {
            Ok(Vec::new())
        }

        async fn upsert_roles(&self, roles: &[DocumentRoleRecord]) -> Result<()> {
            self.calls.lock().await.push(roles.to_vec());
            Ok(())
        }

        async fn upsert_role(
            &self,
            workspace_id: &str,
            doc_id: &str,
            user_id: &str,
            role: &str,
            created_at: i64,
        ) -> Result<()> {
            self.calls.lock().await.push(vec![DocumentRoleRecord {
                workspace_id: workspace_id.to_owned(),
                doc_id: doc_id.to_owned(),
                user_id: user_id.to_owned(),
                role: role.to_owned(),
                created_at,
            }]);
            Ok(())
        }

        async fn remove_role(
            &self,
            _workspace_id: &str,
            _doc_id: &str,
            _user_id: &str,
        ) -> Result<()> {
            Ok(())
        }

        async fn remove_doc_roles(&self, _workspace_id: &str, _doc_id: &str) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn batches_pending_upserts_before_flush() {
        let mock_repo = Arc::new(MockDocRoleRepo::default());
        let backend_ref: DocRoleBackendRef = mock_repo.clone();
        let store = DocumentRoleStore::with_backend(backend_ref);

        let futures = (0..3).map(|i| {
            let store = store.clone();
            async move {
                store
                    .upsert("ws", "doc", &format!("user-{i}"), "editor")
                    .await
                    .unwrap();
            }
        });

        join_all(futures).await;

        tokio::time::sleep(Duration::from_millis(ROLE_UPSERT_FLUSH_DELAY_MS * 4)).await;
        let calls = mock_repo.calls().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].len(), 3);
    }
}
