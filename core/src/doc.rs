use parking_lot::Mutex;
use std::sync::Arc;

use anyhow::Result;
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update, merge_updates_v1};

/// Lightweight wrapper around `yrs::Doc` for local-first document handling.
pub struct DocEngine {
    doc: Arc<Mutex<Doc>>,
}

impl Clone for DocEngine {
    fn clone(&self) -> Self {
        Self {
            doc: self.doc.clone(),
        }
    }
}

impl DocEngine {
    pub fn new() -> Self {
        Self {
            doc: Arc::new(Mutex::new(Doc::new())),
        }
    }

    /// Merge incremental document updates into a single binary payload
    /// compatible with the JavaScript client.
    pub fn merge_updates(&self, updates: &[Vec<u8>]) -> Result<Vec<u8>> {
        merge_updates_v1(updates).map_err(Into::into)
    }

    pub fn snapshot(&self) -> Result<Vec<u8>> {
        let doc = self.doc.lock();
        let txn = doc.transact();
        Ok(txn.encode_state_as_update_v1(&StateVector::default()))
    }

    pub fn doc(&self) -> Arc<Mutex<Doc>> {
        self.doc.clone()
    }

    /// Apply a series of updates to an optional existing snapshot, returning a
    /// fresh snapshot representing the full document state.
    pub fn apply_updates_to_snapshot(
        snapshot: Option<&[u8]>,
        updates: &[Vec<u8>],
    ) -> Result<Vec<u8>> {
        let doc = Doc::new();
        if let Some(snapshot) = snapshot {
            apply_update_bytes(&doc, snapshot)?;
        }

        if !updates.is_empty() {
            let mut txn = doc.transact_mut();
            for update in updates {
                let decoded = Update::decode_v1(update)?;
                txn.apply_update(decoded)?;
            }
        }

        let txn = doc.transact();
        Ok(txn.encode_state_as_update_v1(&StateVector::default()))
    }
}

fn apply_update_bytes(doc: &Doc, bytes: &[u8]) -> Result<()> {
    let update = Update::decode_v1(bytes)?;
    let mut txn = doc.transact_mut();
    txn.apply_update(update)?;
    Ok(())
}

impl Default for DocEngine {
    fn default() -> Self {
        Self::new()
    }
}
