use std::sync::{Arc, Mutex};

use anyhow::Result;
use y_octo::{Doc, StateVector, merge_updates_v1};

/// Lightweight wrapper around `y-octo::Doc` for local-first document handling.
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
        let merged = merge_updates_v1(updates)?;
        merged.encode_v1().map_err(Into::into)
    }

    pub fn snapshot(&self) -> Result<Vec<u8>> {
        let doc = self.doc.lock().expect("doc poisoned");
        doc.encode_state_as_update_v1(&StateVector::default())
            .map_err(Into::into)
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
        let mut doc = if let Some(snapshot) = snapshot {
            Doc::try_from_binary_v1(snapshot)?
        } else {
            Doc::new()
        };

        for update in updates {
            doc.apply_update_from_binary_v1(update)?;
        }

        doc.encode_state_as_update_v1(&StateVector::default())
            .map_err(Into::into)
    }
}

impl Default for DocEngine {
    fn default() -> Self {
        Self::new()
    }
}
