use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use futures_util::FutureExt;
use moka::{future::Cache, notification::RemovalCause};
use serde_json::Value as JsonValue;
use snap::raw::{Decoder as SnapDecoder, Encoder as SnapEncoder};
use tokio::{sync::Mutex, time::sleep};
use tracing::{debug, warn};
use y_octo::{Doc as YoctoDoc, StateVector as YoctoStateVector};

use barffine_core::{
    doc_store::{DocumentSnapshot, DocumentStore},
    user_doc_store::UserDocStore,
};

use crate::socket::rooms::SpaceType;

pub type DocCacheResult<T> = anyhow::Result<T>;

const RATE_EPSILON: f64 = 1e-6;
const COMPRESS_THRESHOLD: f64 = 0.95;

#[derive(Clone, Debug)]
pub struct DocCacheMetrics {
    inner: Arc<DocCacheMetricsInner>,
}

#[derive(Debug, Default)]
struct DocCacheMetricsInner {
    hits: AtomicU64,
    misses: AtomicU64,
    loads_failed: AtomicU64,
    flush_success: AtomicU64,
    flush_errors: AtomicU64,
    flush_duration_ms: AtomicU64,
    pending_bytes: AtomicU64,
}

impl Default for DocCacheMetrics {
    fn default() -> Self {
        Self {
            inner: Arc::new(DocCacheMetricsInner::default()),
        }
    }
}

impl DocCacheMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_hit(&self) {
        self.inner.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.inner.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_load_error(&self) {
        self.inner.loads_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_flush_success(&self, duration: Duration) {
        self.inner.flush_success.fetch_add(1, Ordering::Relaxed);
        self.inner
            .flush_duration_ms
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn record_flush_error(&self) {
        self.inner.flush_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_pending_bytes(&self, bytes: usize) {
        self.inner
            .pending_bytes
            .store(bytes as u64, Ordering::Relaxed);
    }

    pub fn hits(&self) -> u64 {
        self.inner.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.inner.misses.load(Ordering::Relaxed)
    }

    pub fn pending_bytes(&self) -> u64 {
        self.inner.pending_bytes.load(Ordering::Relaxed)
    }

    pub fn flush_success_count(&self) -> u64 {
        self.inner.flush_success.load(Ordering::Relaxed)
    }

    pub fn flush_error_count(&self) -> u64 {
        self.inner.flush_errors.load(Ordering::Relaxed)
    }

    pub fn total_flush_duration_ms(&self) -> u64 {
        self.inner.flush_duration_ms.load(Ordering::Relaxed)
    }

    pub fn avg_flush_duration_ms(&self) -> f64 {
        let success = self.flush_success_count();
        if success == 0 {
            0.0
        } else {
            self.total_flush_duration_ms() as f64 / success as f64
        }
    }
}

#[derive(Clone)]
pub struct DocCache {
    cache: Cache<String, Arc<DocHandle>>,
    workspace_store: DocumentStore,
    userspace_store: UserDocStore,
    metrics: DocCacheMetrics,
    config: DocCacheConfig,
}

#[derive(Clone)]
struct DocDescriptor {
    space_type: SpaceType,
    space_id: String,
    doc_id: String,
}

struct DocHandle {
    descriptor: DocDescriptor,
    inner: Mutex<DocState>,
    estimated_bytes: AtomicUsize,
    adaptive: DocAdaptiveState,
}

#[derive(Clone)]
struct SnapshotEntry {
    bytes: Vec<u8>,
    encoding: SnapshotEncoding,
}

#[derive(Clone, Copy)]
enum SnapshotEncoding {
    Plain,
    Compressed,
}

struct DocState {
    doc: YoctoDoc,
    snapshot: SnapshotEntry,
    state_vector: YoctoStateVector,
    timestamp: i64,
    pending: VecDeque<Vec<u8>>,
    pending_bytes: usize,
    last_editor_id: Option<String>,
    stats: DocStats,
}

struct DocStats {
    last_update_ms: i64,
    ema_rate_per_sec: f64,
    ema_update_bytes: f64,
}

impl SnapshotEntry {
    fn new(bytes: Vec<u8>, mode: SnapshotStorageMode) -> DocCacheResult<Self> {
        let (bytes, encoding) = match mode {
            SnapshotStorageMode::Plain => (bytes, SnapshotEncoding::Plain),
            SnapshotStorageMode::Compressed => {
                let compressed = compress_snapshot(&bytes)?;
                // Only use compressed if it saves at least 5% space
                if compressed.len() < (bytes.len() as f64 * COMPRESS_THRESHOLD) as usize {
                    (compressed, SnapshotEncoding::Compressed)
                } else {
                    (bytes, SnapshotEncoding::Plain)
                }
            }
        };
        Ok(Self { bytes, encoding })
    }

    fn replace(&mut self, bytes: Vec<u8>, mode: SnapshotStorageMode) -> DocCacheResult<()> {
        *self = SnapshotEntry::new(bytes, mode)?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn to_plain_vec(&self) -> DocCacheResult<Vec<u8>> {
        match self.encoding {
            SnapshotEncoding::Plain => Ok(self.bytes.clone()),
            SnapshotEncoding::Compressed => decompress_snapshot(&self.bytes),
        }
    }
}

pub struct DocCacheApplyResult {
    pub snapshot: Vec<u8>,
    pub timestamp: i64,
}

#[derive(Clone)]
pub struct DocCacheConfig {
    pub max_capacity_bytes: u64,
    pub ttl: Option<Duration>,
    pub tti: Option<Duration>,
    pub flush_delay: Duration,
    pub max_pending_bytes: usize,
    pub adaptive: Option<DocCacheAdaptiveConfig>,
    pub adaptive_force_flush_ratio: f64,
    pub adaptive_force_flush_bytes: usize,
    pub adaptive_max_flush_interval: Duration,
    pub snapshot_mode: SnapshotStorageMode,
}

#[derive(Clone)]
pub struct DocCacheAdaptiveConfig {
    pub target_flush_interval: Duration,
    pub min_flush_delay: Duration,
    pub max_flush_delay: Duration,
    pub target_pending_bytes: usize,
    pub min_pending_bytes: usize,
    pub max_pending_bytes: usize,
    pub ema_alpha: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotStorageMode {
    Plain,
    Compressed,
}

impl Default for SnapshotStorageMode {
    fn default() -> Self {
        Self::Compressed
    }
}

impl Default for DocCacheConfig {
    fn default() -> Self {
        Self {
            max_capacity_bytes: 4 * 1024 * 1024 * 1024,
            ttl: Some(Duration::from_secs(30 * 60)),
            tti: Some(Duration::from_secs(5 * 60)),
            flush_delay: Duration::from_millis(500),
            max_pending_bytes: 512 * 1024,
            adaptive: Some(DocCacheAdaptiveConfig::default()),
            adaptive_force_flush_ratio: 0.85,
            adaptive_force_flush_bytes: 256 * 1024,
            adaptive_max_flush_interval: Duration::from_secs(1),
            snapshot_mode: SnapshotStorageMode::default(),
        }
    }
}

impl Default for DocCacheAdaptiveConfig {
    fn default() -> Self {
        Self {
            target_flush_interval: Duration::from_secs_f64(0.75),
            min_flush_delay: Duration::from_millis(50),
            max_flush_delay: Duration::from_secs(5),
            target_pending_bytes: 256 * 1024,
            min_pending_bytes: 64 * 1024,
            max_pending_bytes: 2 * 1024 * 1024,
            ema_alpha: 0.25,
        }
    }
}

pub struct DocCacheBuilder {
    config: DocCacheConfig,
    workspace_store: DocumentStore,
    userspace_store: UserDocStore,
    metrics: Option<DocCacheMetrics>,
}

impl DocCacheBuilder {
    pub fn new(workspace_store: DocumentStore, userspace_store: UserDocStore) -> Self {
        Self {
            config: DocCacheConfig::default(),
            workspace_store,
            userspace_store,
            metrics: None,
        }
    }

    pub fn with_max_capacity_bytes(mut self, bytes: u64) -> Self {
        self.config.max_capacity_bytes = bytes;
        self
    }

    pub fn with_time_to_live(mut self, ttl: Option<Duration>) -> Self {
        self.config.ttl = ttl;
        self
    }

    pub fn with_time_to_idle(mut self, tti: Option<Duration>) -> Self {
        self.config.tti = tti;
        self
    }

    pub fn with_metrics(mut self, metrics: DocCacheMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_flush_delay(mut self, delay: Duration) -> Self {
        self.config.flush_delay = delay;
        self
    }

    pub fn with_max_pending_bytes(mut self, bytes: usize) -> Self {
        self.config.max_pending_bytes = bytes;
        self
    }

    pub fn with_adaptive_config(mut self, adaptive: Option<DocCacheAdaptiveConfig>) -> Self {
        self.config.adaptive = adaptive;
        self
    }

    pub fn with_adaptive_force_flush_ratio(mut self, ratio: f64) -> Self {
        self.config.adaptive_force_flush_ratio = ratio;
        self
    }

    pub fn with_adaptive_force_flush_bytes(mut self, bytes: usize) -> Self {
        self.config.adaptive_force_flush_bytes = bytes;
        self
    }

    pub fn with_adaptive_max_flush_interval(mut self, interval: Duration) -> Self {
        self.config.adaptive_max_flush_interval = interval;
        self
    }

    pub fn with_snapshot_mode(mut self, mode: SnapshotStorageMode) -> Self {
        self.config.snapshot_mode = mode;
        self
    }

    pub fn with_config(mut self, config: DocCacheConfig) -> Self {
        self.config = config;
        self
    }

    pub fn build(self) -> DocCache {
        let metrics = self.metrics.unwrap_or_default();
        let workspace_store = self.workspace_store.clone();
        let userspace_store = self.userspace_store.clone();
        let eviction_workspace = self.workspace_store.clone();
        let eviction_userspace = self.userspace_store.clone();
        let eviction_metrics = metrics.clone();

        let mut builder = Cache::builder()
            .max_capacity(self.config.max_capacity_bytes)
            .weigher(|_, handle: &Arc<DocHandle>| handle.estimated_bytes());

        if let Some(ttl) = self.config.ttl {
            builder = builder.time_to_live(ttl);
        }

        if let Some(tti) = self.config.tti {
            builder = builder.time_to_idle(tti);
        }

        let cache = builder
            .async_eviction_listener(
                move |_key: Arc<String>, handle: Arc<DocHandle>, cause: RemovalCause| {
                    let workspace_store = eviction_workspace.clone();
                    let userspace_store = eviction_userspace.clone();
                    let metrics = eviction_metrics.clone();
                    async move {
                        if let Err(err) = handle
                            .flush_if_needed(&workspace_store, &userspace_store, &metrics)
                            .await
                        {
                            metrics.record_flush_error();
                            warn!(
                                space_type = ?handle.descriptor.space_type,
                                space_id = %handle.descriptor.space_id,
                                doc_id = %handle.descriptor.doc_id,
                                ?cause,
                                error = %err,
                                "failed to flush document on eviction",
                            );
                        }
                    }
                    .boxed()
                },
            )
            .build();

        DocCache {
            cache,
            workspace_store,
            userspace_store,
            metrics,
            config: self.config,
        }
    }
}

impl DocCache {
    pub fn new(workspace_store: DocumentStore, userspace_store: UserDocStore) -> Self {
        DocCacheBuilder::new(workspace_store, userspace_store).build()
    }

    pub fn metrics(&self) -> DocCacheMetrics {
        self.metrics.clone()
    }

    pub async fn snapshot(
        &self,
        space_type: SpaceType,
        space_id: &str,
        doc_id: &str,
    ) -> DocCacheResult<(Vec<u8>, i64)> {
        let handle = self
            .get_or_insert_handle(space_type, space_id, doc_id)
            .await?;
        handle.touch_access();

        let state = handle.inner.lock().await;
        Ok((state.snapshot.to_plain_vec()?, state.timestamp))
    }

    pub async fn apply_updates(
        &self,
        space_type: SpaceType,
        space_id: &str,
        doc_id: &str,
        updates: Vec<Vec<u8>>,
        editor_id: Option<&str>,
    ) -> DocCacheResult<DocCacheApplyResult> {
        if updates.is_empty() {
            return Err(anyhow!("updates payload must not be empty"));
        }

        let handle = self
            .get_or_insert_handle(space_type, space_id, doc_id)
            .await?;
        handle.touch_access();

        let mut state = handle.inner.lock().await;
        let prev_snapshot = state.snapshot.clone();
        let prev_snapshot_plain = prev_snapshot.to_plain_vec()?;
        let prev_state_vector = state.state_vector.clone();
        let prev_pending_bytes = state.pending_bytes;
        let prev_pending_len = state.pending.len();
        let prev_last_editor = state.last_editor_id.clone();

        let mut appended_bytes = 0usize;
        let update_count = updates.len();
        for update in &updates {
            state.doc.apply_update_from_binary_v1(update)?;
            state.pending.push_back(update.clone());
            appended_bytes += update.len();
        }
        state.pending_bytes += appended_bytes;
        state.state_vector = state.doc.get_state_vector();
        self.metrics.observe_pending_bytes(state.pending_bytes);

        let snapshot = match state
            .doc
            .encode_state_as_update_v1(&YoctoStateVector::default())
        {
            Ok(snapshot) => snapshot,
            Err(err) => {
                state.pending_bytes = prev_pending_bytes;
                while state.pending.len() > prev_pending_len {
                    state.pending.pop_back();
                }
                self.metrics.observe_pending_bytes(state.pending_bytes);
                state.doc = YoctoDoc::try_from_binary_v1(&prev_snapshot_plain)
                    .context("restore document snapshot after failed encode")?;
                state.snapshot = prev_snapshot;
                state.state_vector = prev_state_vector;
                state.last_editor_id = prev_last_editor;
                handle.update_estimated_bytes(&state);
                return Err(err.into());
            }
        };
        let timestamp = extract_doc_updated_at(&state.doc, &handle.descriptor.doc_id)
            .unwrap_or_else(current_time_millis);
        state
            .snapshot
            .replace(snapshot.clone(), self.config.snapshot_mode)?;
        state.timestamp = timestamp;
        state.last_editor_id = editor_id.map(str::to_owned);
        let now_ms = current_time_millis();
        if let Some(adaptive) = &self.config.adaptive {
            state
                .stats
                .record_updates(update_count, appended_bytes, now_ms, adaptive.ema_alpha);
            handle.adaptive.refresh_thresholds(adaptive, &state.stats);
        }
        handle.update_estimated_bytes(&state);
        debug!(
            space_type = ?handle.descriptor.space_type,
            space_id = %handle.descriptor.space_id,
            doc_id = %handle.descriptor.doc_id,
            pending_bytes = state.pending_bytes,
            "buffered document updates via cache",
        );

        let pending_limit = handle.adaptive.pending_limit();
        let should_flush_now = handle.adaptive.should_force_flush(
            state.pending_bytes,
            now_ms,
            self.config.adaptive_force_flush_bytes,
            self.config.adaptive_max_flush_interval,
        ) || state.pending_bytes >= pending_limit;
        let result = DocCacheApplyResult {
            snapshot,
            timestamp,
        };
        drop(state);

        if should_flush_now {
            handle
                .flush_if_needed(&self.workspace_store, &self.userspace_store, &self.metrics)
                .await?;
        } else {
            let flush_delay = handle.adaptive.current_flush_delay(self.config.flush_delay);
            handle.schedule_flush(
                flush_delay,
                self.workspace_store.clone(),
                self.userspace_store.clone(),
                self.metrics.clone(),
            );
        }

        self.maybe_evict_idle(space_type, space_id, doc_id, &handle)
            .await;
        Ok(result)
    }

    pub async fn open_session(
        &self,
        space_type: SpaceType,
        space_id: &str,
        doc_id: &str,
    ) -> DocCacheResult<()> {
        let handle = self
            .get_or_insert_handle(space_type, space_id, doc_id)
            .await?;
        handle.increment_sessions();
        Ok(())
    }

    pub async fn close_session(&self, space_type: SpaceType, space_id: &str, doc_id: &str) {
        let key = Self::key(space_type, space_id, doc_id);
        if let Some(handle) = self.cache.get(&key).await {
            if handle.decrement_sessions() {
                if let Err(err) = handle
                    .flush_if_needed(&self.workspace_store, &self.userspace_store, &self.metrics)
                    .await
                {
                    warn!(
                        space_type = ?space_type,
                        space_id,
                        doc_id,
                        error = %err,
                        "failed to flush doc cache entry while closing session",
                    );
                } else {
                    self.cache.invalidate(&key).await;
                }
            }
        }
    }

    pub async fn invalidate(&self, space_type: SpaceType, space_id: &str, doc_id: &str) {
        let key = Self::key(space_type, space_id, doc_id);
        if let Some(handle) = self.cache.get(&key).await {
            if let Err(err) = handle
                .flush_if_needed(&self.workspace_store, &self.userspace_store, &self.metrics)
                .await
            {
                warn!(
                    space_type = ?space_type,
                    space_id,
                    doc_id,
                    error = %err,
                    "failed to flush document before invalidation",
                );
            }
        }
        self.cache.invalidate(&key).await;
    }

    pub async fn reap_idle_entries(&self, idle_after: Duration) {
        let idle_ms = idle_after.as_millis() as i64;
        if idle_ms <= 0 {
            return;
        }

        self.cache.run_pending_tasks().await;
        let now = current_time_millis();
        let mut keys = Vec::new();
        for entry in self.cache.iter() {
            let (key_arc, handle) = entry;
            if handle.is_idle(now, idle_ms) {
                keys.push(key_arc.clone());
            }
        }

        for key in keys {
            self.cache.invalidate(key.as_ref()).await;
        }
    }

    async fn get_or_insert_handle(
        &self,
        space_type: SpaceType,
        space_id: &str,
        doc_id: &str,
    ) -> DocCacheResult<Arc<DocHandle>> {
        let key = Self::key(space_type, space_id, doc_id);
        if let Some(handle) = self.cache.get(&key).await {
            self.metrics.record_hit();
            return Ok(handle);
        }

        self.metrics.record_miss();
        let handle = Arc::new(
            DocHandle::load_initial(
                space_type,
                space_id.to_owned(),
                doc_id.to_owned(),
                &self.workspace_store,
                &self.userspace_store,
                &self.config,
            )
            .await
            .map_err(|err| {
                self.metrics.record_load_error();
                err
            })?,
        );

        self.cache.insert(key, handle.clone()).await;
        Ok(handle)
    }

    fn key(space_type: SpaceType, space_id: &str, doc_id: &str) -> String {
        format!("{}:{}:{}", space_type.as_str(), space_id, doc_id)
    }

    /// Immediately evict cache entries that have no active sessions and no pending updates.
    ///
    /// `apply_doc_updates` always calls this after it flushes or schedules a flush so that handles
    /// that are no longer in use do not sit around until the periodic reaper runs. This keeps the
    /// cache bounded during bursty update workloads: once a document has no sessions and nothing
    /// buffered (`is_instant_evictable`), we can safely drop it right away instead of burning
    /// memory for the default idle timeout.
    async fn maybe_evict_idle(
        &self,
        space_type: SpaceType,
        space_id: &str,
        doc_id: &str,
        handle: &Arc<DocHandle>,
    ) {
        if handle.is_instant_evictable() {
            let key = Self::key(space_type, space_id, doc_id);
            self.cache.invalidate(&key).await;
        }
    }
}

impl DocHandle {
    async fn load_initial(
        space_type: SpaceType,
        space_id: String,
        doc_id: String,
        workspace_store: &DocumentStore,
        userspace_store: &UserDocStore,
        config: &DocCacheConfig,
    ) -> DocCacheResult<Self> {
        let descriptor = DocDescriptor {
            space_type,
            space_id,
            doc_id,
        };

        let (snapshot, timestamp) = match descriptor.space_type {
            SpaceType::Workspace => {
                let DocumentSnapshot {
                    snapshot,
                    updated_at,
                } = workspace_store
                    .fetch_snapshot_with_timestamp(&descriptor.space_id, &descriptor.doc_id)
                    .await?
                    .ok_or_else(|| {
                        anyhow!(
                            "workspace document not found: {}/{}",
                            descriptor.space_id,
                            descriptor.doc_id
                        )
                    })?;
                (snapshot, updated_at)
            }
            SpaceType::Userspace => {
                let snapshot = userspace_store
                    .ensure_doc_record(&descriptor.space_id, &descriptor.doc_id)
                    .await?;
                (snapshot.snapshot, snapshot.updated_at)
            }
        };

        let (doc, raw_snapshot) = if snapshot.is_empty() {
            (YoctoDoc::new(), snapshot)
        } else {
            (
                YoctoDoc::try_from_binary_v1(&snapshot)
                    .context("decode yocto snapshot for cache entry")?,
                snapshot,
            )
        };
        let state_vector = doc.get_state_vector();
        let snapshot_entry = SnapshotEntry::new(raw_snapshot, config.snapshot_mode)?;
        let pending = VecDeque::new();
        let estimated_bytes = snapshot_entry.len();
        let now = current_time_millis();
        let adaptive_pending = config
            .adaptive
            .as_ref()
            .map(|adaptive| adaptive.target_pending_bytes)
            .unwrap_or(config.max_pending_bytes)
            .max(1);
        let adaptive_delay = config
            .adaptive
            .as_ref()
            .map(|adaptive| adaptive.target_flush_interval)
            .unwrap_or(config.flush_delay);

        let handle = Self {
            descriptor,
            inner: Mutex::new(DocState {
                doc,
                snapshot: snapshot_entry,
                state_vector,
                timestamp,
                pending,
                pending_bytes: 0,
                last_editor_id: None,
                stats: DocStats::new(now),
            }),
            estimated_bytes: AtomicUsize::new(estimated_bytes),
            adaptive: DocAdaptiveState::default(),
        };

        handle.adaptive.init(
            adaptive_pending,
            adaptive_delay,
            now,
            config.adaptive_force_flush_ratio,
        );
        Ok(handle)
    }

    fn touch_access(&self) {
        self.adaptive.touch_access();
    }

    fn update_estimated_bytes(&self, state: &DocState) {
        let pending_bytes = state.pending.iter().map(|u| u.len()).sum::<usize>();
        let total = state.snapshot.len() + pending_bytes;
        self.estimated_bytes.store(total, Ordering::Relaxed);
    }

    fn estimated_bytes(&self) -> u32 {
        let bytes = self.estimated_bytes.load(Ordering::Relaxed);
        bytes.min(u32::MAX as usize) as u32
    }

    fn is_idle(&self, now_ms: i64, idle_after_ms: i64) -> bool {
        if !self.adaptive.has_active_sessions() {
            if let Ok(state) = self.inner.try_lock() {
                return state.pending.is_empty();
            }
        }

        self.adaptive.is_idle(now_ms, idle_after_ms)
            && self
                .inner
                .try_lock()
                .map(|state| state.pending.is_empty())
                .unwrap_or(false)
    }

    fn is_instant_evictable(&self) -> bool {
        if self.adaptive.has_active_sessions() {
            return false;
        }

        self.inner
            .try_lock()
            .map(|state| state.pending.is_empty())
            .unwrap_or(false)
    }

    fn increment_sessions(&self) {
        self.adaptive.increment_sessions();
    }

    fn decrement_sessions(&self) -> bool {
        self.adaptive.decrement_sessions()
    }

    async fn flush_if_needed(
        &self,
        workspace_store: &DocumentStore,
        userspace_store: &UserDocStore,
        metrics: &DocCacheMetrics,
    ) -> DocCacheResult<()> {
        let updates = {
            let state = self.inner.lock().await;
            if state.pending.is_empty() {
                return Ok(());
            }
            state.pending.iter().cloned().collect::<Vec<_>>()
        };

        let mut state = self.inner.lock().await;
        if state.pending.is_empty() {
            return Ok(());
        }

        let descriptor = self.descriptor.clone();
        let timestamp = state.timestamp;
        let editor_id = state.last_editor_id.clone();

        let start = Instant::now();
        let persist_result = match descriptor.space_type {
            SpaceType::Workspace => {
                workspace_store
                    .append_doc_updates(
                        &descriptor.space_id,
                        &descriptor.doc_id,
                        &updates,
                        editor_id.as_deref(),
                        timestamp,
                    )
                    .await
            }
            SpaceType::Userspace => {
                userspace_store
                    .append_doc_updates(
                        &descriptor.space_id,
                        &descriptor.doc_id,
                        &updates,
                        editor_id.as_deref(),
                        timestamp,
                    )
                    .await
            }
        };

        match persist_result {
            Ok(()) => {
                state.pending.clear();
                state.pending_bytes = 0;
                metrics.observe_pending_bytes(0);
                self.update_estimated_bytes(&state);
                metrics.record_flush_success(start.elapsed());
                self.adaptive.notify_flush();
                Ok(())
            }
            Err(err) => {
                metrics.record_flush_error();
                Err(err)
            }
        }
    }

    fn schedule_flush(
        self: &Arc<Self>,
        flush_delay: Duration,
        workspace_store: DocumentStore,
        userspace_store: UserDocStore,
        metrics: DocCacheMetrics,
    ) {
        self.adaptive.schedule_flush(
            Arc::clone(self),
            flush_delay,
            workspace_store,
            userspace_store,
            metrics,
        );
    }
}

struct DocAdaptiveState {
    timing: AdaptiveTiming,
    flush_scheduled: AtomicBool,
    active_sessions: AtomicI32,
    pending_limit_bytes: AtomicUsize,
    flush_delay_ms: AtomicU64,
    force_ratio_bits: AtomicU64,
}

#[derive(Default)]
struct AdaptiveTiming {
    last_access_ms: AtomicI64,
    last_flush_ms: AtomicI64,
}

impl Default for DocAdaptiveState {
    fn default() -> Self {
        Self {
            timing: AdaptiveTiming::default(),
            flush_scheduled: AtomicBool::new(false),
            active_sessions: AtomicI32::new(0),
            pending_limit_bytes: AtomicUsize::new(1),
            flush_delay_ms: AtomicU64::new(1),
            force_ratio_bits: AtomicU64::new(0.85f64.to_bits()),
        }
    }
}

impl DocAdaptiveState {
    fn init(&self, pending_limit: usize, flush_delay: Duration, now_ms: i64, force_ratio: f64) {
        self.pending_limit_bytes
            .store(pending_limit.max(1), Ordering::Relaxed);
        self.flush_delay_ms
            .store(duration_to_millis(flush_delay), Ordering::Relaxed);
        self.timing.last_access_ms.store(now_ms, Ordering::Relaxed);
        self.timing.last_flush_ms.store(now_ms, Ordering::Relaxed);
        self.force_ratio_bits
            .store(force_ratio.clamp(0.1, 0.99).to_bits(), Ordering::Relaxed);
    }

    fn touch_access(&self) {
        self.timing
            .last_access_ms
            .store(current_time_millis(), Ordering::Relaxed);
    }

    fn pending_limit(&self) -> usize {
        self.pending_limit_bytes.load(Ordering::Relaxed).max(1)
    }

    fn force_ratio(&self) -> f64 {
        f64::from_bits(self.force_ratio_bits.load(Ordering::Relaxed)).clamp(0.1, 0.99)
    }

    fn set_force_ratio(&self, value: f64) {
        self.force_ratio_bits
            .store(value.clamp(0.1, 0.99).to_bits(), Ordering::Relaxed);
    }

    fn refresh_thresholds(&self, adaptive: &DocCacheAdaptiveConfig, stats: &DocStats) {
        let desired_pending = stats.desired_pending(adaptive);
        let pending_clamped = desired_pending
            .min(adaptive.target_pending_bytes as f64)
            .round()
            .clamp(
                adaptive.min_pending_bytes as f64,
                adaptive.max_pending_bytes as f64,
            ) as usize;
        self.pending_limit_bytes
            .store(pending_clamped.max(1), Ordering::Relaxed);

        let flush_secs = stats.desired_flush_secs(adaptive, pending_clamped).clamp(
            adaptive.min_flush_delay.as_secs_f64(),
            adaptive.max_flush_delay.as_secs_f64(),
        );
        let flush_duration = Duration::from_secs_f64(flush_secs);
        self.flush_delay_ms
            .store(duration_to_millis(flush_duration), Ordering::Relaxed);

        self.adjust_force_ratio(adaptive, pending_clamped);
    }

    fn pending_limit_bytes(&self) -> usize {
        self.pending_limit_bytes.load(Ordering::Relaxed).max(1)
    }

    fn current_flush_delay(&self, fallback: Duration) -> Duration {
        let millis = self.flush_delay_ms.load(Ordering::Relaxed);
        if millis == 0 {
            fallback
        } else {
            Duration::from_millis(millis)
        }
    }

    fn adjust_force_ratio(&self, adaptive: &DocCacheAdaptiveConfig, pending_limit: usize) {
        let target_secs = adaptive.target_flush_interval.as_secs_f64().max(0.050);
        let last_flush = self.timing.last_flush_ms.load(Ordering::Relaxed);
        let now_ms = current_time_millis();
        let actual_secs = ((now_ms - last_flush).max(1) as f64) / 1000.0;
        let util = (actual_secs / target_secs).clamp(0.1, 3.0);
        let pending_ratio = pending_limit as f64 / adaptive.target_pending_bytes.max(1) as f64;

        let current = self.force_ratio();
        let mut next = current * (1.0 - 0.35 * (util - 1.0));
        next += 0.05 * (pending_ratio - 1.0);
        next = next.clamp(0.4, 0.95);
        self.set_force_ratio(next);
    }

    fn should_force_flush(
        &self,
        pending_bytes: usize,
        now_ms: i64,
        force_bytes: usize,
        max_interval: Duration,
    ) -> bool {
        let pending_limit = self.pending_limit_bytes();
        if pending_bytes >= pending_limit {
            return true;
        }

        let trigger = (pending_limit as f64 * self.force_ratio()).round() as usize;
        if trigger > 0 && pending_bytes >= trigger {
            return true;
        }

        if pending_bytes >= force_bytes {
            return true;
        }

        self.should_flush_by_time(now_ms, max_interval)
    }

    fn should_flush_by_time(&self, now_ms: i64, max_interval: Duration) -> bool {
        let last_flush = self.timing.last_flush_ms.load(Ordering::Relaxed);
        now_ms - last_flush >= max_interval.as_millis() as i64
    }

    fn notify_flush(&self) {
        self.timing
            .last_flush_ms
            .store(current_time_millis(), Ordering::Relaxed);
    }

    fn is_idle(&self, now_ms: i64, idle_after_ms: i64) -> bool {
        if self.active_sessions.load(Ordering::Relaxed) > 0 {
            return false;
        }

        let last = self.timing.last_access_ms.load(Ordering::Relaxed);
        now_ms - last >= idle_after_ms
    }

    fn has_active_sessions(&self) -> bool {
        self.active_sessions.load(Ordering::Relaxed) > 0
    }

    fn increment_sessions(&self) {
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
    }

    fn decrement_sessions(&self) -> bool {
        let prev = self.active_sessions.fetch_sub(1, Ordering::SeqCst);
        prev <= 1
    }

    fn schedule_flush(
        &self,
        handle: Arc<DocHandle>,
        flush_delay: Duration,
        workspace_store: DocumentStore,
        userspace_store: UserDocStore,
        metrics: DocCacheMetrics,
    ) {
        if flush_delay.is_zero() {
            tokio::spawn(async move {
                if let Err(err) = handle
                    .flush_if_needed(&workspace_store, &userspace_store, &metrics)
                    .await
                {
                    metrics.record_flush_error();
                    warn!(
                        space_type = ?handle.descriptor.space_type,
                        space_id = %handle.descriptor.space_id,
                        doc_id = %handle.descriptor.doc_id,
                        error = %err,
                        "failed to flush document (immediate)",
                    );
                }
            });
            return;
        }

        if self
            .flush_scheduled
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let owned = Arc::clone(&handle);
        tokio::spawn(async move {
            sleep(flush_delay).await;
            if let Err(err) = owned
                .flush_if_needed(&workspace_store, &userspace_store, &metrics)
                .await
            {
                metrics.record_flush_error();
                warn!(
                    space_type = ?owned.descriptor.space_type,
                    space_id = %owned.descriptor.space_id,
                    doc_id = %owned.descriptor.doc_id,
                    error = %err,
                    "failed to flush document (delayed)",
                );
            }
            owned
                .adaptive
                .flush_scheduled
                .store(false, Ordering::SeqCst);
        });
    }
}

fn extract_doc_updated_at(doc: &YoctoDoc, doc_id: &str) -> Option<i64> {
    let map = doc.get_map("meta").ok()?;
    let value = map.get("pages")?;
    let json = serde_json::to_value(&value).ok()?;
    parse_updated_at(&json, doc_id)
}

fn parse_updated_at(json: &JsonValue, doc_id: &str) -> Option<i64> {
    let array = json.as_array()?;
    for entry in array {
        let obj = entry.as_object()?;
        if obj.get("id").and_then(JsonValue::as_str) != Some(doc_id) {
            continue;
        }

        if let Some(updated) = obj.get("updatedDate").and_then(number_to_millis) {
            return Some(updated);
        }

        if let Some(created) = obj.get("createDate").and_then(number_to_millis) {
            return Some(created);
        }
    }

    None
}

fn number_to_millis(value: &JsonValue) -> Option<i64> {
    if let Some(int) = value.as_i64() {
        return Some(int);
    }

    value.as_f64().map(|float| float.round() as i64)
}

fn current_time_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl DocStats {
    fn new(now_ms: i64) -> Self {
        Self {
            last_update_ms: now_ms,
            ema_rate_per_sec: 0.0,
            ema_update_bytes: 0.0,
        }
    }

    fn record_updates(
        &mut self,
        update_count: usize,
        appended_bytes: usize,
        now_ms: i64,
        alpha: f64,
    ) {
        if update_count == 0 {
            self.last_update_ms = now_ms;
            return;
        }

        let delta_ms = (now_ms - self.last_update_ms).max(1);
        let rate = (update_count as f64) * 1000.0 / delta_ms as f64;
        let avg_bytes = appended_bytes as f64 / update_count as f64;
        self.last_update_ms = now_ms;
        self.ema_rate_per_sec = blend(self.ema_rate_per_sec, rate, alpha);
        self.ema_update_bytes = blend(self.ema_update_bytes, avg_bytes, alpha);
    }

    fn desired_pending(&self, adaptive: &DocCacheAdaptiveConfig) -> f64 {
        if self.ema_rate_per_sec > RATE_EPSILON && self.ema_update_bytes > RATE_EPSILON {
            self.ema_rate_per_sec
                * self.ema_update_bytes
                * adaptive.target_flush_interval.as_secs_f64()
        } else {
            adaptive.target_pending_bytes as f64
        }
    }

    fn desired_flush_secs(&self, adaptive: &DocCacheAdaptiveConfig, pending_limit: usize) -> f64 {
        if self.ema_rate_per_sec > RATE_EPSILON && self.ema_update_bytes > RATE_EPSILON {
            (pending_limit as f64 / (self.ema_rate_per_sec * self.ema_update_bytes))
                .min(adaptive.target_flush_interval.as_secs_f64())
        } else {
            adaptive.max_flush_delay.as_secs_f64()
        }
    }
}

fn blend(previous: f64, sample: f64, alpha: f64) -> f64 {
    if previous == 0.0 {
        sample
    } else {
        (alpha * sample) + ((1.0 - alpha) * previous)
    }
}

fn duration_to_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)).max(1u128) as u64
}

fn compress_snapshot(bytes: &[u8]) -> DocCacheResult<Vec<u8>> {
    let mut encoder = SnapEncoder::new();
    encoder
        .compress_vec(bytes)
        .context("compress document snapshot with Snappy")
}

fn decompress_snapshot(bytes: &[u8]) -> DocCacheResult<Vec<u8>> {
    let mut decoder = SnapDecoder::new();
    decoder
        .decompress_vec(bytes)
        .context("decompress document snapshot with Snappy")
}

#[cfg(test)]
impl DocCache {
    pub async fn debug_entry_count(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_entry_plain_roundtrip() {
        let src = vec![1u8, 2, 3, 4, 5];
        let entry = SnapshotEntry::new(src.clone(), SnapshotStorageMode::Plain).unwrap();
        assert_eq!(entry.len(), src.len());
        assert!(matches!(entry.encoding, SnapshotEncoding::Plain));
        let decoded = entry.to_plain_vec().unwrap();
        assert_eq!(decoded, src);
    }

    #[test]
    fn snapshot_entry_compressed_roundtrip() {
        let src = vec![42u8; 16 * 1024];
        let entry = SnapshotEntry::new(src.clone(), SnapshotStorageMode::Compressed).unwrap();
        assert!(entry.len() <= src.len());
        assert!(matches!(entry.encoding, SnapshotEncoding::Compressed));
        let decoded = entry.to_plain_vec().unwrap();
        assert_eq!(decoded, src);
    }
}
