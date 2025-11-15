use std::sync::Arc;

use barffine_core::{
    db::doc_repo::CompactionSource,
    doc::DocEngine,
    doc_store::{DocumentCompactionJob, DocumentStore},
    doc_update_log::DocUpdateRecord,
};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use yrs::{Doc, ReadTxn, StateVector, Text, Transact};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("doc_apply_updates_100x64", |b| {
        let (base_snapshot, updates) = sample_updates(100, 64);
        b.iter(|| {
            let merged = DocEngine::apply_updates_to_snapshot(
                Some(base_snapshot.as_slice()),
                black_box(&updates),
            )
            .expect("apply updates");
            black_box(merged);
        });
    });

    c.bench_function("compute_compaction_plan_100x64", |b| {
        let job = sample_compaction_job(100, 64);
        b.iter_batched(
            || job.clone(),
            |job| {
                let params = DocumentStore::compute_compaction_plan(job).expect("plan");
                black_box(params);
            },
            BatchSize::SmallInput,
        );
    });
}

fn sample_updates(count: usize, chunk_len: usize) -> (Vec<u8>, Vec<Vec<u8>>) {
    let doc = Doc::new();
    let name: Arc<str> = Arc::from("bench-text");
    let text = doc.get_or_insert_text(name.clone());

    let mut txn = doc.transact_mut();
    text.insert(&mut txn, 0, "seed");
    let mut state_vector = txn.state_vector();
    let base_snapshot = txn.encode_state_as_update_v1(&StateVector::default());
    drop(txn);

    let mut updates = Vec::with_capacity(count);
    for idx in 0..count {
        let mut txn = doc.transact_mut();
        let len = text.len(&txn);
        let chunk = format!("chunk-{idx:04}{}", "x".repeat(chunk_len));
        text.insert(&mut txn, len, &chunk);
        updates.push(txn.encode_state_as_update_v1(&state_vector));
        state_vector = txn.state_vector();
    }

    (base_snapshot, updates)
}

fn sample_compaction_job(count: usize, chunk_len: usize) -> DocumentCompactionJob {
    let (base_snapshot, updates) = sample_updates(count, chunk_len);
    let logs = updates
        .into_iter()
        .enumerate()
        .map(|(idx, update)| DocUpdateRecord {
            id: idx as i64,
            update,
            created_at: idx as i64 + 1,
            editor_id: Some("bench".to_string()),
        })
        .collect();
    DocumentCompactionJob {
        workspace_id: "ws-bench".to_string(),
        doc_id: "doc-bench".to_string(),
        source: CompactionSource {
            base_snapshot,
            logs,
            doc_updated_at: 0,
        },
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
