use std::time::{Duration, Instant};

use anyhow::Result;
use sysinfo::{Pid, ProcessExt, System, SystemExt};
use tokio::{sync::oneshot, task::JoinHandle};

#[derive(Debug, Clone, Copy)]
pub struct ProcessSample {
    pub timestamp_ms: u64,
    pub rss_bytes: u64,
    pub virtual_bytes: u64,
    pub cpu_percent: f32,
}

pub struct SamplerHandle {
    stop: Option<oneshot::Sender<()>>,
    join: JoinHandle<Vec<ProcessSample>>,
}

pub fn spawn(pid: i32, interval: Duration, start: Instant) -> SamplerHandle {
    let (tx, rx) = oneshot::channel();
    let join = tokio::spawn(async move {
        let mut sys = System::new();
        let pid = Pid::from(pid as usize);
        let mut ticker = tokio::time::interval(interval);
        let mut samples = Vec::new();
        tokio::pin!(rx);
        loop {
            tokio::select! {
                _ = &mut rx => {
                    break;
                }
                _ = ticker.tick() => {
                    sys.refresh_process(pid);
                    if let Some(process) = sys.process(pid) {
                        samples.push(ProcessSample {
                            timestamp_ms: start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                            rss_bytes: process.memory() * 1024,
                            virtual_bytes: process.virtual_memory() * 1024,
                            cpu_percent: process.cpu_usage(),
                        });
                    }
                }
            }
        }
        samples
    });

    SamplerHandle {
        stop: Some(tx),
        join,
    }
}

impl SamplerHandle {
    pub async fn stop(mut self) -> Result<Vec<ProcessSample>> {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        let samples = self.join.await?;
        Ok(samples)
    }
}
