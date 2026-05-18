//! Persistent job queue backed by NATS JetStream.
//!
//! - Stream `mindbox-jobs` (work_queue retention), subject `mindbox.jobs.submit`
//! - KV bucket `mindbox-results` holds per-job state (pending/running/done/failed)
//! - Pull consumer with durable name `mindbox-worker` — multiple api-rust
//!   instances all subscribe to the same name and compete for jobs.
//!
//! Wire model: POST /jobs → publish + initial KV put; consumer task in api-rust
//! pulls msg, marks running, dispatches to exec_direct, writes result, acks.
//! GET /jobs/<id> reads from KV. DELETE /jobs/<id> marks cancelled (consumer
//! checks at start of processing).

use anyhow::{anyhow, Result};
use async_nats::jetstream::{self, kv, stream};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

const STREAM_NAME: &str = "mindbox-jobs";
const SUBMIT_SUBJECT: &str = "mindbox.jobs.submit";
const STREAM_SUBJECTS: &str = "mindbox.jobs.>";
const KV_BUCKET: &str = "mindbox-results";
pub const CONSUMER_NAME: &str = "mindbox-worker";

#[derive(Clone)]
pub struct Jobs {
    pub js: jetstream::Context,
    pub kv: kv::Store,
    pub stream: jetstream::stream::Stream,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JobSpec {
    pub job_id: String,
    pub template: String,
    pub code: String,
    #[serde(default = "default_timeout")]
    pub timeout: u32,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub files: HashMap<String, String>,
    #[serde(default)]
    pub persist_changes: bool,
    #[serde(default)]
    pub persist_root_label: String,
}
fn default_timeout() -> u32 { 10 }

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JobState {
    pub job_id: String,
    pub status: String, // pending | running | done | failed | cancelled
    pub template: String,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<JobResult>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JobResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub elapsed_ms: u64,
    pub container_id: String,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub output_files: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deleted_files: Vec<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub output_files_b64: HashMap<String, String>,
}

pub async fn connect(url: &str) -> Result<Jobs> {
    let client = async_nats::connect(url).await
        .map_err(|e| anyhow!("nats connect {}: {}", url, e))?;
    let js = jetstream::new(client);
    let s = js.get_or_create_stream(stream::Config {
        name: STREAM_NAME.into(),
        subjects: vec![STREAM_SUBJECTS.into()],
        retention: stream::RetentionPolicy::WorkQueue,
        max_age: Duration::from_secs(24 * 3600),
        ..Default::default()
    }).await.map_err(|e| anyhow!("create stream: {}", e))?;
    let kv = js.create_key_value(kv::Config {
        bucket: KV_BUCKET.into(),
        history: 1,
        max_age: Duration::from_secs(24 * 3600),
        ..Default::default()
    }).await.map_err(|e| anyhow!("create kv: {}", e))?;
    Ok(Jobs { js, kv, stream: s })
}

pub async fn publish_job(jobs: &Jobs, spec: &JobSpec) -> Result<()> {
    let body = serde_json::to_vec(spec)?;
    let ack = jobs.js.publish(SUBMIT_SUBJECT.to_string(), body.into())
        .await.map_err(|e| anyhow!("publish: {}", e))?;
    ack.await.map_err(|e| anyhow!("publish ack: {}", e))?;
    Ok(())
}

pub async fn put_state(jobs: &Jobs, st: &JobState) -> Result<()> {
    let body = serde_json::to_vec(st)?;
    jobs.kv.put(&st.job_id, body.into()).await
        .map_err(|e| anyhow!("kv put: {}", e))?;
    Ok(())
}

pub async fn get_state(jobs: &Jobs, job_id: &str) -> Result<Option<JobState>> {
    let bytes = match jobs.kv.get(job_id).await
        .map_err(|e| anyhow!("kv get: {}", e))? {
        Some(b) => b,
        None => return Ok(None),
    };
    let st: JobState = serde_json::from_slice(&bytes)?;
    Ok(Some(st))
}

pub fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

pub fn new_job_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    // 16 hex chars from nanos + 8 random
    let rand: u32 = rand_u32();
    format!("j{:016x}{:08x}", nanos as u64, rand)
}

fn rand_u32() -> u32 {
    // We don't need crypto strength — just enough to disambiguate jobs created in
    // the same nanosecond. Avoid pulling rand crate.
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().subsec_nanos();
    n.wrapping_mul(2654435761)
}
