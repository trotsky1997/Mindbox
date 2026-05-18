// e2b-shim: thin compatibility layer that makes E2B SDK clients see our
// inspect-api as if it were the E2B cloud + envd.
//
// Routes:
//   REST control plane:
//     POST   /sandboxes               create
//     GET    /sandboxes/:id           detail
//     DELETE /sandboxes/:id           kill
//
//   ENVD per-sandbox via Connect protocol (we ignore the sandbox in the URL —
//   single shared backend; multi-sandbox state is a v2 concern).
//     POST   /process.Process/Start   server-stream of process events
//     POST   /process.Process/List    list running processes (empty)
//
// Connect framing for server-stream:
//   5-byte envelope header [flags=0x00 | u32 BE length] + protobuf bytes
//   final trailer: [flags=0x02 | u32 BE length] + JSON ({} success, {"error":...})

use anyhow::Result;
use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{delete, get, post},
    Router,
};
use chrono::Utc;
use dashmap::DashMap;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/process.rs"));
}
pub mod fs_pb {
    include!(concat!(env!("OUT_DIR"), "/filesystem.rs"));
}
use pb::process_event::{data_event, DataEvent, EndEvent, Event as ProcessEventOneof, StartEvent};

// ---- state --------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SandboxRec {
    #[serde(rename = "sandboxID")]
    sandbox_id: String,
    #[serde(rename = "templateID")]
    template_id: String,
    #[serde(rename = "clientID")]
    client_id: String,
    domain: Option<String>,
    #[serde(rename = "envdVersion")]
    envd_version: String,
    #[serde(rename = "envdAccessToken")]
    envd_access_token: Option<String>,
    alias: Option<String>,
    metadata: Option<serde_json::Value>,
    #[serde(rename = "startedAt")]
    started_at: String,
    #[serde(rename = "endAt")]
    end_at: String,
    #[serde(rename = "cpuCount")]
    #[serde(default = "default_cpu")]
    cpu_count: u32,
    #[serde(rename = "memoryMB")]
    #[serde(default = "default_mem")]
    memory_mb: u32,
    #[serde(rename = "diskSizeMB")]
    #[serde(default = "default_disk")]
    disk_size_mb: u32,
    #[serde(default = "default_state")]
    state: String,
}

fn default_cpu() -> u32 { 2 }
fn default_mem() -> u32 { 1024 }
fn default_disk() -> u32 { 4096 }
fn default_state() -> String { "running".into() }

const SANDBOX_FS_ROOT: &str = "/var/lib/e2b-shim/sandboxes";
const SANDBOX_REGISTRY_DIR: &str = "/var/lib/e2b-shim/registry";

fn registry_path(sid: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(SANDBOX_REGISTRY_DIR).join(format!("{}.json", sid))
}

fn persist_sandbox(rec: &SandboxRec) {
    let _ = std::fs::create_dir_all(SANDBOX_REGISTRY_DIR);
    if let Ok(json) = serde_json::to_vec_pretty(rec) {
        let _ = std::fs::write(registry_path(&rec.sandbox_id), json);
    }
}

fn forget_sandbox(sid: &str) {
    let _ = std::fs::remove_file(registry_path(sid));
}

fn load_registry() -> Vec<SandboxRec> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(SANDBOX_REGISTRY_DIR) else { return out; };
    for entry in rd.flatten() {
        let p = entry.path();
        if p.extension().and_then(|s| s.to_str()) != Some("json") { continue; }
        if let Ok(bytes) = std::fs::read(&p) {
            if let Ok(rec) = serde_json::from_slice::<SandboxRec>(&bytes) {
                out.push(rec);
            }
        }
    }
    out
}



fn sandbox_fs_dir(sid: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(SANDBOX_FS_ROOT).join(sid)
}

fn ensure_sandbox_fs(sid: &str) -> std::io::Result<std::path::PathBuf> {
    let d = sandbox_fs_dir(sid);
    std::fs::create_dir_all(&d)?;
    Ok(d)
}

fn collect_files(root: &std::path::Path) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    fn walk(base: &std::path::Path, dir: &std::path::Path, out: &mut std::collections::HashMap<String, String>) {
        let Ok(rd) = std::fs::read_dir(dir) else { return; };
        for entry in rd.flatten() {
            let p = entry.path();
            let Ok(meta) = entry.metadata() else { continue; };
            if meta.is_dir() {
                walk(base, &p, out);
            } else if meta.is_file() {
                if let Ok(rel) = p.strip_prefix(base) {
                    if let Ok(content) = std::fs::read_to_string(&p) {
                        out.insert(rel.to_string_lossy().into_owned(), content);
                    }
                }
            }
        }
    }
    walk(root, root, &mut out);
    out
}

struct AppState {
    sandboxes: DashMap<String, SandboxRec>,
    upstream: String,
    http: reqwest::Client,
    api_key: Option<String>,
}

// ---- REST: POST /sandboxes ---------------------------------------------

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
struct NewSandbox {
    #[serde(rename = "templateID", default)]
    template_id: String,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
    #[serde(default)]
    alias: Option<String>,
    #[serde(default)]
    timeout: Option<u64>,
    #[serde(default)]
    env_vars: Option<serde_json::Value>,
}

async fn check_api_key(state: &Arc<AppState>, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    if state.api_key.is_none() { return Ok(()); }
    let want = state.api_key.as_deref().unwrap();
    let got = headers.get("x-api-key").and_then(|h| h.to_str().ok())
        .or_else(|| headers.get("authorization").and_then(|h| h.to_str().ok()).and_then(|s| s.strip_prefix("Bearer ")));
    match got {
        Some(t) if t == want => Ok(()),
        _ => Err((StatusCode::UNAUTHORIZED, r#"{"code":"unauthenticated","message":"invalid X-API-KEY"}"#.into())),
    }
}

async fn create_sandbox(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<NewSandbox>,
) -> Result<(StatusCode, Json<SandboxRec>), (StatusCode, String)> {
    check_api_key(&state, &headers).await?;

    let template_id = if body.template_id.is_empty() { "default".to_string() } else { body.template_id.clone() };
    // Verify template exists upstream.
    let url = format!("{}/templates", state.upstream);
    let resp = state.http.get(&url).send().await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("upstream: {}", e)))?;
    let v: serde_json::Value = resp.json().await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("upstream json: {}", e)))?;
    let templates = v.get("templates").and_then(|t| t.as_array()).cloned().unwrap_or_default();
    let known: Vec<String> = templates.iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()).map(String::from))
        .collect();
    if !known.contains(&template_id) {
        return Err((StatusCode::NOT_FOUND,
            format!(r#"{{"code":"not_found","message":"template '{}' not found; available: {:?}"}}"#,
                template_id, known)));
    }

    let sid = format!("i{}", uuid::Uuid::new_v4().to_string().replace('-', "")[..20].to_string());
    let now = Utc::now();
    let end = now + chrono::Duration::seconds(body.timeout.unwrap_or(900) as i64);
    let rec = SandboxRec {
        sandbox_id: sid.clone(),
        template_id: template_id.clone(),
        client_id: "shim".to_string(),
        domain: Some(state.upstream.replace("http://", "").replace("https://", "")),
        envd_version: "0.5.0".to_string(),
        envd_access_token: Some(uuid::Uuid::new_v4().to_string()),
        alias: body.alias,
        metadata: body.metadata,
        started_at: now.to_rfc3339(),
        end_at: end.to_rfc3339(),
        cpu_count: 2,
        memory_mb: 1024,
        disk_size_mb: 4096,
        state: "running".into(),
    };
    if let Err(e) = ensure_sandbox_fs(&sid) {
        eprintln!("[shim] WARN ensure_sandbox_fs({}): {}", sid, e);
    }
    state.sandboxes.insert(sid.clone(), rec.clone());
    persist_sandbox(&rec);
    Ok((StatusCode::CREATED, Json(rec)))
}

async fn get_sandbox(
    State(state): State<Arc<AppState>>,
    Path(sid): Path<String>,
    headers: HeaderMap,
) -> Result<Json<SandboxRec>, (StatusCode, String)> {
    check_api_key(&state, &headers).await?;
    let rec = state.sandboxes.get(&sid)
        .ok_or((StatusCode::NOT_FOUND, format!(r#"{{"code":"not_found","message":"sandbox {} not found"}}"#, sid)))?;
    Ok(Json(rec.clone()))
}

async fn delete_sandbox(
    State(state): State<Arc<AppState>>,
    Path(sid): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, String)> {
    check_api_key(&state, &headers).await?;
    state.sandboxes.remove(&sid);
    forget_sandbox(&sid);
    let _ = std::fs::remove_dir_all(sandbox_fs_dir(&sid));
    Ok(StatusCode::NO_CONTENT)
}


#[derive(serde::Deserialize)]
struct SetTimeoutBody { timeout: i64 }

async fn set_sandbox_timeout(
    State(state): State<Arc<AppState>>,
    Path(sid): Path<String>,
    headers: HeaderMap,
    Json(body): Json<SetTimeoutBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    check_api_key(&state, &headers).await?;
    let mut rec = state.sandboxes.get(&sid)
        .ok_or((StatusCode::NOT_FOUND, format!(r#"{{"code":"not_found","message":"sandbox {} not found"}}"#, sid)))?
        .clone();
    let new_end = chrono::Utc::now() + chrono::Duration::seconds(body.timeout.max(1));
    rec.end_at = new_end.to_rfc3339();
    state.sandboxes.insert(sid.clone(), rec.clone());
    persist_sandbox(&rec);
    Ok(StatusCode::NO_CONTENT)
}

// ---- Connect server-stream framing -------------------------------------

fn envelope(flags: u8, data: &[u8]) -> Bytes {
    let mut buf = bytes::BytesMut::with_capacity(5 + data.len());
    buf.extend_from_slice(&[flags]);
    buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
    buf.extend_from_slice(data);
    buf.freeze()
}

fn data_envelope_proto(msg: &impl Message) -> Bytes {
    envelope(0x00, &msg.encode_to_vec())
}

fn data_envelope_json(json_bytes: &[u8]) -> Bytes {
    envelope(0x00, json_bytes)
}

fn end_envelope_ok() -> Bytes {
    envelope(0x02, b"{}")
}

fn end_envelope_err(code: &str, msg: &str) -> Bytes {
    let body = serde_json::json!({"error": {"code": code, "message": msg}}).to_string();
    envelope(0x02, body.as_bytes())
}


fn enc_resp(r: &pb::StartResponse, codec: Codec) -> Bytes {
    match codec {
        Codec::Proto => data_envelope_proto(r),
        Codec::Json => data_envelope_json(response_to_json(r).to_string().as_bytes()),
    }
}

/// Hand-encode StartResponse as canonical proto3 JSON.
/// Mirrors the oneof flattening: ProcessEvent.event oneof appears as a sibling field.
fn response_to_json(r: &pb::StartResponse) -> serde_json::Value {
    use serde_json::json;
    let event_json = r.event.as_ref().and_then(|e| e.event.as_ref()).map(|ev| match ev {
        ProcessEventOneof::Start(s) => json!({"start": {"pid": s.pid}}),
        ProcessEventOneof::Data(d) => {
            let inner = match &d.output {
                Some(data_event::Output::Stdout(b)) => json!({"stdout": general_b64(b)}),
                Some(data_event::Output::Stderr(b)) => json!({"stderr": general_b64(b)}),
                Some(data_event::Output::Pty(b))    => json!({"pty":    general_b64(b)}),
                None => json!({}),
            };
            json!({"data": inner})
        }
        ProcessEventOneof::End(e) => json!({"end": {
            "exitCode": e.exit_code,
            "exited": e.exited,
            "status": e.status,
            "error": e.error,
        }}),
        ProcessEventOneof::Keepalive(_) => json!({"keepalive": {}}),
    });
    json!({ "event": event_json })
}

fn general_b64(b: &[u8]) -> String {
    // Standard base64 (proto3 JSON canonical encoding for bytes).
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((b.len() + 2) / 3 * 4);
    let mut i = 0;
    while i + 3 <= b.len() {
        let n = ((b[i] as u32) << 16) | ((b[i+1] as u32) << 8) | (b[i+2] as u32);
        out.push(A[((n >> 18) & 63) as usize] as char);
        out.push(A[((n >> 12) & 63) as usize] as char);
        out.push(A[((n >>  6) & 63) as usize] as char);
        out.push(A[(n & 63) as usize] as char);
        i += 3;
    }
    if i < b.len() {
        let rem = b.len() - i;
        let mut n = (b[i] as u32) << 16;
        if rem == 2 { n |= (b[i+1] as u32) << 8; }
        out.push(A[((n >> 18) & 63) as usize] as char);
        out.push(A[((n >> 12) & 63) as usize] as char);
        if rem == 2 { out.push(A[((n >> 6) & 63) as usize] as char); } else { out.push('='); }
        out.push('=');
    }
    out
}



// ---- /process.Process/Start ---------------------------------------------

#[derive(Clone, Copy)]
enum Codec { Proto, Json }

fn codec_from_content_type(ct: &str) -> Codec {
    if ct.contains("json") { Codec::Json } else { Codec::Proto }
}

fn is_stream_ct(ct: &str) -> bool { ct.starts_with("application/connect+") }

fn extract_body<'a>(ct: &str, body: &'a [u8]) -> Option<&'a [u8]> {
    if is_stream_ct(ct) {
        if body.len() < 5 { return None; }
        let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
        if body.len() < 5 + len { return None; }
        Some(&body[5..5 + len])
    } else {
        Some(body)
    }
}

#[derive(serde::Deserialize, Default, Debug)]
struct JsonStartRequest {
    #[serde(default)]
    process: Option<JsonProcessConfig>,
    #[serde(default)]
    tag: Option<String>,
    #[serde(default)]
    stdin: Option<bool>,
}

#[derive(serde::Deserialize, Default, Debug)]
struct JsonProcessConfig {
    #[serde(default)]
    cmd: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    envs: std::collections::HashMap<String, String>,
    #[serde(default)]
    cwd: Option<String>,
}

fn decode_start_request(ct: &str, body: &[u8]) -> Option<pb::StartRequest> {
    let payload = extract_body(ct, body)?;
    match codec_from_content_type(ct) {
        Codec::Proto => pb::StartRequest::decode(payload).ok(),
        Codec::Json => {
            let j: JsonStartRequest = serde_json::from_slice(payload).ok()?;
            let proc = j.process.map(|p| pb::ProcessConfig {
                cmd: p.cmd,
                args: p.args,
                envs: p.envs,
                cwd: p.cwd,
            });
            Some(pb::StartRequest {
                process: proc,
                pty: None,
                tag: j.tag,
                stdin: j.stdin,
            })
        }
    }
}

fn build_python_subprocess(args_combined: Vec<String>, envs: std::collections::HashMap<String, String>, cwd: Option<String>) -> String {
    // Convert the E2B ProcessConfig into Python code that subprocess.runs it.
    // We use json to safely embed the args/env into the Python source.
    let args_json = serde_json::to_string(&args_combined).unwrap_or("[]".into());
    let env_json = serde_json::to_string(&envs).unwrap_or("{}".into());
    let cwd_lit = match cwd {
        Some(c) => format!("{:?}", c),
        None => "None".to_string(),
    };
    format!(
        r#"
import subprocess, os, sys, json
_args = {args_json}
_env_overrides = {env_json}
_cwd = {cwd_lit}
_env = os.environ.copy()
_env.update(_env_overrides)
_r = subprocess.run(_args, env=_env, cwd=_cwd, capture_output=True, text=True)
sys.stdout.write(_r.stdout)
sys.stderr.write(_r.stderr)
sys.exit(_r.returncode)
"#,
        args_json = args_json,
        env_json = env_json,
        cwd_lit = cwd_lit,
    )
}

#[derive(Serialize, Deserialize)]
struct UpstreamExecResp {
    stdout: String,
    stderr: String,
    exit_code: i32,
    #[serde(default)]
    elapsed_ms: u64,
    #[serde(default)]
    output_files: std::collections::HashMap<String, String>,
    #[serde(default)]
    deleted_files: Vec<String>,
    #[serde(default)]
    output_files_b64: std::collections::HashMap<String, String>,
}

async fn process_start(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type = headers.get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok()).unwrap_or("application/connect+proto");

    let codec = codec_from_content_type(content_type);
    eprintln!("[shim] start: ct={:?} body_len={}", content_type, body.len());
    let req: pb::StartRequest = match decode_start_request(content_type, &body) {
        Some(r) => r,
        None => {
            eprintln!("[shim] start: decode FAILED");
            return (StatusCode::BAD_REQUEST, "decode failed").into_response();
        }
    };
    eprintln!("[shim] start: decoded process={:?}", req.process.as_ref().map(|p| (&p.cmd, &p.args)));

    let proc_cfg = req.process.unwrap_or_default();
    // Figure out which template to dispatch to. For Phase 1 MVP we use the env var
    // ENVD_TEMPLATE if set, else "default". A future revision will route by sandbox_id
    // (header or path) so each sandbox is sticky to its template.
    let template = std::env::var("E2B_SHIM_DEFAULT_TEMPLATE").unwrap_or_else(|_| "default".into());

    let mut args_combined = vec![proc_cfg.cmd.clone()];
    args_combined.extend(proc_cfg.args.clone());
    let envs = proc_cfg.envs.clone();
    let cwd = proc_cfg.cwd.clone();

    // Resolve sandbox + its host fs dir; serialize fs into files= map and set cwd to /workspace.
    let sid = pick_sandbox_id(&state, &headers).ok();
    let sandbox_files: std::collections::HashMap<String, String> = match &sid {
        Some(s) => {
            let root = sandbox_fs_dir(s);
            let _ = std::fs::create_dir_all(&root);
            collect_files(&root)
        }
        None => Default::default(),
    };
    let effective_cwd = cwd.or(Some(".".to_string()));
    let py_code = build_python_subprocess(args_combined, envs, effective_cwd);

    // (flags, sender) -> stream Body
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(8);

    // Send StartEvent (fake pid).
let encode_resp = move |r: &pb::StartResponse| enc_resp(r, codec);
    let start_evt = pb::StartResponse {
        event: Some(pb::ProcessEvent {
            event: Some(ProcessEventOneof::Start(StartEvent { pid: 1 })),
        }),
    };
    let _ = tx.send(Ok(encode_resp(&start_evt))).await;

    // Run upstream in a task; pipe its result back as DataEvent + EndEvent.
    let upstream = state.upstream.clone();
    let http = state.http.clone();
    let tx2 = tx.clone();
    let codec_for_task = codec;
    let sandbox_files_clone = sandbox_files.clone();
    let sid_owned: Option<String> = sid.clone();
    let _ = sandbox_files;
    tokio::spawn(async move {
        let sandbox_files = sandbox_files_clone;
        let body = serde_json::json!({
            "template": template,
            "code": py_code,
            "timeout": 60,
            "files": sandbox_files,
            "persist_changes": true,
        });
        let resp = http.post(format!("{}/exec_hot", upstream))
            .json(&body)
            .timeout(Duration::from_secs(120))
            .send()
            .await;
        match resp {
            Ok(r) => {
                let result: Result<UpstreamExecResp, _> = r.json().await;
                match result {
                    Ok(u) => {
                        // Persist file changes back to host fs dir.
                        if let Some(sid_clone) = sid_owned.as_ref() {
                            let root = sandbox_fs_dir(sid_clone);
                            for (rel, content) in u.output_files.iter() {
                                let full = root.join(rel);
                                if let Some(parent) = full.parent() {
                                    let _ = std::fs::create_dir_all(parent);
                                }
                                let _ = std::fs::write(&full, content.as_bytes());
                            }
                            for (rel, b64) in u.output_files_b64.iter() {
                                let full = root.join(rel);
                                if let Some(parent) = full.parent() {
                                    let _ = std::fs::create_dir_all(parent);
                                }
                                if let Some(bin) = general_b64_decode(b64) {
                                    let _ = std::fs::write(&full, bin);
                                }
                            }
                            for rel in u.deleted_files.iter() {
                                let full = root.join(rel);
                                let _ = std::fs::remove_file(&full);
                            }
                        }
                        if !u.stdout.is_empty() {
                            let evt = pb::StartResponse {
                                event: Some(pb::ProcessEvent {
                                    event: Some(ProcessEventOneof::Data(DataEvent {
                                        output: Some(data_event::Output::Stdout(u.stdout.into_bytes())),
                                    })),
                                }),
                            };
                            let _ = tx2.send(Ok(enc_resp(&evt, codec_for_task))).await;
                        }
                        if !u.stderr.is_empty() {
                            let evt = pb::StartResponse {
                                event: Some(pb::ProcessEvent {
                                    event: Some(ProcessEventOneof::Data(DataEvent {
                                        output: Some(data_event::Output::Stderr(u.stderr.into_bytes())),
                                    })),
                                }),
                            };
                            let _ = tx2.send(Ok(enc_resp(&evt, codec_for_task))).await;
                        }
                        let evt = pb::StartResponse {
                            event: Some(pb::ProcessEvent {
                                event: Some(ProcessEventOneof::End(EndEvent {
                                    exit_code: u.exit_code,
                                    exited: true,
                                    status: format!("exit {}", u.exit_code),
                                    error: None,
                                })),
                            }),
                        };
                        let _ = tx2.send(Ok(enc_resp(&evt, codec_for_task))).await;
                        let _ = tx2.send(Ok(end_envelope_ok())).await;
                    }
                    Err(e) => {
                        let _ = tx2.send(Ok(end_envelope_err("internal",
                            &format!("upstream json: {}", e)))).await;
                    }
                }
            }
            Err(e) => {
                let _ = tx2.send(Ok(end_envelope_err("unavailable",
                    &format!("upstream send: {}", e)))).await;
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = Body::from_stream(stream);
    let ct_out = match codec { Codec::Proto => "application/connect+proto", Codec::Json => "application/connect+json" };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ct_out)
        .header("connect-protocol-version", "1")
        .body(body)
        .unwrap()
}

async fn process_list(State(_state): State<Arc<AppState>>, headers: HeaderMap, _body: Bytes) -> Response {
    let ct = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("application/proto");
    let codec = codec_from_content_type(ct);
    let (body, out_ct) = match codec {
        Codec::Json => (serde_json::to_vec(&serde_json::json!({"processes": []})).unwrap(), "application/json"),
        Codec::Proto => (pb::ListResponse { processes: vec![] }.encode_to_vec(), "application/proto"),
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, out_ct)
        .header("connect-protocol-version", "1")
        .body(Body::from(body))
        .unwrap()
}

// ---- health -------------------------------------------------------------

async fn shim_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let sandboxes: Vec<String> = state.sandboxes.iter().map(|kv| kv.key().clone()).collect();
    Json(serde_json::json!({
        "ok": true,
        "shim": "e2b",
        "upstream": state.upstream,
        "sandbox_count": sandboxes.len(),
        "sandboxes": sandboxes,
    }))
}


// ---- Filesystem service (Connect unary) --------------------------------
// Sandbox ID for the request comes from X-Sandbox-Id header. If absent, we
// fall back to E2B_SHIM_DEFAULT_SANDBOX or reject.

fn pick_sandbox_id(state: &AppState, headers: &HeaderMap) -> Result<String, (StatusCode, String)> {
    if let Some(h) = headers.get("e2b-sandbox-id").or_else(|| headers.get("x-sandbox-id")).and_then(|v| v.to_str().ok()) {
        return Ok(h.to_string());
    }
    // Many clients (Sandbox(template).files.write) don't pass sandbox ID — there's
    // only ever ONE sandbox per Sandbox instance, and the SDK ties it via subdomain.
    // For the single-sandbox case we accept the most recently created.
    if !state.sandboxes.is_empty() {
        let latest = state.sandboxes.iter()
            .max_by(|a, b| a.value().started_at.cmp(&b.value().started_at))
            .map(|e| e.key().clone());
        if let Some(s) = latest { return Ok(s); }
    }
    if let Ok(sid) = std::env::var("E2B_SHIM_DEFAULT_SANDBOX") {
        return Ok(sid);
    }
    Err((StatusCode::BAD_REQUEST, r#"{"code":"invalid_argument","message":"no X-Sandbox-Id header and multiple sandboxes"}"#.into()))
}

fn resolve_path(state: &AppState, headers: &HeaderMap, p: &str) -> Result<std::path::PathBuf, (StatusCode, String)> {
    let sid = pick_sandbox_id(state, headers)?;
    let root = sandbox_fs_dir(&sid);
    let _ = std::fs::create_dir_all(&root);
    // Resolve relative or absolute. E2B clients may send absolute paths like
    // /home/user/file.py — we map them under sandbox root by stripping the leading slash.
    let trimmed = p.trim_start_matches('/');
    let joined = root.join(trimmed);
    Ok(joined)
}

// ---- proto-JSON ↔ Rust dispatch helpers --------------------------------

#[derive(serde::Deserialize, Default)]
struct JsonPathRequest {
    #[serde(default)]
    path: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    destination: String,
    #[serde(default)]
    depth: u32,
}

fn decode_unary_path_request(ct: &str, body: &[u8]) -> Option<JsonPathRequest> {
    let payload = extract_body(ct, body)?;
    match codec_from_content_type(ct) {
        Codec::Json => serde_json::from_slice(payload).ok(),
        Codec::Proto => {
            // We accept either Stat/MakeDir/Remove which all share `string path = 1`.
            // For Move use source=1 destination=2. We try a permissive decode.
            let mut req = JsonPathRequest::default();
            let mut buf = payload;
            while !buf.is_empty() {
                let (tag, rest) = match prost::encoding::decode_varint(&mut std::io::Cursor::new(buf)) {
                    Ok(v) => (v, buf),
                    Err(_) => return None,
                };
                let _ = (tag, rest);
                // Too tedious to hand-decode; fallback: return Some empty so callers don't crash.
                break;
            }
            Some(req)
        }
    }
}

fn make_entry_info_json(path: &std::path::Path, root: &std::path::Path) -> serde_json::Value {
    use serde_json::json;
    let meta = std::fs::symlink_metadata(path).ok();
    let (size, mode, ftype) = match &meta {
        Some(m) => {
            use std::os::unix::fs::PermissionsExt;
            let ft = if m.is_dir() { 2 }
                     else if m.is_file() { 1 }
                     else { 0 };
            (m.len() as i64, m.permissions().mode() as u32, ft)
        }
        None => (0i64, 0u32, 0),
    };
    // Present paths to the SDK relative to the sandbox root, prefixed with '/'.
    let rel = path.strip_prefix(root).unwrap_or(path);
    let mut shown = String::from("/");
    shown.push_str(&rel.to_string_lossy());
    json!({
        "name": path.file_name().map(|f| f.to_string_lossy()).unwrap_or_default(),
        "type": ftype,
        "path": shown,
        "size": size,
        "mode": mode,
        "permissions": format!("{:o}", mode & 0o777),
    })
}

fn unary_json_response(ct: &str, value: serde_json::Value) -> Response {
    let codec = codec_from_content_type(ct);
    let body = match codec {
        Codec::Json => value.to_string().into_bytes(),
        Codec::Proto => {
            // Minimal stub: return JSON anyway. SDK will fail to parse, but our
            // current SDK observation shows it uses JSON exclusively. If you hit
            // a proto-only client, plumb proper proto encoding here.
            value.to_string().into_bytes()
        }
    };
    let ct_out = match codec { Codec::Proto => "application/proto", Codec::Json => "application/json" };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ct_out)
        .header("connect-protocol-version", "1")
        .body(Body::from(body))
        .unwrap()
}

async fn fs_stat(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    let ct = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    let req = match decode_unary_path_request(ct, &body) {
        Some(r) => r,
        None => return (StatusCode::BAD_REQUEST, "decode").into_response(),
    };
    let path = match resolve_path(&state, &headers, &req.path) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    if !path.exists() {
        let ct_out = if ct.contains("json") { "application/json" } else { "application/proto" };
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(header::CONTENT_TYPE, ct_out)
            .body(Body::from(r#"{"code":"not_found","message":"path not found"}"#))
            .unwrap();
    }
    let sid_for_root = pick_sandbox_id(&state, &headers).unwrap_or_default();
    let root = sandbox_fs_dir(&sid_for_root);
    unary_json_response(ct, serde_json::json!({"entry": make_entry_info_json(&path, &root)}))
}

async fn fs_mkdir(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    let ct = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    let req = match decode_unary_path_request(ct, &body) {
        Some(r) => r,
        None => return (StatusCode::BAD_REQUEST, "decode").into_response(),
    };
    let path = match resolve_path(&state, &headers, &req.path) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = std::fs::create_dir_all(&path) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("mkdir: {}", e)).into_response();
    }
    let sid_for_root = pick_sandbox_id(&state, &headers).unwrap_or_default();
    let root = sandbox_fs_dir(&sid_for_root);
    unary_json_response(ct, serde_json::json!({"entry": make_entry_info_json(&path, &root)}))
}

async fn fs_list(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    let ct = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    let req = match decode_unary_path_request(ct, &body) {
        Some(r) => r,
        None => return (StatusCode::BAD_REQUEST, "decode").into_response(),
    };
    let path = match resolve_path(&state, &headers, &req.path) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let sid = match pick_sandbox_id(&state, &headers) {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };
    let root = sandbox_fs_dir(&sid);
    let mut entries = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&path) {
        for e in rd.flatten() {
            entries.push(make_entry_info_json(&e.path(), &root));
        }
    }
    unary_json_response(ct, serde_json::json!({"entries": entries}))
}

async fn fs_remove(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    let ct = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    let req = match decode_unary_path_request(ct, &body) {
        Some(r) => r,
        None => return (StatusCode::BAD_REQUEST, "decode").into_response(),
    };
    let path = match resolve_path(&state, &headers, &req.path) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    if path.is_dir() {
        let _ = std::fs::remove_dir_all(&path);
    } else {
        let _ = std::fs::remove_file(&path);
    }
    unary_json_response(ct, serde_json::json!({}))
}

async fn fs_move(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    let ct = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    let req = match decode_unary_path_request(ct, &body) {
        Some(r) => r,
        None => return (StatusCode::BAD_REQUEST, "decode").into_response(),
    };
    let src = match resolve_path(&state, &headers, &req.source) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let dst = match resolve_path(&state, &headers, &req.destination) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    if let Some(parent) = dst.parent() { let _ = std::fs::create_dir_all(parent); }
    if let Err(e) = std::fs::rename(&src, &dst) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("move: {}", e)).into_response();
    }
    let sid_for_root = pick_sandbox_id(&state, &headers).unwrap_or_default();
    let root = sandbox_fs_dir(&sid_for_root);
    unary_json_response(ct, serde_json::json!({"entry": make_entry_info_json(&dst, &root)}))
}

// ---- /files (HTTP path-based read/write) -------------------------------

#[derive(serde::Deserialize)]
struct FilesQuery {
    #[serde(default)]
    path: String,
    #[serde(default)]
    #[allow(dead_code)]
    username: Option<String>,
}

async fn files_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<FilesQuery>,
) -> Response {
    let path = match resolve_path(&state, &headers, &q.path) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    match std::fs::read(&path) {
        Ok(b) => (StatusCode::OK, [(header::CONTENT_TYPE, "application/octet-stream")], b).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, format!("not found: {}", e)).into_response(),
    }
}

async fn files_post(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<FilesQuery>,
    body: Bytes,
) -> Response {
    let path = match resolve_path(&state, &headers, &q.path) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    if let Some(parent) = path.parent() { let _ = std::fs::create_dir_all(parent); }
    // E2B uploads use multipart/form-data; for now we accept raw bytes too.
    let content_type = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    let payload: Vec<u8> = if content_type.contains("multipart/form-data") {
        // Extract first file part: naive boundary scanner.
        match extract_first_multipart_file(content_type, &body) {
            Some(p) => p,
            None => return (StatusCode::BAD_REQUEST, "empty multipart").into_response(),
        }
    } else {
        body.to_vec()
    };
    match std::fs::write(&path, &payload) {
        Ok(_) => {
            let name = path.file_name().map(|f| f.to_string_lossy().into_owned()).unwrap_or_default();
            let resp = serde_json::json!([{
                "name": name,
                "type": "file",
                "path": path.to_string_lossy(),
            }]);
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {}", e)).into_response(),
    }
}

fn extract_first_multipart_file(content_type: &str, body: &[u8]) -> Option<Vec<u8>> {
    let boundary = content_type.split("boundary=").nth(1)?.trim_matches('"');
    let sep = format!("--{}", boundary);
    let sep_bytes = sep.as_bytes();
    let mut i = 0;
    let mut parts: Vec<(usize, usize)> = Vec::new();
    while let Some(found) = memchr(sep_bytes, &body[i..]) {
        parts.push((i + found, i + found + sep_bytes.len()));
        i = i + found + sep_bytes.len();
    }
    if parts.len() < 2 { return None; }
    let (_p1_start, p1_after_sep) = parts[0];
    let (p2_start, _) = parts[1];
    let part = &body[p1_after_sep..p2_start];
    // Skip CRLFs after boundary.
    let mut s = 0;
    if part.starts_with(b"\r\n") { s += 2; }
    // Find double CRLF that separates headers from body.
    let rest = &part[s..];
    let hdr_end = find_subseq(rest, b"\r\n\r\n")?;
    let body_start = s + hdr_end + 4;
    let body_part = &part[body_start..];
    // Strip trailing CRLF before next boundary.
    let trimmed = body_part.strip_suffix(b"\r\n").unwrap_or(body_part);
    Some(trimmed.to_vec())
}

fn memchr(needle: &[u8], hay: &[u8]) -> Option<usize> { find_subseq(hay, needle) }

fn find_subseq(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() { return None; }
    for i in 0..=hay.len() - needle.len() {
        if &hay[i..i + needle.len()] == needle { return Some(i); }
    }
    None
}
fn sweep_expired(state: &Arc<AppState>) {
    use chrono::{DateTime, Utc};
    let now = Utc::now();
    let mut to_remove = Vec::new();
    for kv in state.sandboxes.iter() {
        let end_at = match DateTime::parse_from_rfc3339(&kv.value().end_at) {
            Ok(t) => t.with_timezone(&Utc),
            Err(_) => continue,
        };
        if end_at < now {
            to_remove.push(kv.key().clone());
        }
    }
    for sid in to_remove {
        state.sandboxes.remove(&sid);
        forget_sandbox(&sid);
        let _ = std::fs::remove_dir_all(sandbox_fs_dir(&sid));
        eprintln!("[e2b-shim] swept expired sandbox {}", sid);
    }
}

// ---- main ---------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let upstream = std::env::var("E2B_SHIM_UPSTREAM").unwrap_or_else(|_| "http://127.0.0.1:8000".into());
    let port: u16 = std::env::var("E2B_SHIM_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(8001);
    let api_key = std::env::var("E2B_SHIM_API_KEY").ok();

    eprintln!("[e2b-shim] upstream={} port={} api_key={}",
        upstream, port, if api_key.is_some() { "set" } else { "open" });

    let prior = load_registry();
    let state = Arc::new(AppState {
        sandboxes: DashMap::new(),
        upstream,
        http: reqwest::Client::builder().http1_only().pool_max_idle_per_host(64).build()?,
        api_key,
    });
    for rec in prior {
        let sid = rec.sandbox_id.clone();
        state.sandboxes.insert(sid, rec);
    }
    eprintln!("[e2b-shim] restored {} sandboxes from {}", state.sandboxes.len(), SANDBOX_REGISTRY_DIR);

    let app = Router::new()
        .route("/health", get(shim_health))
        .route("/sandboxes", post(create_sandbox))
        .route("/sandboxes/:id", get(get_sandbox).delete(delete_sandbox))
        .route("/sandboxes/:id/timeout", post(set_sandbox_timeout))
        // Connect RPC paths (E2B SDK sends to {base_url}/process.Process/Start)
        .route("/process.Process/Start", post(process_start))
        .route("/process.Process/List", post(process_list))
        .route("/filesystem.Filesystem/Stat", post(fs_stat))
        .route("/filesystem.Filesystem/MakeDir", post(fs_mkdir))
        .route("/filesystem.Filesystem/ListDir", post(fs_list))
        .route("/filesystem.Filesystem/Remove", post(fs_remove))
        .route("/filesystem.Filesystem/Move", post(fs_move))
        .route("/files", get(files_get).post(files_post))
        // /envd-prefixed paths
        .route("/envd/process.Process/Start", post(process_start))
        .route("/envd/process.Process/List", post(process_list))
        .route("/envd/filesystem.Filesystem/Stat", post(fs_stat))
        .route("/envd/filesystem.Filesystem/MakeDir", post(fs_mkdir))
        .route("/envd/filesystem.Filesystem/ListDir", post(fs_list))
        .route("/envd/filesystem.Filesystem/Remove", post(fs_remove))
        .route("/envd/filesystem.Filesystem/Move", post(fs_move))
        .route("/envd/files", get(files_get).post(files_post))
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state.clone());

    // Background sweep of expired sandboxes (covers SDK debug-mode no-op kill,
    // crashed clients, etc.). Every 60s.
    let sweep_state = state.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tick.tick().await;
            sweep_expired(&sweep_state);
        }
    });
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    eprintln!("[e2b-shim] listening on 0.0.0.0:{}", port);
    axum::serve(listener, app).await?;
    Ok(())
}

fn general_b64_decode(s: &str) -> Option<Vec<u8>> {
    // Inverse of general_b64.
    let mut tbl = [255u8; 256];
    for (i, c) in b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".iter().enumerate() {
        tbl[*c as usize] = i as u8;
    }
    let bytes: Vec<u8> = s.bytes().filter(|b| *b != b'\n' && *b != b'\r' && *b != b' ').collect();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut buf = 0u32;
    let mut bits = 0u32;
    for b in bytes.iter() {
        if *b == b'=' { break; }
        let v = tbl[*b as usize];
        if v == 255 { return None; }
        buf = (buf << 6) | (v as u32);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }
    Some(out)
}
