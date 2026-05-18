//! Build a hot-template docker image from templates/<name>/template.toml.
//!
//! Replaces template-build.py. Same behavior:
//!   - Reads template.toml
//!   - Copies worker-rust binary + inspect_pb2.py into a temp build context
//!   - Generates Dockerfile (or uses templates/<name>/Dockerfile if present)
//!   - Shells out to `docker build`

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use serde::Deserialize;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

const ROOT: &str = "/opt/inspect-api";
// Public default. Override with PIP_INDEX_URL env var for a regional mirror.
const DEFAULT_PIP_INDEX: &str = "https://pypi.org/simple/";
const DEFAULT_PIP_TRUSTED: &str = "pypi.org";
const PROTOBUF_PIN: &str = "protobuf";

#[derive(Debug, Parser)]
#[command(about = "Build template image(s) from templates/<name>/template.toml")]
struct Args {
    /// One or more template names to build (relative to templates/)
    names: Vec<String>,

    /// Override ROOT (default: /opt/inspect-api)
    #[arg(long)]
    root: Option<PathBuf>,

    /// After build, also `docker tag` + `docker push` to the registry.
    /// REGISTRY_HOST and REGISTRY_NAMESPACE must be set (in .env or shell env).
    /// You must `docker login` to REGISTRY_HOST beforehand.
    #[arg(long)]
    push: bool,

    /// Tag to use when pushing (default: "latest").
    #[arg(long, default_value = "latest")]
    push_tag: String,
}

#[derive(Debug, Deserialize)]
struct TemplateCfg {
    #[serde(default)]
    #[allow(dead_code)]
    name: String,
    #[serde(default = "default_base")]
    base_image: String,
    #[serde(default)]
    prewarm: Vec<String>,
    #[serde(default)]
    extra_pip: Vec<String>,
    #[serde(default = "default_pool_size")]
    pool_size: usize,
}
fn default_base() -> String { "python:3.12-slim".into() }
fn default_pool_size() -> usize { 32 }

fn build(root: &Path, name: &str) -> Result<()> {
    let tpl_dir = root.join("templates").join(name);
    let toml_path = tpl_dir.join("template.toml");
    if !toml_path.exists() {
        return Err(anyhow!("missing {}", toml_path.display()));
    }
    let cfg: TemplateCfg = toml::from_str(
        &fs::read_to_string(&toml_path).with_context(|| format!("read {}", toml_path.display()))?,
    ).with_context(|| format!("parse {}", toml_path.display()))?;

    let tag = format!("inspect-tpl-{}:latest", name);
    let worker_bin = root.join("worker-rust/target/release/worker-rust");
    let proto_py = root.join("proto/inspect_pb2.py");

    if !worker_bin.exists() {
        return Err(anyhow!(
            "missing rust binary at {}; run `cargo build --release` in worker-rust/ first",
            worker_bin.display()
        ));
    }
    if !proto_py.exists() {
        return Err(anyhow!(
            "missing {}; run `protoc --python_out=. inspect.proto` in proto/ first",
            proto_py.display()
        ));
    }

    let pip_index = std::env::var("PIP_INDEX_URL").unwrap_or_else(|_| DEFAULT_PIP_INDEX.into());
    let pip_trusted = std::env::var("PIP_TRUSTED_HOST").unwrap_or_else(|_| DEFAULT_PIP_TRUSTED.into());

    let bd = tempfile::tempdir().context("mktempdir")?;
    let bd_path = bd.path();

    let bin_dst = bd_path.join("worker-rust");
    fs::copy(&worker_bin, &bin_dst).context("copy worker binary")?;
    fs::set_permissions(&bin_dst, fs::Permissions::from_mode(0o755))?;

    fs::copy(&proto_py, bd_path.join("inspect_pb2.py")).context("copy inspect_pb2.py")?;

    let mut pkgs: Vec<&str> = vec![PROTOBUF_PIN];
    pkgs.extend(cfg.prewarm.iter().map(|s| s.as_str()));
    pkgs.extend(cfg.extra_pip.iter().map(|s| s.as_str()));

    let custom_df = tpl_dir.join("Dockerfile");
    let dockerfile = if custom_df.exists() {
        fs::read_to_string(&custom_df).context("read custom Dockerfile")?
    } else {
        let prewarm_csv = cfg.prewarm.join(",");
        format!(
            "FROM {base}\n\
             WORKDIR /\n\
             RUN pip install --no-cache-dir --index-url {pip_idx} --trusted-host {pip_trust} {pkgs}\n\
             ENV WORKER_PREWARM_MODULES=\"{prewarm_csv}\"\n\
             ENV WORKER_POOL_SIZE=\"{pool}\"\n\
             COPY worker-rust /usr/local/bin/worker-rust\n\
             COPY inspect_pb2.py /inspect_pb2.py\n\
             ENV PYTHONPATH=\"/\"\n\
             ENV LD_LIBRARY_PATH=\"/usr/local/lib\"\n\
             CMD [\"/usr/local/bin/worker-rust\"]\n",
            base = cfg.base_image,
            pip_idx = pip_index,
            pip_trust = pip_trusted,
            pkgs = pkgs.join(" "),
            prewarm_csv = prewarm_csv,
            pool = cfg.pool_size,
        )
    };
    fs::write(bd_path.join("Dockerfile"), &dockerfile).context("write Dockerfile")?;

    println!("--- Dockerfile for {} ---\n{}", tag, dockerfile);

    let status = Command::new("docker")
        .args(["build", "-t", &tag])
        .arg(bd_path)
        .status()
        .context("spawn docker build")?;
    if !status.success() {
        return Err(anyhow!("docker build failed (exit {:?})", status.code()));
    }
    println!("=== built {} ===", tag);
    Ok(())
}

fn push_to_registry(name: &str, push_tag: &str) -> Result<()> {
    let host = std::env::var("REGISTRY_HOST")
        .map_err(|_| anyhow!("--push requires REGISTRY_HOST env var"))?;
    let ns = std::env::var("REGISTRY_NAMESPACE")
        .map_err(|_| anyhow!("--push requires REGISTRY_NAMESPACE env var"))?;
    let local_tag = format!("inspect-tpl-{}:latest", name);
    let remote_tag = format!("{}/{}/inspect-tpl-{}:{}", host, ns, name, push_tag);

    println!("[push] tagging {} → {}", local_tag, remote_tag);
    let st = Command::new("docker").args(["tag", &local_tag, &remote_tag]).status()
        .context("spawn docker tag")?;
    if !st.success() { return Err(anyhow!("docker tag failed")); }

    println!("[push] pushing {}", remote_tag);
    let st = Command::new("docker").args(["push", &remote_tag]).status()
        .context("spawn docker push")?;
    if !st.success() {
        return Err(anyhow!(
            "docker push failed; did you `docker login {}` ? (REGISTRY_USER / REGISTRY_PASSWORD)",
            host
        ));
    }
    println!("=== pushed {} ===", remote_tag);
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let root = args.root.unwrap_or_else(|| PathBuf::from(ROOT));
    if args.names.is_empty() {
        return Err(anyhow!("no template names provided (try: template-build default)"));
    }
    for name in &args.names {
        build(&root, name)?;
        if args.push {
            push_to_registry(name, &args.push_tag)?;
        }
    }
    Ok(())
}
