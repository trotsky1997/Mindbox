//! Build a tools template docker image from templates/<name>/template.toml.
//!
//! Reads template.toml, copies the tools-rust daemon into a temporary build
//! context, generates a Dockerfile when needed, and shells out to `docker build`.

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use serde::Deserialize;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

const ROOT: &str = "/opt/inspect-api";
// Default to the Volcano Engine intranet pip mirror. The standard
// deployment target is Volcano ECS + Volcano CR, where this URL resolves
// over the internal network — order(s) of magnitude faster than pypi.org.
// Override with PIP_INDEX_URL / PIP_TRUSTED_HOST when building elsewhere
// (laptop, CI, other cloud).
const DEFAULT_PIP_INDEX: &str = "https://mirrors.ivolces.com/pypi/simple/";
const DEFAULT_PIP_TRUSTED: &str = "mirrors.ivolces.com";

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
    extra_pip: Vec<String>,
    /// Extra apt packages (any kind). For kind="tools" this is the primary
    /// way to install bash/ripgrep/fd/etc into the image.
    #[serde(default)]
    extra_apt: Vec<String>,
}
fn default_base() -> String {
    "python:3.12-slim".into()
}

fn build(root: &Path, name: &str) -> Result<()> {
    let tpl_dir = root.join("templates").join(name);
    let toml_path = tpl_dir.join("template.toml");
    if !toml_path.exists() {
        return Err(anyhow!("missing {}", toml_path.display()));
    }
    let cfg: TemplateCfg = toml::from_str(
        &fs::read_to_string(&toml_path).with_context(|| format!("read {}", toml_path.display()))?,
    )
    .with_context(|| format!("parse {}", toml_path.display()))?;
    build_tools(root, &tpl_dir, name, &cfg)
}

/// Generate + docker-build a kind="tools" template image: thin debian-slim
/// base with apt packages from extra_apt and the tools-rust daemon binary.
/// No Python pool, no prewarm, no protobuf — sessions are managed inside
/// the daemon as cwd subdirs.
fn build_tools(root: &Path, tpl_dir: &Path, name: &str, cfg: &TemplateCfg) -> Result<()> {
    let tag = format!("inspect-tpl-tools-{}:latest", name);
    let tools_bin = root.join("tools-rust/target/release/tools-rust");
    if !tools_bin.exists() {
        return Err(anyhow!(
            "missing tools-rust binary at {}; run `cargo build --release` in tools-rust/ first",
            tools_bin.display()
        ));
    }

    let bd = tempfile::tempdir().context("mktempdir")?;
    let bd_path = bd.path();
    let bin_dst = bd_path.join("tools-rust");
    fs::copy(&tools_bin, &bin_dst).context("copy tools-rust binary")?;
    fs::set_permissions(&bin_dst, fs::Permissions::from_mode(0o755))?;

    // Compose apt list. Always include the floor (bash + tini + ca-certs +
    // ripgrep + fd-find + coreutils + findutils) so any kind="tools" image
    // has the 7-tool toolchain regardless of what extra_apt the user adds.
    let mut apt_pkgs: Vec<&str> = vec![
        "ca-certificates",
        "tini",
        "bash",
        "ripgrep",
        "fd-find",
        "coreutils",
        "findutils",
    ];
    apt_pkgs.extend(cfg.extra_apt.iter().map(|s| s.as_str()));

    let pip_index = std::env::var("PIP_INDEX_URL").unwrap_or_else(|_| DEFAULT_PIP_INDEX.into());
    let pip_trusted =
        std::env::var("PIP_TRUSTED_HOST").unwrap_or_else(|_| DEFAULT_PIP_TRUSTED.into());
    let npm_registry = std::env::var("NPM_REGISTRY_URL")
        .unwrap_or_else(|_| format!("https://{}/npm/", pip_trusted));

    let pip_install_line = if cfg.extra_pip.is_empty() {
        String::new()
    } else {
        format!(
            "RUN pip install --no-cache-dir --index-url {pip_idx} --trusted-host {pip_trust} {pkgs}
",
            pip_idx = pip_index,
            pip_trust = pip_trusted,
            pkgs = cfg.extra_pip.join(" "),
        )
    };

    let custom_df = tpl_dir.join("Dockerfile");
    let dockerfile = if custom_df.exists() {
        fs::read_to_string(&custom_df).context("read custom Dockerfile")?
    } else {
        // Raw string sidesteps the `\"` / `\n` escape soup in generated Dockerfiles.
        format!(
            r#"FROM {base}
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends {apt_pkgs} \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/fdfind /usr/local/bin/fd 2>/dev/null || true
ENV PIP_INDEX_URL="{pip_idx}" PIP_TRUSTED_HOST="{pip_trust}" UV_INDEX_URL="{pip_idx}" UV_INDEX_STRATEGY="unsafe-best-match" npm_config_registry="{npm_reg}" BUN_CONFIG_REGISTRY="{npm_reg}"
{pip_install_line}COPY tools-rust /usr/local/bin/tools-rust
RUN mkdir -p /sandboxes
ENV TOOLS_PORT=8002 TOOLS_SANDBOX_ROOT=/sandboxes RUST_LOG=info
EXPOSE 8002
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/tools-rust"]
"#,
            base = cfg.base_image,
            apt_pkgs = apt_pkgs.join(" "),
            pip_idx = pip_index,
            pip_trust = pip_trusted,
            npm_reg = npm_registry,
            pip_install_line = pip_install_line,
        )
    };
    fs::write(bd_path.join("Dockerfile"), &dockerfile).context("write Dockerfile")?;

    println!(
        "--- Dockerfile for {} ---
{}",
        tag, dockerfile
    );
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
    let local_tag = format!("inspect-tpl-tools-{}:latest", name);
    let remote_tag = format!("{}/{}/inspect-tpl-tools-{}:{}", host, ns, name, push_tag);

    println!("[push] tagging {} → {}", local_tag, remote_tag);
    let st = Command::new("docker")
        .args(["tag", &local_tag, &remote_tag])
        .status()
        .context("spawn docker tag")?;
    if !st.success() {
        return Err(anyhow!("docker tag failed"));
    }

    println!("[push] pushing {}", remote_tag);
    let st = Command::new("docker")
        .args(["push", &remote_tag])
        .status()
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
        return Err(anyhow!(
            "no template names provided (try: template-build default)"
        ));
    }
    for name in &args.names {
        build(&root, name)?;
        if args.push {
            push_to_registry(name, &args.push_tag)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_cfg_uses_defaults_when_fields_missing() {
        // All fields are #[serde(default)]; empty TOML should parse and use defaults.
        let cfg: TemplateCfg = toml::from_str("").expect("parse empty");
        assert_eq!(cfg.name, ""); // default_name = empty
        assert_eq!(cfg.base_image, default_base()); // python:3.12-slim
        assert!(cfg.extra_apt.is_empty());
    }

    #[test]
    fn template_cfg_rejects_malformed_toml() {
        // Unterminated string is a parse error, NOT a missing-field error.
        let err = toml::from_str::<TemplateCfg>("name = \"unterminated").unwrap_err();
        // We don't assert the exact error message — just that parsing failed.
        let _ = err.to_string();
    }
}
