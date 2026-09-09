use std::{io::Read, path::PathBuf};

use anyhow::{ensure, Context, Result};
use sha2::{Digest, Sha256};

use super::{acp::AcpLaunchConfig, EngineInstanceSettings, ModelInfo};
use crate::runtime_env;

pub const VERSION: &str = "1.1.0";
pub const SHA256: &str = "9ef7afa432341c05d6c049d143349ea71fbb48989813ba625054a7224e2804fc";
pub const DEFAULT_MODEL: &str = "gemini-3.8-flash-high";
pub const DIFFS: bool = false;

pub fn launch(id: &str, name: &str, settings: &EngineInstanceSettings) -> Result<AcpLaunchConfig> {
    ensure!(settings.home_path.is_none(), "Antigravity does not support a provider home override; configure the agy CLI account directly");
    let mut env = settings.process_env("agy");
    let command = env
        .get("PANES_AGY_ACP_COMMAND")
        .cloned()
        .or_else(|| std::env::var("PANES_AGY_ACP_COMMAND").ok());
    let explicit = settings
        .binary_path
        .clone()
        .or_else(|| command.map(PathBuf::from));
    let executable = if let Some(path) = explicit {
        if path.is_absolute() {
            path
        } else if let Some(search) = env.get("PATH") {
            which::which_in(&path, Some(search), std::env::current_dir()?)?
        } else {
            runtime_env::resolve_executable(path.to_str().context("invalid adapter path")?)
                .context("Antigravity ACP override was not found")?
        }
    } else {
        ensure!(cfg!(all(target_os = "macos", target_arch = "aarch64")), "The pinned Antigravity ACP adapter supports Apple Silicon macOS. Set PANES_AGY_ACP_COMMAND to a compatible adapter on this platform.");
        let path = if let Some(search) = env.get("PATH") {
            which::which_in("agy-acp", Some(search), std::env::current_dir()?).ok()
        } else {
            runtime_env::resolve_executable("agy-acp")
                .or_else(|| runtime_env::home_dir().map(|home| home.join(".local/bin/agy-acp")))
        }
        .context("Install agy-acp v1.1.0 or set PANES_AGY_ACP_COMMAND")?;
        verify_binary(&path)?;
        path
    };
    ensure!(
        runtime_env::is_executable_file(&executable),
        "Antigravity ACP adapter is missing or not executable: {}",
        executable.display()
    );
    if let Some(path) = runtime_env::augmented_path_with_prepend(None) {
        env.entry("PATH".into())
            .or_insert_with(|| path.to_string_lossy().into_owned());
    }
    env.entry("AGY_SKIP_DOWNLOAD".into())
        .or_insert_with(|| "1".into());
    Ok(AcpLaunchConfig {
        id: id.into(),
        name: name.into(),
        executable,
        args: settings.launch_args.clone(),
        env,
        request_timeout: super::acp::DEFAULT_REQUEST_TIMEOUT,
        turn_timeout: super::acp::DEFAULT_TURN_TIMEOUT,
    })
}

fn verify_binary(path: &std::path::Path) -> Result<()> {
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("Install agy-acp v{VERSION} at {}", path.display()))?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    ensure!(
        format!("{:x}", hash.finalize()) == SHA256,
        "Antigravity ACP checksum mismatch: expected agy-acp v{VERSION} ({SHA256})"
    );
    Ok(())
}

pub fn fallback_models() -> Vec<ModelInfo> {
    [
        ("gemini-3.8-flash-high", "Gemini 3.8 Flash (High)"),
        ("gemini-3.8-flash-medium", "Gemini 3.8 Flash (Medium)"),
        ("gemini-3.8-flash-low", "Gemini 3.8 Flash (Low)"),
        ("gemini-3.7-flash-high", "Gemini 3.7 Flash (High)"),
        ("gemini-3.7-flash-medium", "Gemini 3.7 Flash (Medium)"),
        ("gemini-3.7-flash-low", "Gemini 3.7 Flash (Low)"),
        ("gemini-3.6-flash-high", "Gemini 3.6 Flash (High)"),
        ("gemini-3.6-flash-medium", "Gemini 3.6 Flash (Medium)"),
        ("gemini-3.6-flash-low", "Gemini 3.6 Flash (Low)"),
        ("gemini-3.1-pro-high", "Gemini 3.1 Pro (High)"),
        ("gemini-3.1-pro-low", "Gemini 3.1 Pro (Low)"),
        ("claude-sonnet-4-6", "Claude Sonnet 4.6 (Thinking)"),
        ("claude-opus-4-6-thinking", "Claude Opus 4.6 (Thinking)"),
        ("gpt-oss-120b-medium", "GPT-OSS 120B (Medium)"),
    ]
    .into_iter()
    .map(|(id, name)| {
        super::hermes::model_info(id, name, "Antigravity CLI model", id == DEFAULT_MODEL)
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agy_catalog_matches_recorded_adapter_options() {
        let frames: Vec<serde_json::Value> =
            include_str!("../../tests/fixtures/acp/agy-prompt.jsonl")
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
        let options = frames
            .iter()
            .filter_map(|frame| frame["params"]["update"]["configOptions"].as_array())
            .flatten()
            .find(|option| option["id"] == "model")
            .unwrap();
        let slugs: Vec<_> = options["options"]
            .as_array()
            .unwrap()
            .iter()
            .map(|option| option["value"].as_str().unwrap())
            .collect();
        assert_eq!(
            fallback_models()
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>(),
            slugs
        );
        assert!(!DIFFS);
    }

    #[test]
    fn agy_rejects_unpinned_binary_and_missing_override() {
        assert!(verify_binary(&std::env::current_exe().unwrap()).is_err());
        assert!(launch(
            "agy",
            "Antigravity",
            &EngineInstanceSettings {
                binary_path: Some("/missing/agy-acp".into()),
                ..Default::default()
            }
        )
        .is_err());
    }

    #[test]
    fn agy_override_preserves_arguments_and_environment() {
        let binary = std::env::current_exe().unwrap();
        let settings = EngineInstanceSettings {
            env: std::collections::BTreeMap::from([
                (
                    "PANES_AGY_ACP_COMMAND".into(),
                    binary.to_string_lossy().into_owned(),
                ),
                ("AGY_BIN".into(), "/custom/agy".into()),
            ]),
            launch_args: vec!["--custom".into()],
            ..Default::default()
        };
        let config = launch("agy_work", "Work", &settings).unwrap();
        assert_eq!(config.executable, binary);
        assert_eq!(config.args, ["--custom"]);
        assert_eq!(config.env["AGY_BIN"], "/custom/agy");
        assert_eq!(config.env["AGY_SKIP_DOWNLOAD"], "1");
    }
}
