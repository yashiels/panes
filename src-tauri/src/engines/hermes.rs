use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;

use super::{
    acp::{AcpLaunchConfig, DEFAULT_REQUEST_TIMEOUT, DEFAULT_TURN_TIMEOUT},
    EngineInstanceSettings, ModelInfo,
};
use crate::runtime_env;

pub const DEFAULT_MODEL: &str = "default";

pub fn launch(id: &str, name: &str, settings: &EngineInstanceSettings) -> Result<AcpLaunchConfig> {
    let mut env = settings.process_env("hermes");
    let executable = resolve_executable(settings, &env)?;
    if let Some(path) =
        runtime_env::augmented_path_with_prepend(executable.parent().map(PathBuf::from))
    {
        env.entry("PATH".into())
            .or_insert_with(|| path.to_string_lossy().into_owned());
    }
    env.entry("PYTHONUNBUFFERED".into())
        .or_insert_with(|| "1".into());
    let mut args = vec!["acp".into()];
    args.extend(settings.launch_args.clone());
    Ok(AcpLaunchConfig {
        id: id.into(),
        name: name.into(),
        executable,
        args,
        env,
        request_timeout: DEFAULT_REQUEST_TIMEOUT,
        turn_timeout: DEFAULT_TURN_TIMEOUT,
    })
}

fn resolve_executable(
    settings: &EngineInstanceSettings,
    env: &BTreeMap<String, String>,
) -> Result<PathBuf> {
    if let Some(path) = &settings.binary_path {
        ensure!(
            runtime_env::is_executable_file(path),
            "Hermes executable is missing or not executable: {}",
            path.display()
        );
        return Ok(path.clone());
    }
    if let Some(path) = env.get("PATH") {
        return which::which_in("hermes", Some(path), std::env::current_dir()?)
            .context("Hermes executable was not found in the provider's configured PATH");
    }
    if let Some(path) = runtime_env::resolve_executable("hermes") {
        return Ok(path);
    }
    let home = env
        .get("HERMES_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HERMES_HOME").map(PathBuf::from))
        .or_else(|| runtime_env::home_dir().map(|home| home.join(".hermes")));
    home.into_iter().flat_map(|home| managed_candidates(&home))
        .chain(managed_candidates(&PathBuf::from("/usr/local/lib")))
        .find(|path| runtime_env::is_executable_file(path))
        .context("Hermes was not found in PATH or its managed virtual environment. Install Hermes with ACP support, run `hermes setup`, or set the provider binary path to its venv/bin/hermes executable. Packaged Panes cannot use shell aliases or an activated terminal's Python environment.")
}

fn managed_candidates(home: &std::path::Path) -> Vec<PathBuf> {
    [
        "hermes-agent/venv/bin/hermes",
        "hermes-agent/.venv/bin/hermes",
        "venv/bin/hermes",
        "hermes-agent/venv/Scripts/hermes.exe",
    ]
    .iter()
    .map(|suffix| home.join(suffix))
    .collect()
}

pub fn fallback_models() -> Vec<ModelInfo> {
    vec![model_info(
        DEFAULT_MODEL,
        "Configured Hermes model",
        "Uses the model and provider selected by hermes setup.",
        true,
    )]
}

pub fn model_info(id: &str, name: &str, description: &str, is_default: bool) -> ModelInfo {
    ModelInfo {
        id: id.into(),
        display_name: name.into(),
        description: description.into(),
        hidden: false,
        is_default,
        upgrade: None,
        availability_nux: None,
        upgrade_info: None,
        input_modalities: vec!["text".into()],
        attachment_modalities: vec![],
        limits: None,
        supports_personality: false,
        default_reasoning_effort: String::new(),
        supported_reasoning_efforts: vec![],
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionModels {
    pub current_model_id: String,
    pub available_models: Vec<SessionModel>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionModel {
    pub model_id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

pub fn session_models(response: &Value) -> Result<Option<SessionModels>> {
    response
        .get("models")
        .filter(|value| !value.is_null())
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .context("invalid Hermes ACP model catalog")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hermes_explicit_missing_binary_does_not_use_another_account() {
        let settings = EngineInstanceSettings {
            binary_path: Some(PathBuf::from("/missing/panes-hermes")),
            ..Default::default()
        };
        assert!(launch("hermes_work", "Work", &settings).is_err());
    }

    #[test]
    fn hermes_configured_path_does_not_fall_back_to_ambient_path() {
        let settings = EngineInstanceSettings {
            env: BTreeMap::from([("PATH".into(), "/missing/hermes-provider-path".into())]),
            ..Default::default()
        };
        let error = launch("hermes_work", "Work", &settings).err().unwrap();
        assert!(error.to_string().contains("configured PATH"));
    }

    #[test]
    fn hermes_launch_preserves_profile_environment_and_arguments() {
        let binary = std::env::current_exe().unwrap();
        let settings = EngineInstanceSettings {
            binary_path: Some(binary.clone()),
            home_path: Some(PathBuf::from("/tmp/hermes-work")),
            launch_args: vec!["--verbose".into()],
            env: BTreeMap::from([("CUSTOM".into(), "value".into())]),
        };
        let launch = launch("hermes_work", "Work", &settings).unwrap();
        assert_eq!(launch.executable, binary);
        assert_eq!(launch.args, ["acp", "--verbose"]);
        assert_eq!(launch.env["HERMES_HOME"], "/tmp/hermes-work");
        assert_eq!(launch.env["CUSTOM"], "value");
        assert_eq!(launch.env["PYTHONUNBUFFERED"], "1");
        assert_eq!(fallback_models()[0].id, DEFAULT_MODEL);
    }
}
