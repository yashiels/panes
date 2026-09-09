use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use agent_client_protocol::schema::{v1 as acp, ProtocolVersion};
use anyhow::{anyhow, bail, ensure, Context, Result};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{mpsc, oneshot, OnceCell},
    time::{timeout, Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    trim_action_output_delta_content, ActionResult, ActionType, ApprovalRequestRoute, Engine,
    EngineEvent, EngineThread, ModelInfo, OutputStream, SandboxPolicy, ThreadScope, TokenUsage,
    TurnCompletionStatus, TurnInput,
};

const MAX_FRAME: usize = 1024 * 1024;
const MAX_TOOLS: usize = 1024;
const MAX_TOOL_SNAPSHOT: usize = 128 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_TURN_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const CANCEL_GRACE: Duration = Duration::from_millis(250);

type Reply<T> = oneshot::Sender<Result<T>>;
struct SessionSlot {
    process: OnceCell<Arc<Process>>,
    predecessor: Option<Arc<Process>>,
}

impl SessionSlot {
    fn new(predecessor: Option<Arc<Process>>) -> Self {
        Self {
            process: OnceCell::new(),
            predecessor,
        }
    }

    fn get(&self) -> Option<&Arc<Process>> {
        self.process.get()
    }
}

#[derive(Clone)]
pub struct AcpLaunchConfig {
    pub id: String,
    pub name: String,
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub request_timeout: Duration,
    pub turn_timeout: Duration,
}

struct ConfigurationGeneration {
    revision: u64,
    shutdown: CancellationToken,
}

pub struct AcpEngine {
    launch: AcpLaunchConfig,
    instance: Uuid,
    configuration: Mutex<ConfigurationGeneration>,
    product_kind: &'static str,
    profile_settings: Option<Mutex<super::EngineInstanceSettings>>,
    runtime_models: Mutex<Vec<ModelInfo>>,
    sessions: Mutex<HashMap<String, Arc<SessionSlot>>>,
}

impl AcpEngine {
    pub fn new(launch: AcpLaunchConfig) -> Self {
        Self {
            launch,
            instance: Uuid::new_v4(),
            configuration: Mutex::new(ConfigurationGeneration {
                revision: 0,
                shutdown: CancellationToken::new(),
            }),
            product_kind: "acp",
            profile_settings: None,
            runtime_models: Mutex::new(Vec::new()),
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub fn hermes(id: &str, name: &str, settings: super::EngineInstanceSettings) -> Self {
        let mut engine = Self::new(AcpLaunchConfig {
            id: id.into(),
            name: name.into(),
            executable: PathBuf::new(),
            args: vec![],
            env: BTreeMap::new(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            turn_timeout: DEFAULT_TURN_TIMEOUT,
        });
        engine.product_kind = "hermes";
        engine.profile_settings = Some(Mutex::new(settings));
        engine
    }

    pub fn agy(id: &str, name: &str, settings: super::EngineInstanceSettings) -> Self {
        let mut engine = Self::hermes(id, name, settings);
        engine.product_kind = "agy";
        engine
    }

    pub fn kind(&self) -> &'static str {
        self.product_kind
    }

    fn launch_snapshot(&self) -> Result<AcpLaunchConfig> {
        match &self.profile_settings {
            Some(settings) => {
                if self.kind() == "agy" {
                    super::agy::launch(self.id(), self.name(), &settings.lock().unwrap())
                } else {
                    super::hermes::launch(self.id(), self.name(), &settings.lock().unwrap())
                }
            }
            None => Ok(self.launch.clone()),
        }
    }

    pub fn update_instance_settings_sync(&self, settings: super::EngineInstanceSettings) {
        self.replace_instance_settings(settings);
    }

    fn replace_instance_settings(
        &self,
        settings: super::EngineInstanceSettings,
    ) -> Vec<Arc<Process>> {
        let mut configuration = self.configuration.lock().unwrap();
        let changed = self.profile_settings.as_ref().is_some_and(|current| {
            let mut current = current.lock().unwrap();
            if *current == settings {
                return false;
            }
            *current = settings;
            true
        });
        if !changed {
            return vec![];
        }
        configuration.shutdown.cancel();
        configuration.revision += 1;
        configuration.shutdown = CancellationToken::new();
        self.runtime_models.lock().unwrap().clear();
        std::mem::take(&mut *self.sessions.lock().unwrap())
            .into_values()
            .filter_map(|slot| slot.get().cloned())
            .collect()
    }

    pub async fn update_instance_settings(&self, settings: super::EngineInstanceSettings) {
        for process in self.replace_instance_settings(settings) {
            process.stop().await;
        }
    }

    fn spawn_process(&self, cwd: &PathBuf) -> Result<Arc<Process>> {
        let configuration = self.configuration.lock().unwrap();
        Process::spawn(
            &self.launch_snapshot()?,
            self.instance,
            cwd,
            configuration.revision,
            configuration.shutdown.clone(),
        )
    }

    async fn probe(&self) -> Result<acp::InitializeResponse> {
        let cwd = std::env::temp_dir();
        let process = self.spawn_process(&cwd)?;
        let result = initialize(&process).await;
        process.stop().await;
        result
    }

    pub async fn prewarm(&self) -> Result<()> {
        self.probe().await.map(|_| ())
    }

    pub async fn health_report(&self) -> crate::models::EngineHealthDto {
        let result = self.probe().await;
        let (available, version, details) = match result {
            Ok(init) => (true, init.agent_info.map(|info| info.version),
                format!("{} ACP protocol 1 is ready. Model credentials are checked on the first turn.", self.name())),
            Err(error) => (false, None, format!("{} ACP could not start: {error:#}. Check the provider adapter path and CLI credentials.", self.name())),
        };
        crate::models::EngineHealthDto {
            id: self.id().into(),
            available,
            version,
            details: Some(details),
            warnings: vec![],
            checks: vec!["ACP initialize (protocol 1)".into()],
            fixes: if available {
                vec![]
            } else {
                vec![if self.kind() == "agy" {
                    "Install agy-acp v1.1.0 and run agy to sign in".into()
                } else {
                    "hermes setup".into()
                }]
            },
            protocol_diagnostics: None,
        }
    }

    fn capture_models(&self, response: &Value, process: &Process) -> Result<()> {
        let configuration = self.configuration.lock().unwrap();
        ensure!(
            configuration.revision == process.configuration_revision,
            "ACP configuration changed while starting session"
        );
        if self.kind() == "hermes" {
            if let Some(models) = super::hermes::session_models(response)? {
                let mut catalog = super::hermes::fallback_models();
                catalog.extend(
                    models
                        .available_models
                        .iter()
                        .filter(|model| model.model_id != super::hermes::DEFAULT_MODEL)
                        .map(|model| {
                            super::hermes::model_info(
                                &model.model_id,
                                &model.name,
                                model.description.as_deref().unwrap_or("Hermes model"),
                                false,
                            )
                        }),
                );
                *self.runtime_models.lock().unwrap() = catalog;
                *process.model.lock().unwrap() = Some(models.current_model_id.clone());
                *process.default_model.lock().unwrap() = Some(models.current_model_id);
            }
        }
        Ok(())
    }

    async fn select_model(&self, process: &Process, session: &str, model: &str) -> Result<()> {
        if self.profile_settings.is_none() {
            return Ok(());
        }
        if self.kind() == "agy" {
            let selected = if model.is_empty() {
                super::agy::DEFAULT_MODEL
            } else {
                model
            };
            ensure!(
                super::agy::fallback_models()
                    .iter()
                    .any(|model| model.id == selected),
                "Unknown Antigravity model slug: {selected}"
            );
            if process.model.lock().unwrap().as_deref() != Some(selected) {
                let _: Value = process
                    .rpc(
                        "session/set_config_option",
                        json!({"sessionId": session, "configId": "model", "value": selected}),
                    )
                    .await?;
                *process.model.lock().unwrap() = Some(selected.into());
            }
            return Ok(());
        }
        let selected = if model.is_empty() || model == super::hermes::DEFAULT_MODEL {
            process.default_model.lock().unwrap().clone()
        } else {
            Some(model.to_string())
        };
        let Some(selected) = selected else {
            return Ok(());
        };
        if process.model.lock().unwrap().as_ref() == Some(&selected) {
            return Ok(());
        }
        let _: Value = process
            .rpc(
                "session/set_model",
                json!({"sessionId": session, "modelId": selected}),
            )
            .await?;
        *process.model.lock().unwrap() = Some(selected);
        Ok(())
    }

    fn process(&self, session: &str) -> Result<Arc<Process>> {
        self.sessions
            .lock()
            .unwrap()
            .get(session)
            .and_then(|slot| slot.get())
            .cloned()
            .filter(|process| process.alive.load(Ordering::Acquire))
            .context("ACP session is not live; call start_thread to load it")
    }

    async fn open(&self, cwd: PathBuf, resume: Option<&str>) -> Result<(String, Arc<Process>)> {
        let process = self.spawn_process(&cwd)?;
        let result = async {
            let initialized = initialize(&process).await?;
            let session = if let Some(session) = resume {
                ensure!(
                    initialized.agent_capabilities.load_session,
                    "ACP server does not support session/load"
                );
                let loaded: Value = process
                    .rpc(
                        "session/load",
                        acp::LoadSessionRequest::new(session.to_string(), cwd),
                    )
                    .await?;
                let _: acp::LoadSessionResponse = serde_json::from_value(loaded.clone())?;
                self.capture_models(&loaded, &process)?;
                session.to_string()
            } else {
                let created: Value = process
                    .rpc("session/new", acp::NewSessionRequest::new(cwd))
                    .await?;
                self.capture_models(&created, &process)?;
                serde_json::from_value::<acp::NewSessionResponse>(created)?
                    .session_id
                    .to_string()
            };
            ensure!(!session.is_empty(), "ACP returned an empty session id");
            Ok(session)
        }
        .await;
        match result {
            Ok(session) => Ok((session, process)),
            Err(error) => {
                process.stop().await;
                Err(error)
            }
        }
    }
}

#[async_trait]
impl Engine for AcpEngine {
    fn id(&self) -> &str {
        &self.launch.id
    }
    fn name(&self) -> &str {
        &self.launch.name
    }
    fn models(&self) -> Vec<ModelInfo> {
        let models = self.runtime_models.lock().unwrap();
        if models.is_empty() && self.profile_settings.is_some() {
            if self.kind() == "agy" {
                super::agy::fallback_models()
            } else {
                super::hermes::fallback_models()
            }
        } else {
            models.clone()
        }
    }
    async fn is_available(&self) -> bool {
        self.launch_snapshot()
            .is_ok_and(|launch| crate::runtime_env::is_executable_file(&launch.executable))
    }

    async fn start_thread(
        &self,
        scope: ThreadScope,
        resume: Option<&str>,
        model: &str,
        sandbox: SandboxPolicy,
    ) -> Result<EngineThread> {
        ensure!(
            model.is_empty() || self.profile_settings.is_some(),
            "ACP model selection requires a launch profile"
        );
        ensure!(
            sandbox.sandbox_mode.is_none()
                && sandbox.approval_policy.is_none()
                && sandbox.permission_profile.is_none(),
            "ACP substrate does not enforce panes sandbox or approval policies"
        );
        let cwd = match scope {
            ThreadScope::Repo { repo_path } => PathBuf::from(repo_path),
            ThreadScope::Workspace { root_path, .. } => PathBuf::from(root_path),
        };
        ensure!(cwd.is_absolute(), "ACP session cwd must be absolute");
        if let Some(session) = resume {
            let slot = {
                let mut sessions = self.sessions.lock().unwrap();
                let slot = sessions
                    .entry(session.to_string())
                    .or_insert_with(|| Arc::new(SessionSlot::new(None)));
                if slot
                    .get()
                    .is_some_and(|process| !process.alive.load(Ordering::Acquire))
                {
                    *slot = Arc::new(SessionSlot::new(slot.get().cloned()));
                }
                slot.clone()
            };
            let process = slot
                .process
                .get_or_try_init(|| async {
                    if let Some(previous) = &slot.predecessor {
                        previous.stop().await;
                    }
                    self.open(cwd, Some(session))
                        .await
                        .map(|(_, process)| process)
                })
                .await?;
            self.select_model(process, session, model).await?;
            let current = {
                let configuration = self.configuration.lock().unwrap();
                configuration.revision == process.configuration_revision
                    && self
                        .sessions
                        .lock()
                        .unwrap()
                        .get(session)
                        .is_some_and(|current| Arc::ptr_eq(current, &slot))
            };
            if !current {
                process.stop().await;
                bail!("ACP configuration changed while starting session");
            }
            return Ok(EngineThread {
                engine_thread_id: session.to_string(),
            });
        }
        let (session, process) = self.open(cwd, None).await?;
        if let Err(error) = self.select_model(&process, &session, model).await {
            process.stop().await;
            return Err(error);
        }
        let slot = Arc::new(SessionSlot::new(None));
        slot.process
            .set(process.clone())
            .map_err(|_| anyhow!("ACP session already initialized"))?;
        let published = {
            let configuration = self.configuration.lock().unwrap();
            let mut sessions = self.sessions.lock().unwrap();
            if configuration.revision != process.configuration_revision
                || sessions.contains_key(&session)
            {
                false
            } else {
                sessions.insert(session.clone(), slot);
                true
            }
        };
        if !published {
            process.stop().await;
            bail!("ACP configuration changed or server reused an existing session id");
        }
        Ok(EngineThread {
            engine_thread_id: session,
        })
    }

    async fn send_message(
        &self,
        session: &str,
        input: TurnInput,
        event_tx: mpsc::Sender<EngineEvent>,
        cancellation: CancellationToken,
    ) -> Result<()> {
        let process = match self.process(session) {
            Ok(process) => process,
            Err(error) => {
                complete(
                    event_tx,
                    TurnCompletionStatus::Failed,
                    None,
                    Some(error.to_string()),
                )
                .await;
                return Ok(());
            }
        };
        let _cancel_on_drop = cancellation.clone().drop_guard();
        let (done, result) = oneshot::channel();
        let command = ClientCommand::Begin {
            session: session.to_string(),
            input,
            event_tx,
            cancellation,
            done,
        };
        if let Err(error) = process.commands.send(command).await {
            if let ClientCommand::Begin { event_tx, .. } = error.0 {
                complete(
                    event_tx,
                    TurnCompletionStatus::Failed,
                    None,
                    Some("ACP process exited".into()),
                )
                .await;
            }
            return Ok(());
        }
        let outcome = result.await.context("ACP turn dispatcher exited")?;
        if !process.alive.load(Ordering::Acquire) {
            process.reaped.cancelled().await;
        }
        outcome
    }

    async fn steer_message(&self, _: &str, _: TurnInput) -> Result<()> {
        bail!("ACP steering is unsupported")
    }

    async fn respond_to_approval(
        &self,
        approval_id: &str,
        response: Value,
        _: Option<ApprovalRequestRoute>,
    ) -> Result<()> {
        let process = {
            let sessions = self.sessions.lock().unwrap();
            sessions
                .values()
                .filter_map(|slot| slot.get())
                .find(|process| approval_id.starts_with(&process.approval_prefix))
                .cloned()
        }
        .context("stale or foreign ACP approval")?;
        let (done, result) = oneshot::channel();
        process
            .commands
            .send(ClientCommand::Approve {
                id: approval_id.to_string(),
                response,
                done,
            })
            .await
            .map_err(|_| anyhow!("stale ACP approval: process exited"))?;
        result.await.context("ACP approval dispatcher exited")?
    }

    async fn interrupt(&self, session: &str) -> Result<()> {
        let Ok(process) = self.process(session) else {
            return Ok(());
        };
        let (done, result) = oneshot::channel();
        if process
            .commands
            .send(ClientCommand::Interrupt(done))
            .await
            .is_ok()
        {
            let _ = result.await;
        }
        Ok(())
    }

    async fn archive_thread(&self, session: &str) -> Result<()> {
        let slot = self.sessions.lock().unwrap().remove(session);
        if let Some(process) = slot.and_then(|slot| slot.get().cloned()) {
            process.stop().await;
        }
        Ok(())
    }
    async fn unarchive_thread(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

async fn initialize(process: &Process) -> Result<acp::InitializeResponse> {
    let initialized: acp::InitializeResponse = process
        .rpc(
            "initialize",
            acp::InitializeRequest::new(ProtocolVersion::V1)
                .client_info(acp::Implementation::new("panes", env!("CARGO_PKG_VERSION"))),
        )
        .await?;
    ensure!(
        initialized.protocol_version == ProtocolVersion::V1,
        "incompatible ACP protocol version {}; expected 1",
        initialized.protocol_version
    );
    Ok(initialized)
}

struct Process {
    configuration_revision: u64,
    commands: mpsc::Sender<ClientCommand>,
    shutdown: CancellationToken,
    reaped: CancellationToken,
    alive: Arc<AtomicBool>,
    approval_prefix: String,
    request_timeout: Duration,
    model: Mutex<Option<String>>,
    default_model: Mutex<Option<String>>,
}

impl Drop for Process {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl Process {
    fn spawn(
        launch: &AcpLaunchConfig,
        instance: Uuid,
        cwd: &PathBuf,
        configuration_revision: u64,
        configuration_shutdown: CancellationToken,
    ) -> Result<Arc<Self>> {
        let mut command = Command::new(&launch.executable);
        command
            .args(&launch.args)
            .envs(&launch.env)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().context("spawn ACP server")?;
        let mut stdin = child.stdin.take().context("ACP stdin missing")?;
        let process_id = child.id();
        let stdout = child.stdout.take().context("ACP stdout missing")?;
        let mut stderr = child.stderr.take().context("ACP stderr missing")?;
        let (commands, receiver) = mpsc::channel(64);
        let (writer, mut writes) = mpsc::channel::<Vec<u8>>(64);
        let shutdown = CancellationToken::new();
        let reaped = CancellationToken::new();
        let alive = Arc::new(AtomicBool::new(true));
        let approval_prefix = format!("acp:{instance}:{}:", Uuid::new_v4());
        let process = Arc::new(Self {
            configuration_revision,
            commands,
            shutdown: shutdown.clone(),
            reaped: reaped.clone(),
            alive: alive.clone(),
            approval_prefix: approval_prefix.clone(),
            request_timeout: launch.request_timeout,
            model: Mutex::new(None),
            default_model: Mutex::new(None),
        });
        let turn_timeout = launch.turn_timeout;
        tokio::spawn(async move {
            let mut writer_task = tokio::spawn(async move {
                while let Some(frame) = writes.recv().await {
                    timeout(IO_TIMEOUT, stdin.write_all(&frame)).await??;
                    timeout(IO_TIMEOUT, stdin.flush()).await??;
                }
                Ok::<(), anyhow::Error>(())
            });
            let stderr_task = tokio::spawn(async move {
                let mut buffer = [0; 4096];
                while let Ok(count) = stderr.read(&mut buffer).await {
                    if count == 0 {
                        break;
                    }
                    log::debug!("ACP stderr: {}", String::from_utf8_lossy(&buffer[..count]));
                }
            });
            let mut dispatcher = Dispatcher {
                writer,
                pending: HashMap::new(),
                next_id: 0,
                active: None,
                turn_timeout,
                approval_prefix,
            };
            let mut frames = FrameReader {
                reader: BufReader::new(stdout),
                buffer: Vec::new(),
            };
            let mut receiver = receiver;
            let mut writer_joined = false;
            let outcome: Result<()> = async {
                loop {
                    let cancellation = dispatcher.active.as_ref().map(|turn| turn.cancellation.clone()).unwrap_or_default();
                    let deadline = dispatcher.active.as_ref().map(|turn| turn.deadline).unwrap_or_else(|| Instant::now() + DEFAULT_TURN_TIMEOUT);
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => bail!("ACP process stopped"),
                        _ = configuration_shutdown.cancelled() => bail!("ACP configuration changed"),
                        _ = cancellation.cancelled(), if dispatcher.active.is_some() => {
                            alive.store(false, Ordering::Release);
                            dispatcher.cancel().await?;
                            break;
                        }
                        _ = tokio::time::sleep_until(deadline), if dispatcher.active.is_some() => bail!("ACP prompt timed out"),
                        result = &mut writer_task => { writer_joined = true; result??; bail!("ACP writer closed"); }
                        command = receiver.recv() => match command {
                            Some(command) => dispatcher.command(command).await?,
                            None => break,
                        },
                        frame = frames.next() => dispatcher.frame(frame?.context("unexpected ACP stdout EOF")?).await?,
                        status = child.wait() => bail!("ACP process exited: {}", status?),
                    }
                }
                Ok(())
            }.await;
            alive.store(false, Ordering::Release);
            let error = outcome
                .err()
                .map(|error| error.to_string())
                .unwrap_or_else(|| "ACP process stopped".into());
            writer_task.abort();
            if !writer_joined {
                let _ = writer_task.await;
            }
            #[cfg(unix)]
            if let Some(pid) = process_id {
                unsafe {
                    libc::kill(-(pid as i32), libc::SIGKILL);
                }
            }
            let _ = child.start_kill();
            let _ = child.wait().await;
            stderr_task.abort();
            let _ = stderr_task.await;
            dispatcher
                .finish(TurnCompletionStatus::Failed, None, Some(error.clone()))
                .await;
            for (_, reply) in dispatcher.pending.drain() {
                let _ = reply.send(Err(anyhow!(error.clone())));
            }
            receiver.close();
            while let Some(command) = receiver.recv().await {
                reject(command, &error).await;
            }
            reaped.cancel();
        });
        Ok(process)
    }

    async fn rpc<T: DeserializeOwned>(&self, method: &str, params: impl Serialize) -> Result<T> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(ClientCommand::Rpc {
                method: method.to_string(),
                params: serde_json::to_value(params)?,
                reply,
            })
            .await
            .map_err(|_| anyhow!("ACP dispatcher closed"))?;
        match timeout(self.request_timeout, result).await {
            Ok(result) => Ok(serde_json::from_value(
                result.context("ACP RPC dispatcher closed")??,
            )?),
            Err(_) => {
                self.stop().await;
                bail!("ACP {method} timed out")
            }
        }
    }

    async fn stop(&self) {
        self.shutdown.cancel();
        self.reaped.cancelled().await;
    }
}

struct FrameReader<R> {
    reader: BufReader<R>,
    buffer: Vec<u8>,
}

impl<R: tokio::io::AsyncRead + Unpin> FrameReader<R> {
    async fn next(&mut self) -> Result<Option<Value>> {
        loop {
            let bytes = self.reader.fill_buf().await?;
            if bytes.is_empty() {
                ensure!(self.buffer.is_empty(), "incomplete ACP stdout frame");
                return Ok(None);
            }
            let newline = bytes.iter().position(|byte| *byte == b'\n');
            let count = newline.map_or(bytes.len(), |index| index + 1);
            ensure!(
                self.buffer.len() + count <= MAX_FRAME,
                "ACP stdout frame exceeds limit"
            );
            self.buffer.extend_from_slice(&bytes[..count]);
            self.reader.consume(count);
            if newline.is_some() {
                let frame =
                    serde_json::from_slice(&self.buffer).context("malformed ACP stdout JSON")?;
                self.buffer.clear();
                return Ok(Some(frame));
            }
        }
    }
}

enum ClientCommand {
    Rpc {
        method: String,
        params: Value,
        reply: Reply<Value>,
    },
    Begin {
        session: String,
        input: TurnInput,
        event_tx: mpsc::Sender<EngineEvent>,
        cancellation: CancellationToken,
        done: Reply<()>,
    },
    Approve {
        id: String,
        response: Value,
        done: Reply<()>,
    },
    Interrupt(Reply<()>),
}

struct Dispatcher {
    writer: mpsc::Sender<Vec<u8>>,
    pending: HashMap<String, Reply<Value>>,
    next_id: u64,
    active: Option<Turn>,
    turn_timeout: Duration,
    approval_prefix: String,
}

struct Turn {
    generation: Uuid,
    session: String,
    request_id: String,
    events: mpsc::Sender<EngineEvent>,
    cancellation: CancellationToken,
    deadline: Instant,
    done: Reply<()>,
    approvals: HashMap<String, Permission>,
    tools: HashMap<String, ToolState>,
}

struct Permission {
    rpc_id: Value,
    request: acp::RequestPermissionRequest,
}
struct ToolState {
    action_id: String,
    snapshot: String,
    completed: bool,
    diverged: bool,
    diff: Option<String>,
}

impl Dispatcher {
    fn write(&self, value: Value) -> Result<()> {
        let mut frame = serde_json::to_vec(&value)?;
        ensure!(frame.len() < MAX_FRAME, "ACP outgoing frame exceeds limit");
        frame.push(b'\n');
        self.writer
            .try_send(frame)
            .map_err(|_| anyhow!("ACP writer closed or backpressured"))
    }

    fn request(&mut self, method: &str, params: Value) -> Result<String> {
        ensure!(self.pending.len() < 128, "too many pending ACP requests");
        self.next_id += 1;
        let id = format!("client-{}", self.next_id);
        self.write(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))?;
        Ok(id)
    }

    async fn command(&mut self, command: ClientCommand) -> Result<()> {
        match command {
            ClientCommand::Rpc {
                method,
                params,
                reply,
            } => {
                if method == "session/set_model" && self.active.is_some() {
                    let _ = reply.send(Err(anyhow!(
                        "cannot change ACP model during an active turn"
                    )));
                    return Ok(());
                }
                match self.request(&method, params) {
                    Ok(id) => {
                        self.pending.insert(id, reply);
                    }
                    Err(error) => {
                        let _ = reply.send(Err(anyhow!(error.to_string())));
                        return Err(error);
                    }
                }
            }
            ClientCommand::Begin {
                session,
                input,
                event_tx,
                cancellation,
                done,
            } => {
                let validation = if self.active.is_some() {
                    Some("ACP session already has an active turn")
                } else if !input.attachments.is_empty()
                    || input
                        .input_items
                        .iter()
                        .any(|item| !matches!(item, super::TurnInputItem::Text { .. }))
                    || input.plan_mode
                {
                    Some("ACP substrate supports plain text input only")
                } else {
                    None
                };
                if let Some(error) = validation {
                    complete(
                        event_tx,
                        TurnCompletionStatus::Failed,
                        None,
                        Some(error.into()),
                    )
                    .await;
                    let _ = done.send(Ok(()));
                    return Ok(());
                }
                let content = if input.input_items.is_empty() {
                    vec![acp::ContentBlock::Text(acp::TextContent::new(
                        input.message,
                    ))]
                } else {
                    input
                        .input_items
                        .into_iter()
                        .filter_map(|item| match item {
                            super::TurnInputItem::Text { text } => {
                                Some(acp::ContentBlock::Text(acp::TextContent::new(text)))
                            }
                            _ => None,
                        })
                        .collect()
                };
                let prompt = acp::PromptRequest::new(session.clone(), content);
                self.active = Some(Turn {
                    generation: Uuid::new_v4(),
                    session,
                    request_id: String::new(),
                    events: event_tx,
                    cancellation,
                    deadline: Instant::now() + self.turn_timeout,
                    done,
                    approvals: HashMap::new(),
                    tools: HashMap::new(),
                });
                if self.active.as_ref().unwrap().cancellation.is_cancelled() {
                    return Ok(());
                }
                let id = self.request("session/prompt", serde_json::to_value(prompt)?)?;
                self.active.as_mut().unwrap().request_id = id;
            }
            ClientCommand::Approve { id, response, done } => {
                let result = self.approve(&id, response);
                let _ = done.send(result);
            }
            ClientCommand::Interrupt(done) => {
                if let Some(turn) = &self.active {
                    turn.cancellation.cancel();
                }
                let _ = done.send(Ok(()));
            }
        }
        Ok(())
    }

    async fn frame(&mut self, frame: Value) -> Result<()> {
        ensure!(
            frame.is_object() && frame["jsonrpc"] == "2.0",
            "invalid ACP JSON-RPC envelope"
        );
        if let Some(method) = frame.get("method") {
            let method = method.as_str().context("ACP method must be a string")?;
            ensure!(
                frame.get("result").is_none() && frame.get("error").is_none(),
                "invalid ACP request envelope"
            );
            let params = frame.get("params").cloned().unwrap_or(Value::Null);
            if let Some(id) = frame.get("id") {
                ensure!(
                    id.is_string() || id.is_i64() || id.is_u64(),
                    "invalid ACP request id"
                );
                if method == "session/request_permission" {
                    self.permission(
                        id.clone(),
                        serde_json::from_value(params)
                            .context("malformed ACP permission request")?,
                    )
                    .await?;
                } else {
                    self.write(json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Client capability is not supported"}}))?;
                }
            } else if method == "session/update" {
                self.notification(params).await?;
            } else {
                log::debug!("Ignoring optional ACP notification {method}");
            }
            return Ok(());
        }
        let id = frame.get("id").context("ACP response id missing")?;
        ensure!(
            id.is_string() || id.is_i64() || id.is_u64(),
            "invalid ACP response id"
        );
        ensure!(
            frame.get("result").is_some() != frame.get("error").is_some(),
            "invalid ACP response envelope"
        );
        let result = if let Some(error) = frame.get("error") {
            ensure!(
                error["code"].is_i64() && error["message"].is_string(),
                "malformed ACP RPC error"
            );
            Err(anyhow!(
                "ACP RPC error {}: {}",
                error["code"],
                error["message"]
            ))
        } else {
            Ok(frame["result"].clone())
        };
        let key = id.as_str().unwrap_or_default();
        if self
            .active
            .as_ref()
            .is_some_and(|turn| turn.request_id == key)
        {
            let response: acp::PromptResponse = serde_json::from_value(result?)?;
            let usage = response.usage.map(|usage| TokenUsage {
                input: usage.input_tokens,
                output: usage.output_tokens,
                reasoning: usage.thought_tokens,
                cache_read: usage.cached_read_tokens,
                cache_write: usage.cached_write_tokens,
                cost_usd: None,
            });
            let status = if response.stop_reason == acp::StopReason::Cancelled {
                TurnCompletionStatus::Interrupted
            } else {
                TurnCompletionStatus::Completed
            };
            self.finish(status, usage, None).await;
        } else if let Some(reply) = self.pending.remove(key) {
            let _ = reply.send(result);
        } else {
            log::debug!("Ignoring stale ACP response id");
        }
        Ok(())
    }

    async fn notification(&mut self, params: Value) -> Result<()> {
        ensure!(
            params["sessionId"].is_string(),
            "ACP update session id missing"
        );
        let kind = params["update"]["sessionUpdate"]
            .as_str()
            .context("ACP session update discriminator missing")?;
        if !matches!(
            kind,
            "agent_message_chunk"
                | "agent_thought_chunk"
                | "tool_call"
                | "tool_call_update"
                | "user_message_chunk"
                | "plan"
                | "available_commands_update"
                | "current_mode_update"
                | "config_option_update"
                | "session_info_update"
                | "usage_update"
        ) {
            log::debug!("Ignoring optional ACP session update {kind}");
            return Ok(());
        }
        let notification: acp::SessionNotification =
            serde_json::from_value(params).context("malformed ACP session update")?;
        let Some(turn) = &mut self.active else {
            return Ok(());
        };
        ensure!(
            notification.session_id.to_string() == turn.session,
            "ACP update has wrong session id"
        );
        match notification.update {
            acp::SessionUpdate::AgentMessageChunk(chunk) => {
                if let acp::ContentBlock::Text(text) = chunk.content {
                    emit(&turn.events, EngineEvent::TextDelta { content: text.text }).await?;
                }
            }
            acp::SessionUpdate::AgentThoughtChunk(chunk) => {
                if let acp::ContentBlock::Text(text) = chunk.content {
                    emit(
                        &turn.events,
                        EngineEvent::ThinkingDelta { content: text.text },
                    )
                    .await?;
                }
            }
            acp::SessionUpdate::ToolCall(tool) => {
                let details = serde_json::to_value(&tool)?;
                turn.tool(
                    tool.tool_call_id.to_string(),
                    Some(tool.title),
                    Some(tool.kind),
                    Some(tool.status),
                    Some(tool.content),
                    details,
                )
                .await?;
            }
            acp::SessionUpdate::ToolCallUpdate(tool) => {
                let details = serde_json::to_value(&tool)?;
                turn.tool(
                    tool.tool_call_id.to_string(),
                    tool.fields.title,
                    tool.fields.kind,
                    tool.fields.status,
                    tool.fields.content,
                    details,
                )
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn permission(
        &mut self,
        id: Value,
        request: acp::RequestPermissionRequest,
    ) -> Result<()> {
        let Some(turn) = &mut self.active else {
            return self.write(json!({"jsonrpc":"2.0","id":id,"result":acp::RequestPermissionResponse::new(acp::RequestPermissionOutcome::Cancelled)}));
        };
        ensure!(
            request.session_id.to_string() == turn.session,
            "ACP permission has wrong session id"
        );
        ensure!(turn.approvals.len() < 128, "too many ACP approvals");
        ensure!(
            !turn
                .approvals
                .values()
                .any(|permission| permission.rpc_id == id),
            "duplicate ACP permission request id"
        );
        let approval_id = format!(
            "{}{}:{}",
            self.approval_prefix,
            turn.generation,
            Uuid::new_v4()
        );
        let mut details = json!({"sessionId":turn.session,"processGeneration":self.approval_prefix,"turnGeneration":turn.generation,"requestId":id,"options":request.options,"toolCall":request.tool_call});
        if let Some(content) = &request.tool_call.fields.content {
            if let Some(diff) = content_diff(content)? {
                details["diff"] = Value::String(diff);
            }
        }
        let summary = request
            .tool_call
            .fields
            .title
            .clone()
            .unwrap_or_else(|| "ACP permission requested".into());
        let action_type = action_type(request.tool_call.fields.kind.unwrap_or_default());
        turn.approvals.insert(
            approval_id.clone(),
            Permission {
                rpc_id: id,
                request,
            },
        );
        emit(
            &turn.events,
            EngineEvent::ApprovalRequested {
                approval_id,
                action_type,
                summary,
                details,
            },
        )
        .await
    }

    fn approve(&mut self, id: &str, response: Value) -> Result<()> {
        let turn = self
            .active
            .as_mut()
            .context("stale ACP approval: turn completed")?;
        ensure!(
            !turn.cancellation.is_cancelled(),
            "stale ACP approval: turn cancelled"
        );
        let permission = turn
            .approvals
            .get(id)
            .context("stale or foreign ACP approval")?;
        let outcome = if response.get("decision").and_then(Value::as_str) == Some("cancel") {
            acp::RequestPermissionOutcome::Cancelled
        } else {
            let option_id = response
                .get("optionId")
                .and_then(Value::as_str)
                .context("ACP approval requires an original optionId or decision=cancel")?;
            let option = permission
                .request
                .options
                .iter()
                .find(|option| option.option_id.to_string() == option_id)
                .context("unknown ACP permission optionId")?;
            acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(
                option.option_id.clone(),
            ))
        };
        let rpc_id = permission.rpc_id.clone();
        self.write(json!({"jsonrpc":"2.0","id":rpc_id,"result":acp::RequestPermissionResponse::new(outcome)}))?;
        self.active.as_mut().unwrap().approvals.remove(id);
        Ok(())
    }

    async fn cancel(&mut self) -> Result<()> {
        let session = self
            .active
            .as_ref()
            .context("no ACP turn to cancel")?
            .session
            .clone();
        self.cancel_approvals();
        let result = self.write(json!({"jsonrpc":"2.0","method":"session/cancel","params":acp::CancelNotification::new(session)}));
        self.finish(TurnCompletionStatus::Interrupted, None, None)
            .await;
        tokio::time::sleep(CANCEL_GRACE).await;
        result
    }

    fn cancel_approvals(&mut self) {
        if let Some(turn) = &mut self.active {
            let approvals = std::mem::take(&mut turn.approvals);
            for (_, permission) in approvals {
                let _ = self.write(json!({"jsonrpc":"2.0","id":permission.rpc_id,"result":acp::RequestPermissionResponse::new(acp::RequestPermissionOutcome::Cancelled)}));
            }
        }
    }

    async fn finish(
        &mut self,
        status: TurnCompletionStatus,
        usage: Option<TokenUsage>,
        error: Option<String>,
    ) {
        self.cancel_approvals();
        if let Some(turn) = self.active.take() {
            complete(turn.events, status, usage, error).await;
            let _ = turn.done.send(Ok(()));
        }
    }
}

impl Turn {
    async fn tool(
        &mut self,
        id: String,
        title: Option<String>,
        kind: Option<acp::ToolKind>,
        status: Option<acp::ToolCallStatus>,
        content: Option<Vec<acp::ToolCallContent>>,
        details: Value,
    ) -> Result<()> {
        if !self.tools.contains_key(&id) {
            ensure!(self.tools.len() < MAX_TOOLS, "ACP tool count exceeds limit");
            let action_id = format!("acp-action:{}:{}", self.generation, Uuid::new_v4());
            emit(
                &self.events,
                EngineEvent::ActionStarted {
                    action_id: action_id.clone(),
                    engine_action_id: Some(id.clone()),
                    action_type: action_type(kind.unwrap_or_default()),
                    summary: title.unwrap_or_else(|| "ACP tool".into()),
                    details,
                    agent_id: None,
                },
            )
            .await?;
            self.tools.insert(
                id.clone(),
                ToolState {
                    action_id,
                    snapshot: String::new(),
                    completed: false,
                    diverged: false,
                    diff: None,
                },
            );
        }
        let tool = self.tools.get_mut(&id).unwrap();
        if tool.completed {
            return Ok(());
        }
        if let Some(content) = content {
            let diff = content_diff(&content)?;
            if diff != tool.diff {
                if let Some(diff) = &diff {
                    emit(
                        &self.events,
                        EngineEvent::DiffUpdated {
                            diff: diff.clone(),
                            scope: super::DiffScope::File,
                        },
                    )
                    .await?;
                }
                tool.diff = diff;
            }
            let mut snapshot = String::new();
            for block in content {
                if let acp::ToolCallContent::Content(content) = block {
                    if let acp::ContentBlock::Text(text) = content.content {
                        ensure!(
                            snapshot.len() + text.text.len() <= MAX_TOOL_SNAPSHOT,
                            "ACP tool snapshot exceeds limit"
                        );
                        snapshot.push_str(&text.text);
                    }
                }
            }
            tool.diverged |= !snapshot.starts_with(&tool.snapshot);
            if let Some(delta) = snapshot
                .strip_prefix(&tool.snapshot)
                .filter(|delta| !delta.is_empty() && !tool.diverged)
            {
                emit(
                    &self.events,
                    EngineEvent::ActionOutputDelta {
                        action_id: tool.action_id.clone(),
                        stream: OutputStream::Stdout,
                        content: trim_action_output_delta_content(delta),
                    },
                )
                .await?;
            }
            tool.snapshot = snapshot;
        }
        if matches!(
            status,
            Some(acp::ToolCallStatus::Completed | acp::ToolCallStatus::Failed)
        ) {
            tool.completed = true;
            let success = status == Some(acp::ToolCallStatus::Completed);
            emit(
                &self.events,
                EngineEvent::ActionCompleted {
                    action_id: tool.action_id.clone(),
                    result: ActionResult {
                        success,
                        output: Some(trim_action_output_delta_content(&tool.snapshot)),
                        error: (!success).then(|| "ACP tool failed".into()),
                        diff: tool.diff.clone(),
                        duration_ms: 0,
                    },
                },
            )
            .await?;
        }
        Ok(())
    }
}

fn content_diff(content: &[acp::ToolCallContent]) -> Result<Option<String>> {
    let mut patches = String::new();
    for block in content {
        if let acp::ToolCallContent::Diff(diff) = block {
            let old = diff.old_text.as_deref().unwrap_or("");
            ensure!(
                old.len() + diff.new_text.len() <= MAX_TOOL_SNAPSHOT,
                "ACP diff exceeds limit"
            );
            let mut patch = git2::Patch::from_buffers(
                old.as_bytes(),
                Some(&diff.path),
                diff.new_text.as_bytes(),
                Some(&diff.path),
                None,
            )?;
            let buffer = patch.to_buf()?;
            ensure!(
                patches.len() + buffer.len() <= super::STREAMED_DIFF_MAX_CHARS,
                "ACP diff output exceeds limit"
            );
            patches.push_str(&String::from_utf8_lossy(&buffer));
        }
    }
    Ok((!patches.is_empty()).then_some(patches))
}

fn action_type(kind: acp::ToolKind) -> ActionType {
    match kind {
        acp::ToolKind::Read => ActionType::FileRead,
        acp::ToolKind::Edit => ActionType::FileEdit,
        acp::ToolKind::Delete => ActionType::FileDelete,
        acp::ToolKind::Execute => ActionType::Command,
        acp::ToolKind::Search => ActionType::Search,
        _ => ActionType::Other,
    }
}

async fn emit(events: &mpsc::Sender<EngineEvent>, event: EngineEvent) -> Result<()> {
    timeout(IO_TIMEOUT, events.send(event))
        .await
        .context("ACP event consumer backpressured")?
        .map_err(|_| anyhow!("ACP event consumer closed"))
}

async fn complete(
    events: mpsc::Sender<EngineEvent>,
    status: TurnCompletionStatus,
    token_usage: Option<TokenUsage>,
    error: Option<String>,
) {
    if let Some(message) = error {
        let _ = emit(
            &events,
            EngineEvent::Error {
                message,
                recoverable: false,
            },
        )
        .await;
    }
    let _ = emit(
        &events,
        EngineEvent::TurnCompleted {
            token_usage,
            status,
        },
    )
    .await;
}

async fn reject(command: ClientCommand, error: &str) {
    match command {
        ClientCommand::Rpc { reply, .. } => {
            let _ = reply.send(Err(anyhow!(error.to_string())));
        }
        ClientCommand::Begin { event_tx, done, .. } => {
            complete(
                event_tx,
                TurnCompletionStatus::Failed,
                None,
                Some(error.into()),
            )
            .await;
            let _ = done.send(Ok(()));
        }
        ClientCommand::Approve { done, .. } | ClientCommand::Interrupt(done) => {
            let _ = done.send(Err(anyhow!(error.to_string())));
        }
    }
}

#[cfg(test)]
#[path = "acp_tests.rs"]
mod tests;
