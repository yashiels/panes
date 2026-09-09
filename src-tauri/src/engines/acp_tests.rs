use super::*;

struct Fixture {
    engine: Arc<AcpEngine>,
    log: PathBuf,
}

impl Fixture {
    fn new(scenario: &str) -> Self {
        let python = which::which("python3")
            .or_else(|_| which::which("python"))
            .expect("Python is required for the ACP fake subprocess");
        let log = std::env::temp_dir().join(format!("panes-acp-{}.jsonl", Uuid::new_v4()));
        let engine = Arc::new(AcpEngine::new(AcpLaunchConfig {
            id: "test_product".into(),
            name: "Test product".into(),
            executable: python,
            args: vec![
                format!(
                    "{}/tests/fixtures/acp/fake_server.py",
                    env!("CARGO_MANIFEST_DIR")
                ),
                scenario.into(),
                log.to_string_lossy().into_owned(),
            ],
            env: BTreeMap::new(),
            request_timeout: if scenario == "timeout_init" {
                Duration::from_millis(300)
            } else {
                DEFAULT_REQUEST_TIMEOUT
            },
            turn_timeout: if scenario == "timeout" {
                Duration::from_millis(300)
            } else {
                DEFAULT_TURN_TIMEOUT
            },
        }));
        Self { engine, log }
    }

    async fn start(&self, resume: Option<&str>) -> Result<String> {
        timeout(
            Duration::from_secs(10),
            self.engine.start_thread(
                ThreadScope::Repo {
                    repo_path: std::env::temp_dir().to_string_lossy().into_owned(),
                },
                resume,
                "",
                sandbox(),
            ),
        )
        .await?
        .map(|thread| thread.engine_thread_id)
    }

    fn requests(&self) -> Vec<Value> {
        std::fs::read_to_string(&self.log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    async fn stop(&self, session: &str) {
        self.engine.archive_thread(session).await.unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.log);
    }
}

fn sandbox() -> SandboxPolicy {
    SandboxPolicy {
        writable_roots: vec![],
        allow_network: false,
        approval_policy: None,
        permission_profile: None,
        approvals_reviewer: None,
        reasoning_effort: None,
        sandbox_mode: None,
        service_tier: None,
        personality: None,
        output_schema: None,
        opencode_agent: None,
    }
}

fn input() -> TurnInput {
    TurnInput {
        message: "fixture".into(),
        attachments: vec![],
        plan_mode: false,
        input_items: vec![],
    }
}

fn begin(
    engine: Arc<AcpEngine>,
    session: String,
    cancellation: CancellationToken,
) -> (
    mpsc::Receiver<EngineEvent>,
    tokio::task::JoinHandle<Result<()>>,
) {
    let (events, receiver) = mpsc::channel(64);
    let task = tokio::spawn(async move {
        engine
            .send_message(&session, input(), events, cancellation)
            .await
    });
    (receiver, task)
}

async fn drain(receiver: &mut mpsc::Receiver<EngineEvent>) -> Vec<EngineEvent> {
    timeout(Duration::from_secs(10), async {
        let mut events = Vec::new();
        while let Some(event) = receiver.recv().await {
            events.push(event);
        }
        events
    })
    .await
    .expect("turn event sender must be dropped")
}

fn terminal(events: &[EngineEvent], expected: TurnCompletionStatus) {
    let completions: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::TurnCompleted { status, .. } => Some(status),
            _ => None,
        })
        .collect();
    assert_eq!(completions, vec![&expected]);
    assert!(matches!(
        events.last(),
        Some(EngineEvent::TurnCompleted { .. })
    ));
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnStarted { .. })));
}

async fn approve_turn(fixture: &Fixture, session: &str) -> (Vec<EngineEvent>, String) {
    let (mut receiver, task) = begin(
        fixture.engine.clone(),
        session.into(),
        CancellationToken::new(),
    );
    let mut events = vec![];
    let approval = timeout(Duration::from_secs(10), async {
        loop {
            let event = receiver.recv().await.expect("approval event");
            let id = if let EngineEvent::ApprovalRequested {
                approval_id,
                details,
                ..
            } = &event
            {
                assert_eq!(details["requestId"], 7);
                assert_eq!(details["sessionId"], session);
                assert_eq!(details["options"][0]["optionId"], "original/allow-once");
                Some(approval_id.clone())
            } else {
                None
            };
            events.push(event);
            if let Some(id) = id {
                break id;
            }
        }
    })
    .await
    .unwrap();
    assert!(fixture
        .engine
        .respond_to_approval(&approval, json!({"optionId":"guessed"}), None)
        .await
        .is_err());
    fixture
        .engine
        .respond_to_approval(&approval, json!({"optionId":"original/allow-once"}), None)
        .await
        .unwrap();
    events.extend(drain(&mut receiver).await);
    task.await.unwrap().unwrap();
    (events, approval)
}

#[tokio::test]
async fn acp_bidirectional_streaming_permissions_tools_and_reuse() {
    let fixture = Fixture::new("normal");
    let session = fixture.start(None).await.unwrap();
    assert_eq!(session, "fake-durable-session");
    assert_eq!(fixture.engine.id(), "test_product");
    assert!(!fixture.engine.supports_steering());
    let process = fixture.engine.process(&session).unwrap();
    let (events, approval) = approve_turn(&fixture, &session).await;
    terminal(&events, TurnCompletionStatus::Completed);
    let text: String = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::TextDelta { content } => Some(content.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "hello world");
    assert!(events.iter().any(
        |event| matches!(event, EngineEvent::ThinkingDelta { content } if content == "thinking")
    ));
    let output: String = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::ActionOutputDelta { content, .. } => Some(content.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(output, "abc");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, EngineEvent::ActionStarted { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, EngineEvent::ActionCompleted { .. }))
            .count(),
        1
    );
    assert!(matches!(
        events.last(),
        Some(EngineEvent::TurnCompleted {
            token_usage: Some(TokenUsage {
                input: 10,
                output: 5,
                reasoning: Some(2),
                ..
            }),
            ..
        })
    ));
    assert!(fixture
        .engine
        .respond_to_approval(&approval, json!({"optionId":"original/allow-once"}), None)
        .await
        .is_err());
    assert_eq!(fixture.start(Some(&session)).await.unwrap(), session);
    assert!(Arc::ptr_eq(
        &process,
        &fixture.engine.process(&session).unwrap()
    ));
    let (second, next_approval) = approve_turn(&fixture, &session).await;
    terminal(&second, TurnCompletionStatus::Completed);
    assert_ne!(approval, next_approval);
    let requests = fixture.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|value| value["method"] == "session/new")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|value| value["method"] == "session/load")
            .count(),
        0
    );
    fixture.stop(&session).await;
    assert!(process.reaped.is_cancelled());
}

#[tokio::test]
async fn acp_load_after_process_loss_and_concurrent_start() {
    let fixture = Fixture::new("normal");
    let session = fixture.start(None).await.unwrap();
    let process = fixture.engine.process(&session).unwrap();
    process.stop().await;
    let (one, two) = tokio::join!(fixture.start(Some(&session)), fixture.start(Some(&session)));
    assert_eq!(one.unwrap(), session);
    assert_eq!(two.unwrap(), session);
    assert!(!Arc::ptr_eq(
        &process,
        &fixture.engine.process(&session).unwrap()
    ));
    let (events, _) = approve_turn(&fixture, &session).await;
    terminal(&events, TurnCompletionStatus::Completed);
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TextDelta { content } if content == "history")));
    assert_eq!(
        fixture
            .requests()
            .iter()
            .filter(|value| value["method"] == "session/load")
            .count(),
        1
    );
    fixture.stop(&session).await;
}

#[tokio::test]
async fn acp_negotiation_and_initialization_failures() {
    for scenario in [
        "version",
        "no_load",
        "malformed_init",
        "death_init",
        "timeout_init",
    ] {
        let fixture = Fixture::new(scenario);
        assert!(
            fixture.start(Some("saved-session")).await.is_err(),
            "{scenario}"
        );
        assert!(!fixture
            .requests()
            .iter()
            .any(|value| value["method"] == "session/load"));
    }
}

#[tokio::test]
async fn acp_protocol_failures_complete_once_and_reap() {
    for scenario in [
        "timeout",
        "death",
        "malformed",
        "partial",
        "oversized",
        "bad_update",
        "wrong_session",
        "rpc_error",
        "duplicate_permission",
    ] {
        let fixture = Fixture::new(scenario);
        let session = fixture.start(None).await.unwrap();
        let process = fixture.engine.process(&session).unwrap();
        let (mut receiver, task) = begin(
            fixture.engine.clone(),
            session.clone(),
            CancellationToken::new(),
        );
        let events = drain(&mut receiver).await;
        task.await.unwrap().unwrap();
        terminal(&events, TurnCompletionStatus::Failed);
        assert!(process.reaped.is_cancelled(), "{scenario}");
        for event in events {
            if let EngineEvent::ApprovalRequested { approval_id, .. } = event {
                assert!(fixture
                    .engine
                    .respond_to_approval(&approval_id, json!({"decision":"cancel"}), None)
                    .await
                    .is_err());
            }
        }
        fixture.stop(&session).await;
    }
}

#[tokio::test]
async fn acp_token_and_interrupt_share_cancellation_gate() {
    for token_first in [true, false] {
        let fixture = Fixture::new("wait");
        let session = fixture.start(None).await.unwrap();
        let process = fixture.engine.process(&session).unwrap();
        let cancellation = CancellationToken::new();
        let (mut receiver, task) = begin(
            fixture.engine.clone(),
            session.clone(),
            cancellation.clone(),
        );
        let mut events = vec![];
        let approval = timeout(Duration::from_secs(10), async {
            loop {
                let event = receiver.recv().await.unwrap();
                let id = match &event {
                    EngineEvent::ApprovalRequested { approval_id, .. } => Some(approval_id.clone()),
                    _ => None,
                };
                events.push(event);
                if let Some(id) = id {
                    break id;
                }
            }
        })
        .await
        .unwrap();
        if token_first {
            cancellation.cancel();
        } else {
            fixture.engine.interrupt(&session).await.unwrap();
        }
        cancellation.cancel();
        fixture.engine.interrupt(&session).await.unwrap();
        events.extend(drain(&mut receiver).await);
        fixture.start(Some(&session)).await.unwrap();
        assert!(process.reaped.is_cancelled());
        task.await.unwrap().unwrap();
        terminal(&events, TurnCompletionStatus::Interrupted);
        assert!(process.reaped.is_cancelled());
        assert!(fixture
            .engine
            .respond_to_approval(&approval, json!({"optionId":"original/allow-once"}), None)
            .await
            .is_err());
        let requests = fixture.requests();
        assert_eq!(
            requests
                .iter()
                .filter(|value| value["method"] == "session/cancel")
                .count(),
            1
        );
        assert!(requests
            .iter()
            .any(|value| value["id"] == 7 && value["result"]["outcome"]["outcome"] == "cancelled"));
        fixture.start(Some(&session)).await.unwrap();
        assert!(fixture
            .engine
            .respond_to_approval(&approval, json!({"optionId":"original/allow-once"}), None)
            .await
            .is_err());
        fixture.stop(&session).await;
    }
}

#[tokio::test]
async fn acp_approvals_cannot_cross_engine_instances() {
    let first = Fixture::new("wait");
    let second = Fixture::new("wait");
    let session = first.start(None).await.unwrap();
    second.start(None).await.unwrap();
    let cancellation = CancellationToken::new();
    let (mut receiver, task) = begin(first.engine.clone(), session.clone(), cancellation.clone());
    let approval = timeout(Duration::from_secs(10), async {
        loop {
            if let EngineEvent::ApprovalRequested { approval_id, .. } =
                receiver.recv().await.unwrap()
            {
                break approval_id;
            }
        }
    })
    .await
    .unwrap();
    assert!(second
        .engine
        .respond_to_approval(&approval, json!({"optionId":"original/allow-once"}), None)
        .await
        .is_err());
    cancellation.cancel();
    terminal(
        &drain(&mut receiver).await,
        TurnCompletionStatus::Interrupted,
    );
    task.await.unwrap().unwrap();
    first.stop(&session).await;
    second.stop(&session).await;
}

#[tokio::test]
async fn acp_duplicate_completion_is_ignored() {
    let fixture = Fixture::new("duplicate_completion");
    let session = fixture.start(None).await.unwrap();
    let (events, _) = approve_turn(&fixture, &session).await;
    terminal(&events, TurnCompletionStatus::Completed);
    fixture.stop(&session).await;
}

#[test]
fn acp_recorded_agy_frames_match_pinned_schema() {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/acp");
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl") {
            continue;
        }
        let fixture = std::fs::read_to_string(path).unwrap();
        for line in fixture.lines() {
            let value: Value = serde_json::from_str(line).unwrap();
            assert_eq!(value["jsonrpc"], "2.0");
            if value["method"] == "session/update" {
                serde_json::from_value::<acp::SessionNotification>(value["params"].clone())
                    .unwrap();
            }
            if value["result"]["protocolVersion"].is_number() {
                let init: acp::InitializeResponse =
                    serde_json::from_value(value["result"].clone()).unwrap();
                assert_eq!(init.protocol_version, ProtocolVersion::V1);
            }
            if value["result"]["stopReason"].is_string() {
                serde_json::from_value::<acp::PromptResponse>(value["result"].clone()).unwrap();
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires an authenticated ACP server through PANES_ACP_SMOKE_COMMAND"]
async fn acp_real_server_smoke() {
    let executable = std::env::var_os("PANES_ACP_SMOKE_COMMAND").expect("ACP executable");
    let cwd = std::env::temp_dir().join(format!("panes-acp-smoke-{}", Uuid::new_v4()));
    std::fs::create_dir(&cwd).unwrap();
    let engine = Arc::new(AcpEngine::new(AcpLaunchConfig {
        id: "smoke".into(),
        name: "Smoke".into(),
        executable: executable.into(),
        args: vec![],
        env: BTreeMap::new(),
        request_timeout: DEFAULT_REQUEST_TIMEOUT,
        turn_timeout: DEFAULT_TURN_TIMEOUT,
    }));
    let thread = engine
        .start_thread(
            ThreadScope::Repo {
                repo_path: cwd.to_string_lossy().into_owned(),
            },
            None,
            "",
            sandbox(),
        )
        .await
        .unwrap();
    let (events, mut receiver) = mpsc::channel(64);
    let session = thread.engine_thread_id.clone();
    let runner = engine.clone();
    let task = tokio::spawn(async move {
        runner
            .send_message(
                &session,
                TurnInput {
                    message: "Reply with ACP smoke ok. Do not use tools.".into(),
                    ..input()
                },
                events,
                CancellationToken::new(),
            )
            .await
    });
    let events = timeout(Duration::from_secs(90), async {
        let mut events = vec![];
        while let Some(event) = receiver.recv().await {
            events.push(event);
        }
        events
    })
    .await
    .unwrap();
    task.await.unwrap().unwrap();
    engine
        .archive_thread(&thread.engine_thread_id)
        .await
        .unwrap();
    std::fs::remove_dir_all(cwd).unwrap();
    terminal(&events, TurnCompletionStatus::Completed);
    assert!(events.iter().any(|event| matches!(event, EngineEvent::TextDelta { content } if content.contains("ACP smoke ok"))));
}

#[tokio::test]
async fn acp_precancelled_turn_sends_no_prompt() {
    let fixture = Fixture::new("wait");
    let session = fixture.start(None).await.unwrap();
    let token = CancellationToken::new();
    token.cancel();
    let (mut receiver, task) = begin(fixture.engine.clone(), session.clone(), token);
    terminal(
        &drain(&mut receiver).await,
        TurnCompletionStatus::Interrupted,
    );
    task.await.unwrap().unwrap();
    assert!(!fixture
        .requests()
        .iter()
        .any(|value| value["method"] == "session/prompt"));
    fixture.stop(&session).await;
}

#[tokio::test]
async fn acp_overlapping_prompts_are_rejected_without_stopping_first_turn() {
    let fixture = Fixture::new("wait");
    let session = fixture.start(None).await.unwrap();
    let token = CancellationToken::new();
    let (mut first, first_task) = begin(fixture.engine.clone(), session.clone(), token.clone());
    timeout(Duration::from_secs(10), first.recv())
        .await
        .unwrap()
        .unwrap();
    let (mut second, second_task) = begin(
        fixture.engine.clone(),
        session.clone(),
        CancellationToken::new(),
    );
    terminal(&drain(&mut second).await, TurnCompletionStatus::Failed);
    second_task.await.unwrap().unwrap();
    assert!(fixture.engine.process(&session).is_ok());
    token.cancel();
    terminal(&drain(&mut first).await, TurnCompletionStatus::Interrupted);
    first_task.await.unwrap().unwrap();
    assert_eq!(
        fixture
            .requests()
            .iter()
            .filter(|value| value["method"] == "session/prompt")
            .count(),
        1
    );
    fixture.stop(&session).await;
}

#[tokio::test]
async fn acp_abandoned_turn_and_dropped_engine_reap_children() {
    let fixture = Fixture::new("wait");
    let session = fixture.start(None).await.unwrap();
    let process = fixture.engine.process(&session).unwrap();
    let (mut receiver, task) = begin(
        fixture.engine.clone(),
        session.clone(),
        CancellationToken::new(),
    );
    timeout(Duration::from_secs(10), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    task.abort();
    let _ = task.await;
    terminal(
        &drain(&mut receiver).await,
        TurnCompletionStatus::Interrupted,
    );
    timeout(Duration::from_secs(10), process.reaped.cancelled())
        .await
        .unwrap();
    fixture.stop(&session).await;
    let fixture = Fixture::new("normal");
    let session = fixture.start(None).await.unwrap();
    let reaped = fixture.engine.process(&session).unwrap().reaped.clone();
    drop(fixture);
    timeout(Duration::from_secs(10), reaped.cancelled())
        .await
        .unwrap();
}

#[tokio::test]
async fn acp_closed_event_consumer_fails_and_reaps() {
    let fixture = Fixture::new("wait");
    let session = fixture.start(None).await.unwrap();
    let process = fixture.engine.process(&session).unwrap();
    let (receiver, task) = begin(
        fixture.engine.clone(),
        session.clone(),
        CancellationToken::new(),
    );
    drop(receiver);
    timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(process.reaped.is_cancelled());
    fixture.stop(&session).await;
}

#[test]
fn acp_hermes_recorded_models_and_permission_diff() {
    let frames: Vec<Value> = include_str!("../../tests/fixtures/acp/hermes-text.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let response = frames
        .iter()
        .find(|frame| frame["result"]["models"].is_object())
        .unwrap();
    let models = super::super::hermes::session_models(&response["result"])
        .unwrap()
        .unwrap();
    assert!(models
        .available_models
        .iter()
        .any(|model| model.model_id == models.current_model_id));
    let frames: Vec<Value> = include_str!("../../tests/fixtures/acp/hermes-permission.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let request = frames
        .iter()
        .find(|frame| frame["method"] == "session/request_permission")
        .unwrap();
    let permission: acp::RequestPermissionRequest =
        serde_json::from_value(request["params"].clone()).unwrap();
    let diff = content_diff(permission.tool_call.fields.content.as_ref().unwrap())
        .unwrap()
        .unwrap();
    assert!(diff.contains("+Fixture edit."));
    assert!(diff.contains("sample.txt"));
}

#[cfg(unix)]
#[tokio::test]
async fn acp_hermes_profile_selected_model_and_text_turn() {
    use std::os::unix::fs::PermissionsExt;
    let python = which::which("python3").unwrap();
    let directory = std::env::temp_dir().join(format!("panes-hermes-test-{}", Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let executable = directory.join("hermes");
    let log = directory.join("requests.jsonl");
    let quote = |value: &str| format!("'{}'", value.replace('\'', "'\\''"));
    std::fs::write(
        &executable,
        format!(
            "#!/bin/sh\nexec {} {} hermes {}\n",
            quote(&python.to_string_lossy()),
            quote(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/acp/fake_server.py"
            )),
            quote(&log.to_string_lossy())
        ),
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = crate::config::app_config::AppConfig {
        chat_providers: vec![crate::config::app_config::ChatProviderInstanceConfig {
            id: "hermes_work".into(),
            kind: "hermes".into(),
            display_name: "Hermes Work".into(),
            binary_path: Some(executable.to_string_lossy().into_owned()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let manager = super::super::EngineManager::from_config(&config);
    let handle = manager.handle("hermes_work").await.unwrap();
    assert_eq!(handle.kind(), "hermes");
    let engine = match handle {
        super::super::EngineHandle::Acp(engine) => engine,
        _ => panic!("Hermes ACP handle"),
    };
    let thread = engine
        .start_thread(
            ThreadScope::Repo {
                repo_path: directory.to_string_lossy().into_owned(),
            },
            None,
            "default",
            sandbox(),
        )
        .await
        .unwrap();
    let selected = engine
        .models()
        .into_iter()
        .find(|model| model.id.starts_with("opencode-free:"))
        .unwrap();
    engine
        .start_thread(
            ThreadScope::Repo {
                repo_path: directory.to_string_lossy().into_owned(),
            },
            Some(&thread.engine_thread_id),
            &selected.id,
            sandbox(),
        )
        .await
        .unwrap();
    let (sender, mut receiver) = mpsc::channel(64);
    let runner = engine.clone();
    let session = thread.engine_thread_id.clone();
    let task = tokio::spawn(async move {
        runner
            .send_message(
                &session,
                TurnInput {
                    input_items: vec![super::super::TurnInputItem::Text {
                        text: "fixture".into(),
                    }],
                    ..input()
                },
                sender,
                CancellationToken::new(),
            )
            .await
    });
    let mut events = vec![];
    timeout(Duration::from_secs(15), async {
        while let Some(event) = receiver.recv().await {
            if let EngineEvent::ApprovalRequested { approval_id, .. } = &event {
                engine
                    .respond_to_approval(
                        approval_id,
                        json!({"optionId":"original/allow-once"}),
                        None,
                    )
                    .await
                    .unwrap();
            }
            events.push(event);
        }
    })
    .await
    .unwrap();
    task.await.unwrap().unwrap();
    terminal(&events, TurnCompletionStatus::Completed);
    let text: String = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::TextDelta { content } => Some(content.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "hello world");
    engine
        .archive_thread(&thread.engine_thread_id)
        .await
        .unwrap();
    let requests: Vec<Value> = std::fs::read_to_string(log)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        requests
            .iter()
            .filter(|frame| frame["method"] == "session/new")
            .count(),
        1
    );
    assert!(requests
        .iter()
        .any(|frame| frame["method"] == "session/set_model"
            && frame["params"]["modelId"] == selected.id));
    assert!(requests
        .iter()
        .any(|frame| frame["method"] == "session/prompt"
            && frame["params"]["prompt"][0]["text"] == "fixture"));
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
#[ignore = "requires installed Hermes and a configured model endpoint"]
async fn acp_hermes_real_profile_smoke() {
    let executable = std::env::var_os("PANES_HERMES_SMOKE_COMMAND").expect("Hermes executable");
    let directory = std::env::temp_dir().join(format!("panes-hermes-live-{}", Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let engine = Arc::new(AcpEngine::hermes(
        "hermes",
        "Hermes",
        super::super::EngineInstanceSettings {
            binary_path: Some(executable.into()),
            ..Default::default()
        },
    ));
    let health = engine.health_report().await;
    assert!(health.available, "{:?}", health.details);
    let thread = engine
        .start_thread(
            ThreadScope::Repo {
                repo_path: directory.to_string_lossy().into_owned(),
            },
            None,
            "default",
            sandbox(),
        )
        .await
        .unwrap();
    let session = thread.engine_thread_id.clone();
    let runner = engine.clone();
    let (sender, mut receiver) = mpsc::channel(64);
    let task = tokio::spawn(async move {
        runner
            .send_message(
                &session,
                TurnInput {
                    message: "Reply with Hermes ACP fixture ok. Do not use tools.".into(),
                    input_items: vec![],
                    ..input()
                },
                sender,
                CancellationToken::new(),
            )
            .await
    });
    let events = timeout(Duration::from_secs(90), async {
        let mut events = vec![];
        while let Some(event) = receiver.recv().await {
            events.push(event);
        }
        events
    })
    .await
    .unwrap();
    task.await.unwrap().unwrap();
    engine
        .archive_thread(&thread.engine_thread_id)
        .await
        .unwrap();
    std::fs::remove_dir_all(directory).unwrap();
    terminal(&events, TurnCompletionStatus::Completed);
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::TextDelta { content } if !content.is_empty())));
}

#[cfg(unix)]
#[tokio::test]
async fn acp_hermes_configuration_change_reaps_initializing_sessions() {
    use std::os::unix::fs::PermissionsExt;
    for resume in [None, Some("previous-session")] {
        let python = which::which("python3").unwrap();
        let directory = std::env::temp_dir().join(format!("panes-hermes-race-{}", Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let executable = directory.join("hermes");
        let log = directory.join("requests.jsonl");
        let quote = |value: &str| format!("'{}'", value.replace('\'', "'\\''"));
        std::fs::write(
            &executable,
            format!(
                "#!/bin/sh\nexec {} {} slow_init {}\n",
                quote(&python.to_string_lossy()),
                quote(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/acp/fake_server.py"
                )),
                quote(&log.to_string_lossy())
            ),
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let settings = super::super::EngineInstanceSettings {
            binary_path: Some(executable),
            ..Default::default()
        };
        let engine = Arc::new(AcpEngine::hermes("hermes", "Hermes", settings.clone()));
        let runner = engine.clone();
        let cwd = directory.clone();
        let task = tokio::spawn(async move {
            runner
                .start_thread(
                    ThreadScope::Repo {
                        repo_path: cwd.to_string_lossy().into_owned(),
                    },
                    resume,
                    "default",
                    sandbox(),
                )
                .await
        });
        timeout(Duration::from_secs(5), async {
            while !log.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut replacement = settings;
        replacement.env.insert("NEW_ACCOUNT".into(), "1".into());
        engine.update_instance_settings(replacement).await;
        let error = timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            error.to_string().contains("configuration changed"),
            "{error}"
        );
        assert!(engine.sessions.lock().unwrap().is_empty());
        assert!(engine.models().iter().all(|model| model.id == "default"));
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
#[ignore = "requires installed Antigravity and a configured model endpoint"]
async fn acp_agy_real_profile_smoke() {
    let directory = std::env::temp_dir().join(format!("panes-agy-live-{}", Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let engine = Arc::new(AcpEngine::agy(
        "agy",
        "Antigravity",
        super::super::EngineInstanceSettings::default(),
    ));
    let health = engine.health_report().await;
    assert!(health.available, "{:?}", health.details);
    let thread = engine
        .start_thread(
            ThreadScope::Repo {
                repo_path: directory.to_string_lossy().into_owned(),
            },
            None,
            super::super::agy::DEFAULT_MODEL,
            sandbox(),
        )
        .await
        .unwrap();
    let session = thread.engine_thread_id.clone();
    let runner = engine.clone();
    let (sender, mut receiver) = mpsc::channel(64);
    let task = tokio::spawn(async move {
        runner
            .send_message(
                &session,
                TurnInput {
                    message: "Reply with Antigravity ACP fixture ok. Do not use tools.".into(),
                    input_items: vec![],
                    ..input()
                },
                sender,
                CancellationToken::new(),
            )
            .await
    });
    let events = timeout(Duration::from_secs(90), async {
        let mut events = vec![];
        while let Some(event) = receiver.recv().await {
            events.push(event);
        }
        events
    })
    .await
    .unwrap();
    task.await.unwrap().unwrap();
    engine
        .archive_thread(&thread.engine_thread_id)
        .await
        .unwrap();
    std::fs::remove_dir_all(directory).unwrap();
    terminal(&events, TurnCompletionStatus::Completed);
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::TextDelta { content } if !content.is_empty())));
}

#[cfg(unix)]
#[tokio::test]
async fn acp_agy_profile_selected_model_and_text_turn() {
    use std::os::unix::fs::PermissionsExt;
    let python = which::which("python3").unwrap();
    let directory = std::env::temp_dir().join(format!("panes-agy-test-{}", Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let executable = directory.join("agy");
    let log = directory.join("requests.jsonl");
    let quote = |value: &str| format!("'{}'", value.replace('\'', "'\\''"));
    std::fs::write(
        &executable,
        format!(
            "#!/bin/sh\nexec {} {} agy {}\n",
            quote(&python.to_string_lossy()),
            quote(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/acp/fake_server.py"
            )),
            quote(&log.to_string_lossy())
        ),
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = crate::config::app_config::AppConfig {
        chat_providers: vec![crate::config::app_config::ChatProviderInstanceConfig {
            id: "agy_work".into(),
            kind: "agy".into(),
            display_name: "Antigravity Work".into(),
            binary_path: Some(executable.to_string_lossy().into_owned()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let manager = super::super::EngineManager::from_config(&config);
    let handle = manager.handle("agy_work").await.unwrap();
    assert_eq!(handle.kind(), "agy");
    let engine = match handle {
        super::super::EngineHandle::Acp(engine) => engine,
        _ => panic!("Antigravity ACP handle"),
    };
    let thread = engine
        .start_thread(
            ThreadScope::Repo {
                repo_path: directory.to_string_lossy().into_owned(),
            },
            None,
            super::super::agy::DEFAULT_MODEL,
            sandbox(),
        )
        .await
        .unwrap();
    let selected = engine
        .models()
        .into_iter()
        .find(|model| model.id == "claude-sonnet-4-6")
        .unwrap();
    engine
        .start_thread(
            ThreadScope::Repo {
                repo_path: directory.to_string_lossy().into_owned(),
            },
            Some(&thread.engine_thread_id),
            &selected.id,
            sandbox(),
        )
        .await
        .unwrap();
    let (sender, mut receiver) = mpsc::channel(64);
    let runner = engine.clone();
    let session = thread.engine_thread_id.clone();
    let task = tokio::spawn(async move {
        runner
            .send_message(
                &session,
                TurnInput {
                    input_items: vec![super::super::TurnInputItem::Text {
                        text: "fixture".into(),
                    }],
                    ..input()
                },
                sender,
                CancellationToken::new(),
            )
            .await
    });
    let mut events = vec![];
    timeout(Duration::from_secs(15), async {
        while let Some(event) = receiver.recv().await {
            if let EngineEvent::ApprovalRequested { approval_id, .. } = &event {
                engine
                    .respond_to_approval(
                        approval_id,
                        json!({"optionId":"original/allow-once"}),
                        None,
                    )
                    .await
                    .unwrap();
            }
            events.push(event);
        }
    })
    .await
    .unwrap();
    task.await.unwrap().unwrap();
    terminal(&events, TurnCompletionStatus::Completed);
    let text: String = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::TextDelta { content } => Some(content.as_str()),
            _ => None,
        })
        .collect();
    assert!(text.contains("sample.txt"));
    engine
        .archive_thread(&thread.engine_thread_id)
        .await
        .unwrap();
    let requests: Vec<Value> = std::fs::read_to_string(log)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        requests
            .iter()
            .filter(|frame| frame["method"] == "session/new")
            .count(),
        1
    );
    assert!(requests
        .iter()
        .any(|frame| frame["method"] == "session/set_config_option"
            && frame["params"]["value"] == selected.id));
    assert!(requests
        .iter()
        .any(|frame| frame["method"] == "session/prompt"
            && frame["params"]["prompt"][0]["text"] == "fixture"));
    std::fs::remove_dir_all(directory).unwrap();
}
