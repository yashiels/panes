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
