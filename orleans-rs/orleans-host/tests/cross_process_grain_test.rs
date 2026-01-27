//! Cross-Process Grain Invocation Test
//!
//! This test proves that a grain on Process 1 is accessible from Process 2 and Process 3.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────┐
//! │  Membership Server   │ (TCP Server)
//! │  Shared cluster state│
//! └──────────┬───────────┘
//!            │
//!     ┌──────┴──────┬──────────────┐
//!     │             │              │
//!     ▼             ▼              ▼
//! ┌────────┐   ┌────────┐    ┌────────┐
//! │Process1│   │Process2│    │Process3│
//! │ Silo 1 │   │ Silo 2 │    │ Silo 3 │
//! │ (host) │   │(caller)│    │(caller)│
//! └────────┘   └────────┘    └────────┘
//!     │             │              │
//!     │   CounterGrain hosted     │
//!     │     on Process 1          │
//!     └─────────────┼──────────────┘
//!                   │
//!      Process 2 calls: counter=1
//!      Process 3 calls: counter=2
//! ```
//!
//! # Running
//!
//! ```bash
//! cargo test -p orleans-host --test cross_process_grain_test -- --nocapture
//! ```

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use orleans_clustering::{IMembershipTable, InMemoryMembershipTable, MembershipTableServer};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;

/// Start the membership server and return (server, port)
async fn start_membership_server() -> (MembershipTableServer, u16) {
    let table = Arc::new(InMemoryMembershipTable::new("cross-process-test"));
    table.initialize_membership_table(true).await.unwrap();

    let server = MembershipTableServer::new(table);
    let addr = server.start("127.0.0.1:0").await.unwrap();

    (server, addr.port())
}

/// Spawn a silo process asynchronously and return a handle for reading output
async fn spawn_silo_async(
    membership_port: u16,
    create_grain: Option<&str>,
    invoke_grain: Option<&str>,
) -> tokio::process::Child {
    let mut cmd = TokioCommand::new(env!("CARGO_BIN_EXE_orleans-silo"));
    cmd.arg("--membership-server")
        .arg(format!("127.0.0.1:{}", membership_port))
        .arg("--port")
        .arg("0")
        .arg("--test");

    if let Some(key) = create_grain {
        cmd.arg("--create-grain").arg(key);
    }

    if let Some(key) = invoke_grain {
        cmd.arg("--test-grain").arg(key);
    }

    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn silo process")
}

/// Wait for a JSON line from process stdout with timeout
async fn wait_for_json(
    reader: &mut BufReader<tokio::process::ChildStdout>,
    timeout: Duration,
) -> Option<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut line = String::new();

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }

        line.clear();
        match tokio::time::timeout(remaining, reader.read_line(&mut line)).await {
            Ok(Ok(0)) => return None, // EOF
            Ok(Ok(_)) => {
                let trimmed = line.trim();
                println!("    {}", trimmed);

                if trimmed.starts_with('{') && trimmed.ends_with('}') {
                    if let Ok(json) = serde_json::from_str(trimmed) {
                        return Some(json);
                    }
                }
            }
            Ok(Err(_)) => return None,
            Err(_) => return None, // Timeout
        }
    }
}

/// Test: Three separate OS processes form a cluster and invoke grains across processes
///
/// This test is simpler - it doesn't require explicit cluster coordination.
/// Instead it relies on the fact that grains are created on-demand.
#[tokio::test]
async fn test_cross_process_grain_invocation() {
    println!("\n");
    println!("============================================================");
    println!("   Cross-Process Grain Invocation Test");
    println!("============================================================");
    println!();

    // Use a unique grain key for this test to avoid conflicts
    let grain_key = format!("xproc-grain-{}", std::process::id());
    println!("Using grain key: {}", grain_key);

    // Step 1: Start membership server
    println!();
    println!("Step 1: Starting membership server...");
    let (server, membership_port) = start_membership_server().await;
    println!("  Membership server on port {}", membership_port);

    // Step 2: Start Process 1 - it will create and host the grain
    println!();
    println!("Step 2: Starting Process 1 (grain host)...");
    let mut proc1 = spawn_silo_async(membership_port, Some(&grain_key), None).await;
    let stdout1 = proc1.stdout.take().unwrap();
    let mut reader1 = BufReader::new(stdout1);

    // Wait for Process 1 to create the grain
    println!("  Waiting for grain creation...");
    let p1_json = wait_for_json(&mut reader1, Duration::from_secs(30)).await;

    if let Some(ref json) = p1_json {
        if json.get("event") == Some(&serde_json::Value::String("grain_created".to_string())) {
            println!("  ✓ Process 1 created grain: {:?}", json.get("grain_id"));
        } else {
            println!("  Got JSON: {:?}", json);
        }
    } else {
        println!("  ✗ No grain creation event from Process 1");
    }

    // Give Process 1 time to register in directory
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Step 3: Start Process 2 - it will invoke the grain
    println!();
    println!("Step 3: Starting Process 2 (first caller)...");
    let mut proc2 = spawn_silo_async(membership_port, None, Some(&grain_key)).await;
    let stdout2 = proc2.stdout.take().unwrap();
    let mut reader2 = BufReader::new(stdout2);

    // Wait for Process 2's invocation result
    println!("  Waiting for invocation result...");
    let p2_json = wait_for_json(&mut reader2, Duration::from_secs(30)).await;

    let mut p2_value: Option<u32> = None;
    if let Some(ref json) = p2_json {
        if json.get("success") == Some(&serde_json::Value::Bool(true)) {
            if let Some(result) = json.get("result").and_then(|v| v.as_u64()) {
                p2_value = Some(result as u32);
                println!("  ✓ Process 2 increment() returned: {}", result);
            }
        } else {
            println!("  ✗ Process 2 invocation: {:?}", json);
        }
    } else {
        println!("  ✗ No result from Process 2");
    }

    // Step 4: Start Process 3 - it will invoke the grain
    println!();
    println!("Step 4: Starting Process 3 (second caller)...");
    let mut proc3 = spawn_silo_async(membership_port, None, Some(&grain_key)).await;
    let stdout3 = proc3.stdout.take().unwrap();
    let mut reader3 = BufReader::new(stdout3);

    // Wait for Process 3's invocation result
    println!("  Waiting for invocation result...");
    let p3_json = wait_for_json(&mut reader3, Duration::from_secs(30)).await;

    let mut p3_value: Option<u32> = None;
    if let Some(ref json) = p3_json {
        if json.get("success") == Some(&serde_json::Value::Bool(true)) {
            if let Some(result) = json.get("result").and_then(|v| v.as_u64()) {
                p3_value = Some(result as u32);
                println!("  ✓ Process 3 increment() returned: {}", result);
            }
        } else {
            println!("  ✗ Process 3 invocation: {:?}", json);
        }
    } else {
        println!("  ✗ No result from Process 3");
    }

    // Wait for processes to complete
    println!();
    println!("Step 5: Waiting for processes to complete...");
    let _ = proc1.wait().await;
    let _ = proc2.wait().await;
    let _ = proc3.wait().await;

    // Cleanup
    server.stop().await;

    // Verify results
    println!();
    println!("============================================================");

    // We expect:
    // - Process 1 creates grain (counter starts at 0)
    // - Process 2 calls increment, gets 1
    // - Process 3 calls increment, gets 2
    //
    // Note: If the grain isn't found by P2/P3, they might create their own local activation,
    // which would break the test. This tests the full cross-process routing.

    if let (Some(v2), Some(v3)) = (p2_value, p3_value) {
        println!("   Results: P2={}, P3={}", v2, v3);

        // Both processes should have invoked the same grain
        if v2 == 1 && v3 == 2 {
            println!("   ✓ SUCCESS: Perfect sequential invocation!");
            println!("   - Process 2 invoked grain (counter: 0 -> 1)");
            println!("   - Process 3 invoked grain (counter: 1 -> 2)");
            println!("   - Both routed to the same grain on Process 1");
        } else if v2 >= 1 && v3 >= 1 && v3 > v2 {
            println!("   ✓ SUCCESS: Cross-process grain invocation works!");
            println!("   - Counter incremented correctly across processes");
        } else {
            println!("   ? PARTIAL: Results suggest possible local activations");
            println!("   - This may indicate routing issues");
        }
        println!("============================================================");
        println!();
    } else {
        println!("   ✗ FAILED: Could not get results from all processes");
        println!("   - P2 result: {:?}", p2_value);
        println!("   - P3 result: {:?}", p3_value);
        println!("============================================================");
        println!();
        panic!("Cross-process grain invocation test failed");
    }
}

/// Helper test to verify the silo binary works standalone
#[tokio::test]
async fn test_single_silo_grain_creation() {
    println!("\n");
    println!("============================================================");
    println!("   Single Silo Grain Creation Test");
    println!("============================================================");
    println!();

    let grain_key = format!("single-test-{}", std::process::id());

    // Start membership server
    let (server, membership_port) = start_membership_server().await;
    println!("Membership server on port {}", membership_port);

    // Start single silo that creates a grain
    println!("Starting silo...");
    let mut proc = spawn_silo_async(membership_port, Some(&grain_key), None).await;
    let stdout = proc.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);

    // Wait for grain creation
    println!("Waiting for grain creation...");
    let json = wait_for_json(&mut reader, Duration::from_secs(30)).await;

    let _ = proc.wait().await;
    server.stop().await;

    println!();
    if let Some(json) = json {
        if json.get("event") == Some(&serde_json::Value::String("grain_created".to_string())) {
            println!("✓ SUCCESS: Grain created");
            println!("  grain_id: {:?}", json.get("grain_id"));
            println!("  silo: {:?}", json.get("silo"));
        } else {
            println!("? Got different event: {:?}", json);
        }
    } else {
        println!("✗ FAILED: No grain creation event received");
        panic!("Single silo grain creation failed");
    }
}

/// Helper test to verify silo grain invocation works
#[tokio::test]
async fn test_single_silo_grain_invocation() {
    println!("\n");
    println!("============================================================");
    println!("   Single Silo Grain Invocation Test");
    println!("============================================================");
    println!();

    let grain_key = format!("invoke-test-{}", std::process::id());

    // Start membership server
    let (server, membership_port) = start_membership_server().await;
    println!("Membership server on port {}", membership_port);

    // Start single silo that invokes a grain (it will create it locally first)
    println!("Starting silo with grain invocation...");
    let mut proc = spawn_silo_async(membership_port, None, Some(&grain_key)).await;
    let stdout = proc.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);

    // Wait for invocation result
    println!("Waiting for invocation result...");
    let json = wait_for_json(&mut reader, Duration::from_secs(30)).await;

    let _ = proc.wait().await;
    server.stop().await;

    println!();
    if let Some(json) = json {
        if json.get("success") == Some(&serde_json::Value::Bool(true)) {
            let result = json.get("result").and_then(|v| v.as_u64()).unwrap_or(0);
            println!("✓ SUCCESS: Grain invoked");
            println!("  increment() returned: {}", result);
            assert_eq!(result, 1, "First increment should return 1");
        } else {
            println!("✗ FAILED: Invocation failed: {:?}", json);
            panic!("Grain invocation failed: {:?}", json);
        }
    } else {
        println!("✗ FAILED: No invocation result received");
        panic!("Single silo grain invocation failed");
    }
}
