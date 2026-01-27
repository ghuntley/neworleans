//! Orleans Silo Process
//!
//! A standalone silo that connects to a membership server and hosts grains.
//!
//! # Usage
//!
//! ```bash
//! # First start the membership server
//! orleans-membership-server --port 5000
//!
//! # Then start silos
//! orleans-silo --membership-server 127.0.0.1:5000 --port 11111
//! orleans-silo --membership-server 127.0.0.1:5000 --port 22222
//! orleans-silo --membership-server 127.0.0.1:5000 --port 33333
//! ```

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use orleans_clustering::{MembershipVersion, TcpMembershipTable};
use orleans_core::{GrainAddress, GrainId, GrainType, IdSpan};
use orleans_host::{
    GrainTypeData, IGrain, IGrainActivator, IGrainContext, IGrainMethodInvoker, RuntimeResult,
    SiloBuilder,
};
use orleans_messaging::GrainInterfaceType;

// ============================================================================
// CounterGrain - A simple grain for testing
// ============================================================================

/// A simple counter grain that demonstrates cross-process communication.
struct CounterGrain {
    counter: AtomicU32,
}

#[async_trait]
impl IGrain for CounterGrain {
    fn grain_type() -> GrainType {
        GrainType::create("CounterGrain")
    }
}

impl CounterGrain {
    fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
        }
    }

    fn increment(&self) -> u32 {
        self.counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn get_value(&self) -> u32 {
        self.counter.load(Ordering::SeqCst)
    }
}

/// Activator for CounterGrain.
struct CounterGrainActivator;

impl IGrainActivator for CounterGrainActivator {
    fn create(&self, _grain_id: &GrainId) -> Box<dyn std::any::Any + Send + Sync> {
        Box::new(CounterGrain::new())
    }

    fn grain_type(&self) -> GrainType {
        CounterGrain::grain_type()
    }
}

/// Invoker for CounterGrain.
struct CounterGrainInvoker;

impl CounterGrainInvoker {
    const INTERFACE_TYPE: &'static str = "ICounterGrain";
    const METHOD_IDS: [u32; 2] = [1, 2];
}

impl IGrainMethodInvoker for CounterGrainInvoker {
    fn interface_type(&self) -> &str {
        Self::INTERFACE_TYPE
    }

    fn method_ids(&self) -> &[u32] {
        &Self::METHOD_IDS
    }

    fn invoke<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        grain: &'life1 mut dyn std::any::Any,
        _context: &'life2 dyn IGrainContext,
        method_id: u32,
        _body: &'life3 [u8],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Vec<u8>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        let grain = grain.downcast_mut::<CounterGrain>().unwrap();

        let result = match method_id {
            1 => {
                // increment() -> u32
                let new_value = grain.increment();
                Ok(new_value.to_le_bytes().to_vec())
            }
            2 => {
                // get_value() -> u32
                let value = grain.get_value();
                Ok(value.to_le_bytes().to_vec())
            }
            _ => Err(orleans_runtime::RuntimeError::MethodNotFound {
                interface_type: "ICounterGrain".to_string(),
                method_id,
            }),
        };

        Box::pin(std::future::ready(result))
    }
}

fn create_counter_grain_type() -> Arc<GrainTypeData> {
    let activator = Arc::new(CounterGrainActivator);
    let invoker: Arc<dyn IGrainMethodInvoker> = Arc::new(CounterGrainInvoker);

    let grain_type_data = GrainTypeData::new(CounterGrain::grain_type(), activator)
        .with_invoker("ICounterGrain", invoker);

    Arc::new(grain_type_data)
}

// ============================================================================
// CLI Argument Parsing
// ============================================================================

struct CliArgs {
    port: u16,
    membership_server: String,
    test_mode: bool,
    test_grain_key: Option<String>,
    /// Wait for N silos in the cluster before proceeding
    wait_for_cluster: Option<usize>,
    /// Create a grain locally (first call will activate it here)
    create_grain: Option<String>,
}

fn parse_args() -> CliArgs {
    let args: Vec<String> = std::env::args().collect();
    let mut port = 0u16; // 0 = auto-assign
    let mut membership_server = "127.0.0.1:5000".to_string();
    let mut test_mode = false;
    let mut test_grain_key = None;
    let mut wait_for_cluster = None;
    let mut create_grain = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                if i + 1 < args.len() {
                    port = args[i + 1].parse().unwrap_or(0);
                    i += 1;
                }
            }
            "--membership-server" | "-m" => {
                if i + 1 < args.len() {
                    membership_server = args[i + 1].clone();
                    i += 1;
                }
            }
            "--test" => {
                test_mode = true;
            }
            "--test-grain" => {
                if i + 1 < args.len() {
                    test_grain_key = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--wait-for-cluster" => {
                if i + 1 < args.len() {
                    wait_for_cluster = args[i + 1].parse().ok();
                    i += 1;
                }
            }
            "--create-grain" => {
                if i + 1 < args.len() {
                    create_grain = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--help" | "-h" => {
                println!("Orleans Silo Process");
                println!();
                println!("USAGE:");
                println!("    orleans-silo [OPTIONS]");
                println!();
                println!("OPTIONS:");
                println!("    -p, --port <PORT>                   Port to listen on (default: auto)");
                println!("    -m, --membership-server <ADDR>      Membership server address (default: 127.0.0.1:5000)");
                println!("    --test                              Run in test mode (auto-shutdown)");
                println!("    --test-grain <KEY>                  Invoke test grain with given key");
                println!("    --wait-for-cluster <N>              Wait for N silos in cluster before proceeding");
                println!("    --create-grain <KEY>                Create a grain locally with given key");
                println!("    -h, --help                          Show this help message");
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    CliArgs {
        port,
        membership_server,
        test_mode,
        test_grain_key,
        wait_for_cluster,
        create_grain,
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("orleans=info".parse().unwrap()),
        )
        .init();

    let args = parse_args();

    println!("===========================================");
    println!("   Orleans Silo");
    println!("===========================================");
    println!();
    println!("  Membership Server: {}", args.membership_server);
    println!("  Port:              {}", if args.port == 0 { "auto".to_string() } else { args.port.to_string() });
    println!();

    // Connect to the membership server
    println!("Connecting to membership server...");
    let membership_table = match TcpMembershipTable::connect(&args.membership_server).await {
        Ok(table) => Arc::new(table),
        Err(e) => {
            eprintln!("Failed to connect to membership server: {}", e);
            eprintln!();
            eprintln!("Make sure the membership server is running:");
            eprintln!("  orleans-membership-server --port 5000");
            std::process::exit(1);
        }
    };
    println!("  Connected!");

    // Create grain type
    let grain_type = create_counter_grain_type();

    // Build the silo
    let listen_addr: std::net::SocketAddr = format!("127.0.0.1:{}", args.port)
        .parse()
        .expect("Invalid listen address");

    let mut silo = match SiloBuilder::test()
        .listen_address(listen_addr)
        .with_membership_table(membership_table)
        .register_grain_type(grain_type)
        .build()
        .await
    {
        Ok(silo) => silo,
        Err(e) => {
            eprintln!("Failed to build silo: {}", e);
            std::process::exit(1);
        }
    };

    // Start the silo
    println!("Starting silo...");
    if let Err(e) = silo.start().await {
        eprintln!("Failed to start silo: {}", e);
        std::process::exit(1);
    }

    let silo_addr = silo.address();
    println!();
    println!("  Silo Address: {}", silo_addr);
    println!("  State:        {:?}", silo.state());
    println!();

    // Wait for cluster formation if requested
    if let Some(required_silos) = args.wait_for_cluster {
        println!("Waiting for {} silos in cluster...", required_silos);
        let mm = silo.membership_manager().expect("Membership manager should be available");
        let max_wait = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();

        loop {
            // Refresh membership to get latest state
            if let Err(e) = mm.refresh().await {
                eprintln!("Warning: Failed to refresh membership: {}", e);
            }

            let snapshot = mm.get_snapshot();
            let active_count = snapshot.get_active_silos().len();

            if active_count >= required_silos {
                println!("  Cluster ready: {} silos active", active_count);

                // Update directory ring with all active silos
                let dir = silo.directory().expect("Directory should be available");
                for silo_addr in snapshot.get_active_silos() {
                    if !dir.ring().contains_silo(silo_addr) {
                        dir.ring().add_silo(silo_addr.clone());
                    }
                }
                println!("  Directory ring updated: {} silos", dir.ring().silo_count());
                break;
            }

            if start.elapsed() > max_wait {
                eprintln!("Timeout waiting for cluster formation (got {} of {} silos)",
                    active_count, required_silos);
                std::process::exit(1);
            }

            println!("  Currently {} silos, waiting...", active_count);
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    // Create a grain locally if requested
    if let Some(grain_key) = &args.create_grain {
        println!("=== Creating grain locally: {} ===", grain_key);

        let grain_id = GrainId::new(CounterGrain::grain_type(), IdSpan::from_str(grain_key));
        let catalog = silo.catalog().expect("Catalog should be available");
        let dir = silo.directory().expect("Directory should be available");

        // Create the activation locally
        match catalog.get_or_create_activation(&grain_id) {
            Ok(handle) => {
                println!("  Created activation: {}", handle.activation_id());

                // Register in directory so other silos can find it
                let grain_address = GrainAddress::complete(
                    grain_id.clone(),
                    handle.activation_id().clone(),
                    silo.address().clone(),
                );

                if let Err(e) = dir.register(
                    MembershipVersion::default(),
                    grain_address.clone(),
                    None,
                ).await {
                    eprintln!("  Warning: Failed to register in directory: {}", e);
                } else {
                    println!("  Registered in directory");
                }

                // Output JSON for test coordination
                println!("{{\"event\":\"grain_created\",\"grain_id\":\"{}\",\"silo\":\"{}\",\"activation_id\":\"{}\"}}",
                    grain_id, silo.address(), handle.activation_id());
            }
            Err(e) => {
                eprintln!("  Failed to create grain: {:?}", e);
                println!("{{\"event\":\"grain_create_failed\",\"error\":\"{:?}\"}}", e);
            }
        }
    }

    // If test mode with grain key, run the test
    if let Some(grain_key) = args.test_grain_key {
        println!("=== Test Mode: Invoking grain {} ===", grain_key);

        let grain_id = GrainId::new(CounterGrain::grain_type(), IdSpan::from_str(&grain_key));

        // Get grain factory
        let factory = silo.grain_factory().expect("Grain factory should be available");

        // Create grain reference
        let grain_ref = factory.get_grain_reference_by_id(
            grain_id.clone(),
            GrainInterfaceType::create("ICounterGrain"),
        );

        // Invoke increment
        println!("Calling increment() on grain {}...", grain_key);
        match grain_ref
            .invoke(1, Bytes::new(), Some(std::time::Duration::from_secs(10)))
            .await
        {
            Ok(response) => {
                let value = u32::from_le_bytes(response[..4].try_into().unwrap());
                println!("  Result: {}", value);
                // Output JSON for test parsing
                println!("{{\"success\":true,\"method\":\"increment\",\"result\":{}}}", value);
            }
            Err(e) => {
                eprintln!("  Error: {:?}", e);
                println!("{{\"success\":false,\"error\":\"{:?}\"}}", e);
            }
        }
    }

    if args.test_mode {
        // In test mode, wait a bit then shutdown
        println!("Test mode: waiting 2 seconds then shutting down...");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    } else {
        // Normal mode: wait for Ctrl+C
        println!("  Press Ctrl+C to stop...");
        println!("===========================================");

        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");
        println!();
    }

    println!("Shutting down silo...");
    if let Err(e) = silo.stop().await {
        eprintln!("Error during shutdown: {}", e);
    }
    println!("Goodbye!");
}
