//! Orleans Membership Server
//!
//! A standalone TCP server that hosts the membership table for multi-process clusters.
//! Run this first, then start silo processes that connect to it.
//!
//! # Usage
//!
//! ```bash
//! # Start the membership server
//! orleans-membership-server --port 5000 --cluster my-cluster
//!
//! # In other terminals, start silos
//! orleans-silo --membership-server 127.0.0.1:5000 --port 11111
//! orleans-silo --membership-server 127.0.0.1:5000 --port 22222
//! orleans-silo --membership-server 127.0.0.1:5000 --port 33333
//! ```

use std::sync::Arc;
use orleans_clustering::{IMembershipTable, InMemoryMembershipTable, MembershipTableServer};

/// Simple argument parsing for the membership server.
fn parse_args() -> (u16, String) {
    let args: Vec<String> = std::env::args().collect();
    let mut port = 5000u16;
    let mut cluster_id = "default-cluster".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                if i + 1 < args.len() {
                    port = args[i + 1].parse().unwrap_or(5000);
                    i += 1;
                }
            }
            "--cluster" | "-c" => {
                if i + 1 < args.len() {
                    cluster_id = args[i + 1].clone();
                    i += 1;
                }
            }
            "--help" | "-h" => {
                println!("Orleans Membership Server");
                println!();
                println!("USAGE:");
                println!("    orleans-membership-server [OPTIONS]");
                println!();
                println!("OPTIONS:");
                println!("    -p, --port <PORT>         Port to listen on (default: 5000)");
                println!("    -c, --cluster <NAME>      Cluster ID (default: default-cluster)");
                println!("    -h, --help                Show this help message");
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    (port, cluster_id)
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("orleans=info".parse().unwrap()))
        .init();

    let (port, cluster_id) = parse_args();

    println!("===========================================");
    println!("   Orleans Membership Server");
    println!("===========================================");
    println!();
    println!("  Cluster ID: {}", cluster_id);
    println!("  Port:       {}", port);
    println!();

    // Create the membership table
    let table = Arc::new(InMemoryMembershipTable::new(&cluster_id));

    // Initialize the table
    if let Err(e) = table.initialize_membership_table(true).await {
        eprintln!("Failed to initialize membership table: {}", e);
        std::process::exit(1);
    }

    // Create and start the server
    let server = MembershipTableServer::new(table);
    let addr = format!("0.0.0.0:{}", port);

    match server.start(&addr).await {
        Ok(actual_addr) => {
            println!("  Listening on: {}", actual_addr);
            println!();
            println!("  Silos can connect using:");
            println!("    orleans-silo --membership-server {}", actual_addr);
            println!();
            println!("  Press Ctrl+C to stop...");
            println!("===========================================");

            // Wait for shutdown signal
            tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");

            println!();
            println!("Shutting down...");
            server.stop().await;
            println!("Goodbye!");
        }
        Err(e) => {
            eprintln!("Failed to start membership server: {}", e);
            std::process::exit(1);
        }
    }
}
