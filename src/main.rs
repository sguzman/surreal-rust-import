// src/main.rs - Rust SurrealDB Parallel Importer

use std::fs::File;
use std::io::{self, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, info, warn};
use serde_json::Value; // Using dynamic Value for schemaless approach
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root; // Using Root for simplicity if auth needed later
use surrealdb::Surreal;
use thiserror::Error;
use tokio::sync::mpsc; // Async channel for communication
use tokio::task::JoinHandle;
use tokio::time::sleep;

// --- Configuration ---
// TODO: Replace hardcoded values with command-line arguments (e.g., using clap)
const FILE_PATH: &str = "arxiv_array.json";
const DATABASE_URL: &str = "ws://localhost:8000";
const NAMESPACE: &str = "kaggle_data";
const DATABASE: &str = "arxiv";
const TABLE_NAME: &str = "arxiv_data";
const NUM_WORKERS: usize = 8; // Number of parallel insertion workers
const CHANNEL_BUFFER_SIZE: usize = 1000; // Max items buffered between parser and workers

// Optional: Credentials if authentication is needed
// const DB_USER: &str = "root";
// const DB_PASS: &str = "admin";

// --- Error Handling ---
#[derive(Error, Debug)]
enum ImportError {
    #[error("I/O Error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON Parsing Error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Database Error: {0}")]
    Database(#[from] surrealdb::Error),
    #[error("Channel Send Error: {0}")]
    ChannelSend(#[from] mpsc::error::SendError<Value>),
    #[error("Worker Task Failed: {0}")]
    WorkerTaskFailed(String),
}

// --- Main Application Logic ---
#[tokio::main]
async fn main() -> Result<(), ImportError> {
    // Initialize logging (e.g., `RUST_LOG=info cargo run`)
    env_logger::init();

    info!("Starting SurrealDB parallel importer...");
    info!("Configuration:");
    info!("  File Path: {}", FILE_PATH);
    info!("  Database URL: {}", DATABASE_URL);
    info!("  Namespace: {}", NAMESPACE);
    info!("  Database: {}", DATABASE);
    info!("  Table Name: {}", TABLE_NAME);
    info!("  Worker Tasks: {}", NUM_WORKERS);
    info!("  Channel Buffer: {}", CHANNEL_BUFFER_SIZE);

    // Shared atomic counters for statistics
    let processed_count = Arc::new(AtomicUsize::new(0));
    let inserted_count = Arc::new(AtomicUsize::new(0));
    let failed_count = Arc::new(AtomicUsize::new(0));

    // Create a bounded channel to send parsed records to workers
    let (tx, mut rx) = mpsc::channel::<Value>(CHANNEL_BUFFER_SIZE);

    // --- Spawn Parser Task ---
    let parser_processed_count = processed_count.clone();
    let parser_handle: JoinHandle<Result<(), ImportError>> = tokio::spawn(async move {
        info!("Parser task started. Reading from: {}", FILE_PATH);
        let file = File::open(FILE_PATH)?;
        // Use BufReader for potentially better performance
        let reader = BufReader::new(file);
        // Create a stream deserializer for the JSON array
        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<Value>();

        for result in stream {
            match result {
                Ok(record) => {
                    // Validate if it's an object before sending
                    if record.is_object() {
                        debug!("Parser: Sending record to workers.");
                        // Send the parsed record to the workers
                        // This will block if the channel buffer is full
                        tx.send(record).await?;
                        parser_processed_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        warn!(
                            "Parser: Skipping item - not a JSON object. Type: {:?}",
                            record.as_str().map(|_| "string").unwrap_or_else(|| record
                                .as_array()
                                .map(|_| "array")
                                .unwrap_or_else(|| record
                                    .as_bool()
                                    .map(|_| "boolean")
                                    .unwrap_or_else(|| record
                                        .as_number()
                                        .map(|_| "number")
                                        .unwrap_or("other"))))
                        );
                    }
                }
                Err(e) => {
                    // Log parsing error but try to continue if possible (depends on error type)
                    error!("Parser: JSON parsing error: {}. Attempting to continue.", e);
                    // For critical errors, might want to return Err here
                    // return Err(ImportError::Json(e));
                }
            }
        }
        info!("Parser task finished reading file.");
        // tx is automatically dropped here, closing the channel
        Ok(())
    });

    // --- Spawn Worker Tasks ---
    let mut worker_handles = Vec::with_capacity(NUM_WORKERS);
    for i in 0..NUM_WORKERS {
        let mut rx_clone = rx; // Each worker needs its own receiver handle - ERROR: mpsc::Receiver is not Clone
        // Correction: Use a single receiver and have workers compete, or use a broadcast channel,
        // or more simply, pass the receiver into an Arc<Mutex<Receiver>>.
        // Let's try the Arc<Mutex<Receiver>> approach for simplicity here, though it adds lock contention.
        // A better approach for high-throughput might be a dedicated work-stealing queue or multiple producers/consumers.

        // Re-thinking: The simplest for mpsc is to have ONE task receiving and then distributing/processing.
        // Or, create the receiver *outside* the loop and pass its Arc<Mutex<>> to each task.

        // Let's stick to the original plan but fix the receiver cloning.
        // We need to wrap the receiver in something shareable.
        // However, mpsc::Receiver cannot be easily shared and concurrently received from.
        // The typical pattern is one receiver task that then might distribute work,
        // OR use a different channel type like `crossbeam_channel` if sync needed,
        // OR use `tokio::sync::broadcast` if all workers need all messages (not applicable here).

        // Simplest async approach: Have ONE receiver task that pulls from the channel
        // and then potentially uses `tokio::spawn` for each insertion, managing concurrency.
        // Let's refactor to this model: One receiver task spawning insertion sub-tasks.

        // --- Refactored Worker Spawning (Simpler Concurrency Model) ---
        // Instead of NUM_WORKERS competing for the receiver, we'll have one receiver
        // that spawns insertion tasks, limiting concurrency with a semaphore.

        // Create a semaphore to limit concurrent database operations
        let semaphore = Arc::new(tokio::sync::Semaphore::new(NUM_WORKERS));

        let worker_inserted_count = inserted_count.clone();
        let worker_failed_count = failed_count.clone();

        let worker_handle: JoinHandle<Result<(), ImportError>> = tokio::spawn(async move {
             // Establish ONE database connection for this receiver/spawner task
             // Note: For high concurrency, a connection pool would be better.
            info!("Worker spawner task {} starting...", i);
            let db = Surreal::new::<Ws>(DATABASE_URL).await?;
            info!("Worker spawner task {}: DB connection established.", i);

            // --- Optional: Authentication ---
            // info!("Worker spawner task {}: Signing in...", i);
            // db.signin(Root {
            //     username: DB_USER,
            //     password: DB_PASS,
            // }).await?;
            // info!("Worker spawner task {}: Signed in.", i);
            // --- End Optional: Authentication ---

            db.use_ns(NAMESPACE).use_db(DATABASE).await?;
            info!("Worker spawner task {}: Namespace/DB selected.", i);

            let mut insertion_tasks = Vec::new();

            while let Some(record) = rx.recv().await { // Receive from the single shared receiver
                let db_clone = db.clone(); // Clone the DB client handle (cheap)
                let worker_inserted_count_clone = worker_inserted_count.clone();
                let worker_failed_count_clone = worker_failed_count.clone();
                let semaphore_clone = semaphore.clone();

                // Acquire a permit from the semaphore before spawning
                let permit = semaphore_clone.acquire_owned().await.expect("Semaphore closed unexpectedly");

                // Spawn a new task for each insertion
                let task_handle = tokio::spawn(async move {
                    debug!("Insertion task: Processing record...");
                    match db_clone.create(TABLE_NAME).content(record.clone()).await {
                        Ok(created_record) => {
                            // Check if SurrealDB returned a non-empty result (indicating success)
                            // Adjust this check based on the actual success criteria/return type of db.create
                            if !created_record.is_empty() {
                                 worker_inserted_count_clone.fetch_add(1, Ordering::Relaxed);
                                 debug!("Insertion task: Record inserted successfully.");
                            } else {
                                 error!("Insertion task: db.create command succeeded but returned empty result. Record snippet: {:.200}", record.to_string());
                                 worker_failed_count_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            error!("Insertion task: Database error: {}", e);
                            worker_failed_count_clone.fetch_add(1, Ordering::Relaxed);
                            // Optionally log the problematic record snippet
                            // error!("Problematic record snippet: {:.200}", record.to_string());
                        }
                    }
                    drop(permit); // Release the semaphore permit when the task is done
                });
                insertion_tasks.push(task_handle);
            }

            info!("Worker spawner task {}: Channel closed. Waiting for insertions...", i);
            // Wait for all spawned insertion tasks to complete
            futures::future::join_all(insertion_tasks).await;
            info!("Worker spawner task {}: All insertions finished.", i);
            Ok(())

        }); // End of single receiver/spawner task
        worker_handles.push(worker_handle); // Add the handle of the spawner task

        // Break after creating the single receiver/spawner task
        break; // Only need one receiver task in this refactored model
    }


    // --- Wait for Tasks and Report ---
    info!("Waiting for parser task to complete...");
    match parser_handle.await {
        Ok(Ok(_)) => info!("Parser task completed successfully."),
        Ok(Err(e)) => error!("Parser task failed: {}", e),
        Err(e) => error!("Parser task panicked or was cancelled: {}", e),
    }

    info!("Waiting for worker tasks to complete...");
    // Wait for the single spawner task (which waits for its sub-tasks)
    for (i, handle) in worker_handles.into_iter().enumerate() {
         match handle.await {
             Ok(Ok(_)) => info!("Worker spawner task {} completed successfully.", i),
             Ok(Err(e)) => error!("Worker spawner task {} failed: {}", e),
             Err(e) => error!("Worker spawner task {} panicked or was cancelled: {}", e),
         }
    }


    info!("--- Import Summary ---");
    let final_processed = processed_count.load(Ordering::SeqCst);
    let final_inserted = inserted_count.load(Ordering::SeqCst);
    let final_failed = failed_count.load(Ordering::SeqCst);
    info!("Records Processed (by parser): {}", final_processed);
    info!("Records Inserted Successfully: {}", final_inserted);
    info!("Records Failed to Insert: {}", final_failed);
    info!("----------------------");

    if final_failed > 0 || final_processed != final_inserted + final_failed {
         warn!("Import completed with errors or discrepancies.");
    } else {
         info!("Import completed successfully.");
    }


    Ok(())
}

