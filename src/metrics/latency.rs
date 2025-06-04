use tokio::net::{TcpStream, TcpListener};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::time::{Duration, timeout};
use std::net::IpAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use rand::Rng;

#[derive(Debug)]
pub enum LatencyError {
    MalformedTimestamp,
    MalformedMessage,
    SystemTimeError,
    NetworkError,
    SyncError,
    PortError,
    TimeoutError,
    CrashError,
    SocketError
}

// Measure latency
fn latency_msg() -> Result<Vec<u8>, LatencyError> {
    let header = "LATENCY!"; // 8 long

    let mut bytes_vec = header.as_bytes().to_vec();

    let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(timestamp) => timestamp,
        Err(_) => return Err(LatencyError::SystemTimeError)
    };
    let timestamp_bytes: [u8; 16] = timestamp.as_nanos().to_le_bytes();

    bytes_vec.extend_from_slice(&timestamp_bytes);

    return Ok(bytes_vec);
}

fn calculate_latency(original_message: Vec<u8>, response: [u8; 24]) -> Result<Duration, LatencyError> {
    // calculate time right away to avoid latency in processing
    let received_time = SystemTime::now();

    let header = String::from_utf8_lossy(&response[0..8]);
    if header == "RECEIVED" {
        match response[8..16].try_into() {
            Ok(timestamp_bytes) => {
                let nanoseconds = u64::from_le_bytes(timestamp_bytes);
                let original_bytes = &original_message[8..16];
                if timestamp_bytes == original_bytes {
                    let duration = Duration::from_nanos(nanoseconds);
                    let sent_time = UNIX_EPOCH + duration;
                    match received_time.duration_since(sent_time) {
                        Ok(rtt) => return Ok(rtt),
                        Err(_) => return Err(LatencyError::SystemTimeError)
                    }
                } else {
                    return Err(LatencyError::SyncError)
                }
            },
            Err(_) => return Err(LatencyError::MalformedTimestamp),
        }
        
    } else {
        return Err(LatencyError::MalformedMessage)
    }
}

fn calculate_rtt_metrics(rtts: Vec<Duration>) -> (f64, f64, f64) {
    if rtts.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    
    // Convert durations to milliseconds (as f64)
    let ms: Vec<f64> = rtts.iter().map(|d| d.as_nanos() as f64 / 1_000_000.0).collect();
    let count = ms.len();
    
    // 1. RTT Average
    let total: f64 = ms.iter().sum();
    let avg_rtt = total / count as f64;
    
    // 2. RTT Variance (sample variance)
    let variance = if count == 1 {
        0.0
    } else {
        let sum_squares: f64 = ms.iter().map(|&x| (x - avg_rtt).powi(2)).sum();
        sum_squares / (count as f64 - 1.0)
    };
    
    // 3. Jitter (standard deviation of inter-packet delays)
    let mut diffs = Vec::new();
    for i in 1..count {
        diffs.push(ms[i] - ms[i - 1]);
    }
    
    let jitter = if diffs.len() < 2 {
        0.0
    } else {
        let avg_diff = diffs.iter().sum::<f64>() / diffs.len() as f64;
        let sum_diff_squares: f64 = diffs.iter().map(|&x| (x - avg_diff).powi(2)).sum();
        (sum_diff_squares / (diffs.len() as f64 - 1.0)).sqrt()
    };
    
    (avg_rtt, variance, jitter)
}

// Send latency data over TCP
pub async fn send_latency(ip: IpAddr, port: u16) -> Result<(f64, f64, f64), LatencyError> {
    let mut stream = match TcpStream::connect(format!("{}:{}", ip, port)).await {
        Ok(stream) => stream,
        Err(_) => return Err(LatencyError::NetworkError)
    };
    let mut buffer = [0; 24];

    let mut rtts: Vec<Duration> = Vec::new();

    let max_ping_duration = Duration::from_secs(2);
    let max_total_duration = Duration::from_secs(5);

    let start_time = std::time::Instant::now();

    while start_time.elapsed() < max_total_duration {
        // Actual data transmission
        let data = latency_msg()?;
        _ = stream.write_all(&data).await;

        match timeout(max_ping_duration, stream.read(&mut buffer)).await {
            Ok(_) => {
                let rtt = calculate_latency(data, buffer)?;
                rtts.push(rtt);
            }
            Err(_) => {

            }
        }
    }

    // yeet the first element since it will have some of the connection establishing latency
    rtts.remove(0);
    
    // calculate RTT average, variance, jitter
    let (average_rtt, variance, jitter) = calculate_rtt_metrics(rtts);

    // println!("RTT Average: {:.2} ms | RTT Variance: {:.2} ms^2 | Jitter: {:.2} ms", average_rtt, variance, jitter);

    Ok((average_rtt, variance, jitter))
}

async fn receive_latency(port: u16, startup_tx: oneshot::Sender<()>) -> Result<(), LatencyError> {
    match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
        Ok(listener) => {
            // alert startup channel we've bound properly
            _ = startup_tx.send(());

            match listener.accept().await {
                Ok((mut stream, _)) => {
                    let mut buffer = [0; 24];
            
                    let max_ping_duration = Duration::from_secs(2);
                    let max_total_duration = Duration::from_secs(5);

                    let start_time = std::time::Instant::now();

                    while start_time.elapsed() < max_total_duration {
                        match timeout(max_ping_duration, stream.read(&mut buffer)).await {
                            Ok(_) => {
                                // Extract header (first 8 bytes)
                                let header = String::from_utf8_lossy(&buffer[0..8]);
                                if header == "LATENCY!" {
                                    let timestamp_bytes = &buffer[8..24];
                                    let mut response = "RECEIVED".as_bytes().to_vec();
                                    response.extend_from_slice(timestamp_bytes);
                                    // Extract timestamp (next 16 bytes)
                                    _ = stream.write_all(&response).await;
                                }
                            }
                            Err(_) => {
                                // Read error or timeout
                            }
                        }
                    }
                }
                Err(_) => {return Err(LatencyError::SocketError)}
            }
        }
        Err(_) => return Err(LatencyError::PortError)

    }

    // OK Even if we didn't get messages, not receiving issue
    return Ok(())
}

async fn select_port() -> Result<(JoinHandle<Result<(), LatencyError>>, u16), LatencyError> {
    // pick random port in safe range
    const MIN_PORT: u16 = 49152;
    const MAX_PORT: u16 = 65535;

    // Generate random port outside of async context to avoid Send issues
    let latency_port: u16 = {
        let mut rng = rand::rng();
        rng.random_range(MIN_PORT..=MAX_PORT)
    };

    // start the latency listener on this port
    let (latency_startup_tx, latency_startup_rx) = oneshot::channel();

    let latency_task = tokio::spawn(receive_latency(latency_port, latency_startup_tx));

    // timeout for maximum time to wait for latency task to start up
    let timeout_duration = Duration::from_secs(1);

    match timeout(timeout_duration, latency_startup_rx).await {
        Ok(Ok(())) => {
            return Ok((latency_task, latency_port));
        }
        Ok(Err(_)) => {
            return Err(LatencyError::PortError);
        }
        Err(_) => {
            return Err(LatencyError::TimeoutError);
        }
    }
}

pub async fn listener() -> Result<(JoinHandle<Result<(), LatencyError>>, u16), LatencyError> {
    // timeout for maximum time to wait for latency task to start up
    let timeout_duration = Duration::from_secs(5);

    loop {
        match timeout(timeout_duration, select_port()).await {
            Ok(Ok((latency_task, latency_port))) => {return Ok((latency_task, latency_port))}
            Ok(Err(latency_error)) => {return Err(latency_error)}
            Err(_) => {return Err(LatencyError::TimeoutError)}
        }
    }
}