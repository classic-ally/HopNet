use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, timeout, sleep};
use tokio::sync::mpsc;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use rand::Rng;
use chrono::{DateTime, Utc};
use core::time;
use std::alloc::System;
use std::net::IpAddr;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

fn generate_random_u8_array(length: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..length).map(|_| rng.random_range(0..=255)).collect()
}

#[derive(Debug)]
enum LatencyError {
    MalformedTimestamp,
    MalformedMessage,
    SystemTimeError,
    NetworkError,
    SyncError
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
async fn send_latency(ip: IpAddr, port: u16) -> Result<(), LatencyError> {
    let mut stream = match TcpStream::connect(format!("{}:{}", ip, port)).await {
        Ok(stream) => stream,
        Err(_) => return Err(LatencyError::NetworkError)
    };
    println!("Connected to {}:{}", ip, port);
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

    println!("RTT Average: {:.2} ms | RTT Variance: {:.2} ms^2 | Jitter: {:.2} ms", average_rtt, variance, jitter);

    Ok(())
}

async fn receive_latency(port: u16) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;

    let (mut stream, client_addr) = listener.accept().await?;

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
    
    Ok(())
}

// Send data over TCP
async fn send_throughput(ip: IpAddr, port: u16) -> Result<(), std::io::Error> {
    let mut stream = TcpStream::connect(format!("{}:{}", ip, port)).await?;   

    // TCP can handle larger chunks efficiently due to its streaming nature
    let data = generate_random_u8_array(8192);

    let start_time = std::time::Instant::now();

    // Send data continuously for 10 seconds
    while start_time.elapsed() < Duration::from_secs(10) {
        match stream.write_all(&data).await {
            Ok(_) => {}
            Err(_) => { break; }
        }
    }

    // Ensure all data is sent before closing
    stream.shutdown().await?;
    Ok(())
}

// Receive data and measure throughput
async fn receive_throughput(port: u16, stats_tx: mpsc::Sender<(std::time::SystemTime, std::net::SocketAddr, usize, Duration)>) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    
    loop {
        let (mut stream, client_addr) = listener.accept().await?;
        let stats_tx = stats_tx.clone();
        
        // Handle each connection in a separate task
        tokio::spawn(async move {
            let mut buffer = [0; 8192];
            let mut total_bytes = 0;

            // Start timing when connection is established
            let start_time = std::time::Instant::now();
            let test_time = std::time::SystemTime::now();

            let timeout_duration = Duration::from_secs(15); // Longer timeout for TCP

            // Read data until connection closes or timeout
            loop {
                match timeout(timeout_duration, stream.read(&mut buffer)).await {
                    Ok(Ok(0)) => {
                        // Connection closed by client
                        break;
                    }
                    Ok(Ok(bytes_read)) => {
                        total_bytes += bytes_read;
                    }
                    Ok(Err(_)) | Err(_) => {
                        // Read error or timeout
                        break;
                    }
                }
            }

            let duration = start_time.elapsed();

            // Send stats back to main thread
            _ = stats_tx.send((test_time, client_addr, total_bytes, duration)).await;
        });
    }
}

fn format_bandwidth(
    bytes_per_second: f64,
    bits_mode: bool,
) -> String {
    let is_negative = bytes_per_second < 0.0;
    let value = bytes_per_second.abs();

    // Convert to bits if in bits mode
    let converted_value = if bits_mode {
        value * 8.0 // bytes to bits
    } else {
        value // keep as bytes per second
    };

    let unit: &str;
    let scale: f64;

    if converted_value >= 1_000_000_000.0 {
        scale = 1_000_000_000.0;
        unit = if bits_mode { "Gbps" } else { "GB/s" };
    } else if converted_value >= 1_000_000.0 {
        scale = 1_000_000.0;
        unit = if bits_mode { "Mbps" } else { "MB/s" };
    } else if converted_value >= 1_000.0 {
        scale = 1_000.0;
        unit = if bits_mode { "Kbps" } else { "KB/s" };
    } else {
        scale = 1.0;
        unit = if bits_mode { "bps" } else { "B/s" };
    }

    let formatted_value = converted_value / scale;

    let sign = if is_negative { "-" } else { "" };

    format!("{}{:.2}{}", sign, formatted_value, unit)
}


#[tokio::main]
async fn main() {
    let send_ip: IpAddr = "127.0.0.1".parse().unwrap();
    let send_port = 49369;

    // Interface to receiving task
    // let (stats_tx, mut stats_rx) = mpsc::channel(10);

    // Start receiver
    // let receive_task = tokio::spawn(async move {
    //     _ = receive_throughput(send_port, stats_tx).await;
    // });

    // Give the receiver time to start listening
    sleep(Duration::from_millis(100)).await;

    let latency_task = tokio::spawn(receive_latency(send_port));

    _ = sleep(Duration::from_secs(1)).await;

    // Start sender(s) to test
    // let promiseA = tokio::spawn(send_throughput(send_ip, send_port));
    // let promiseB = tokio::spawn(send_throughput(send_ip, send_port));

    let latency_sender = tokio::spawn(send_latency(send_ip, send_port));

    // while let Some((start_time, client, total_bytes, duration)) = stats_rx.recv().await {
    //     let throughput = (total_bytes as f64) / duration.as_secs_f64();
    //     let datetime: DateTime<Utc> = start_time.into();
    //     let formatted_datetime = datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    //     println!("{:} | {:?} | {:}", formatted_datetime, client, format_bandwidth(throughput, false));
    // } 

    // _ = receive_task.await;
    _ = latency_task.await;

}
