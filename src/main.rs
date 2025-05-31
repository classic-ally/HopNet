use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, timeout, sleep};
use tokio::sync::mpsc;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use rand::Rng;
use chrono::{DateTime, Utc};

fn generate_random_u8_array(length: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..length).map(|_| rng.random_range(0..=255)).collect()
}

// Send data over TCP
async fn send_throughput(ip: &str, port: u16) -> Result<(), std::io::Error> {
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
    let send_ip = "127.0.0.1";
    let send_port = 49367;

    // Interface to receiving task
    let (stats_tx, mut stats_rx) = mpsc::channel(10);

    // Start receiver
    let receive_task = tokio::spawn(async move {
        _ = receive_throughput(send_port, stats_tx).await;
    });

    // Give the receiver time to start listening
    sleep(Duration::from_millis(100)).await;

    // Start sender(s) to test
    let promiseA = tokio::spawn(send_throughput(send_ip, send_port));
    let promiseB = tokio::spawn(send_throughput(send_ip, send_port));

    while let Some((start_time, client, total_bytes, duration)) = stats_rx.recv().await {
        let throughput = (total_bytes as f64) / duration.as_secs_f64();
        let datetime: DateTime<Utc> = start_time.into();
        let formatted_datetime = datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        println!("{:} | {:?} | {:}", formatted_datetime, client, format_bandwidth(throughput, false));
    } 

    _ = receive_task.await;

}
