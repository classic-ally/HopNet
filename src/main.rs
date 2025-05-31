use tokio::net::UdpSocket;
use tokio::time::{Duration, timeout, sleep};
use tokio::sync::mpsc;
use rand::Rng;
use chrono::{DateTime, Utc};

fn generate_random_u8_array(length: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..length).map(|_| rng.random_range(0..=255)).collect()
}

// Send data over UDP
async fn send_throughput(ip: &str, port: u16) -> Result<(), std::io::Error> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    socket.connect(format!("{}:{}", ip, port)).await?;

    // we should make an adaptive MTU algorithm here to max all connections
    let data = generate_random_u8_array(1400);

    let start_time = std::time::Instant::now();

    // send first packet outside loop to catch MTU error etc
    socket.send(data.as_slice()).await?;
    while start_time.elapsed() < Duration::from_secs(10) {
        match socket.send(data.as_slice()).await {
            Ok(_) => {}
            Err(_) => { break; }
        }
    }

    Ok(())
}

// Receive data and measure throughput
async fn receive_throughput(port: u16, stats_tx: mpsc::Sender<(std::time::SystemTime, std::net::SocketAddr, usize, Duration)>) -> Result<(), std::io::Error> {
    loop {
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", port)).await?;
        let mut buffer = [0; 65535]; // max UDP payload
        let mut total_bytes = 0;

        let start_time: std::time::Instant;
        let test_time: std::time::SystemTime;
        let client: std::net::SocketAddr;

        // wait for first connection and log the client
        (_, client) = socket.recv_from(&mut buffer).await?;

        // start the clocks
        start_time = std::time::Instant::now();
        test_time = std::time::SystemTime::now();

        let timeout_duration = Duration::from_secs(3);

        while start_time.elapsed() < timeout_duration {
            let (bytes, src) = timeout(timeout_duration,socket.recv_from(&mut buffer)).await??;
            if src == client {
                total_bytes += bytes;
            }
        }

        let duration: Duration = start_time.elapsed();

        // tell our caller
        _ = stats_tx.send((test_time, client, total_bytes, duration)).await;

        drop(socket);
        sleep(Duration::from_secs(1)).await;
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

    // Start sender
    match send_throughput(send_ip, send_port).await {
        Ok(()) => println!("Send completed successfully"),
        Err(e) => eprintln!("Error testing throughput: {}", e),
    }

    while let Some((start_time, client, total_bytes, duration)) = stats_rx.recv().await {
        let throughput = (total_bytes as f64) / duration.as_secs_f64();
        let datetime: DateTime<Utc> = start_time.into();
        let formatted_datetime = datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        println!("{:} | {:?} | {:}", formatted_datetime, client, format_bandwidth(throughput, false));
    } 

    _ = receive_task.await;

}
