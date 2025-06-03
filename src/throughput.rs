use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::time::{Duration, timeout};
use rand::Rng;
use std::net::IpAddr;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use std::time::SystemTime;


#[derive(Debug)]
pub enum ThroughputError {
    PortError,
    TimeoutError,
    SocketError,
    CrashError
}
fn generate_random_u8_array(length: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..length).map(|_| rng.random_range(0..=255)).collect()
}

// Send data over TCP
pub async fn send_throughput(ip: IpAddr, port: u16) -> Result<(), std::io::Error> {
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
async fn receive_throughput(port: u16, startup_tx: oneshot::Sender<()>) -> Result<(SystemTime, std::net::SocketAddr, usize, Duration), ThroughputError> {
    match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
        Ok(listener) => {
            _ = startup_tx.send(());
    
            match listener.accept().await {
                Ok((mut stream, client_addr)) => {
                    // Handle each connection in a separate task
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
                    return Ok((test_time, client_addr, total_bytes, duration))
                }
                Err(_) => {return Err(ThroughputError::SocketError)}
            }
            
            
        }
        Err(_) => return Err(ThroughputError::PortError)
    }
}

async fn select_port() -> Result<(JoinHandle<Result<(SystemTime, std::net::SocketAddr, usize, Duration), ThroughputError>>, u16), ThroughputError> {
    // pick random port in safe range
    let mut rng = rand::rng();
    const MIN_PORT: u16 = 49152;
    const MAX_PORT: u16 = 65535;

    let throughput_port: u16 = rng.random_range(MIN_PORT..=MAX_PORT);

    // start the throughput listener on this port as well as the stats channel
    let (throughput_startup_tx, throughput_startup_rx) = oneshot::channel();

    let throughput_task = tokio::spawn(receive_throughput(throughput_port, throughput_startup_tx));

    // max time for task to start up
    let timeout_duration = Duration::from_secs(1);

    match timeout(timeout_duration, throughput_startup_rx).await {
        Ok(Ok(())) => {
            return Ok((throughput_task, throughput_port))
        }
        Ok(Err(_)) => {
            return Err(ThroughputError::PortError)
        }
        Err(_) => {
            return Err(ThroughputError::TimeoutError)
        }
    }

}

pub async fn downloader() -> Result<(JoinHandle<Result<(SystemTime, std::net::SocketAddr, usize, Duration), ThroughputError>>, u16), ThroughputError> {
    // timeout for maximum time to wait for throughput task to start up
    let timeout_duration = Duration::from_secs(5);

    loop {
        match timeout(timeout_duration, select_port()).await {
            Ok(Ok((throughput_task, throughput_port))) => {return Ok((throughput_task, throughput_port))}
            Ok(Err(throughput_error)) => {return Err(throughput_error)}
            Err(_) => {return Err(ThroughputError::TimeoutError)}
        }
    }
}