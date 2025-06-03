pub fn format_bandwidth(
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
