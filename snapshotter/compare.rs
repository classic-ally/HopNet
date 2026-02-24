use std::path::Path;

use crate::schema::{FunctionResult, Snapshot};

pub struct CompareResult {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<(String, String)>,
    pub unchanged: usize,
}

fn values_equal(a: &serde_json::Value, b: &serde_json::Value, epsilon: f64) -> bool {
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            match (a.as_f64(), b.as_f64()) {
                (Some(af), Some(bf)) => (af - bf).abs() < epsilon,
                _ => a == b,
            }
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(x, y)| values_equal(x, y, epsilon))
        }
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, v)| b.get(k).map_or(false, |bv| values_equal(v, bv, epsilon)))
        }
        _ => a == b,
    }
}

fn describe_diff(a: &FunctionResult, b: &FunctionResult) -> Option<String> {
    match (a, b) {
        (FunctionResult::Ok { value: va }, FunctionResult::Ok { value: vb }) => {
            if !values_equal(va, vb, 1e-10) {
                Some("value changed".to_string())
            } else {
                None
            }
        }
        (FunctionResult::Error { error_variant: ea }, FunctionResult::Error { error_variant: eb }) => {
            if ea != eb {
                Some(format!("error variant changed: {} -> {}", ea, eb))
            } else {
                None
            }
        }
        (FunctionResult::Ok { .. }, FunctionResult::Error { error_variant }) => {
            Some(format!("was ok, now error: {}", error_variant))
        }
        (FunctionResult::Error { error_variant }, FunctionResult::Ok { .. }) => {
            Some(format!("was error: {}, now ok", error_variant))
        }
    }
}

pub fn run_compare(baseline_path: &Path, current_path: &Path, epsilon: f64) -> bool {
    let baseline_json =
        std::fs::read_to_string(baseline_path).expect("Failed to read baseline snapshot");
    let current_json =
        std::fs::read_to_string(current_path).expect("Failed to read current snapshot");

    let baseline: Snapshot =
        serde_json::from_str(&baseline_json).expect("Failed to parse baseline snapshot");
    let current: Snapshot =
        serde_json::from_str(&current_json).expect("Failed to parse current snapshot");

    println!(
        "Comparing: {} ({}) vs {} ({})",
        baseline_path.display(),
        baseline.git_commit,
        current_path.display(),
        current.git_commit,
    );

    let mut result = CompareResult {
        added: Vec::new(),
        removed: Vec::new(),
        changed: Vec::new(),
        unchanged: 0,
    };

    // Functions in current but not baseline
    for key in current.functions.keys() {
        if !baseline.functions.contains_key(key) {
            result.added.push(key.clone());
        }
    }

    // Functions in baseline but not current
    for key in baseline.functions.keys() {
        if !current.functions.contains_key(key) {
            result.removed.push(key.clone());
        }
    }

    // Functions in both — compare
    for (key, baseline_val) in &baseline.functions {
        if let Some(current_val) = current.functions.get(key) {
            if let Some(diff) = describe_diff(baseline_val, current_val) {
                result.changed.push((key.clone(), diff));
            } else {
                result.unchanged += 1;
            }
        }
    }

    // Report
    println!("\n--- Comparison Report ---");
    println!("Unchanged: {}", result.unchanged);

    if !result.added.is_empty() {
        println!("\nAdded ({}):", result.added.len());
        for name in &result.added {
            println!("  + {}", name);
        }
    }

    if !result.removed.is_empty() {
        println!("\nRemoved ({}):", result.removed.len());
        for name in &result.removed {
            println!("  - {}", name);
        }
    }

    if !result.changed.is_empty() {
        println!("\nChanged ({}):", result.changed.len());
        for (name, diff) in &result.changed {
            println!("  ~ {} — {}", name, diff);
        }
    }

    let has_differences =
        !result.added.is_empty() || !result.removed.is_empty() || !result.changed.is_empty();

    if has_differences {
        println!("\nResult: DIFFERENCES FOUND");
    } else {
        println!("\nResult: IDENTICAL");
    }

    has_differences
}
