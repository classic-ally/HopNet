//! Human rendering: hand-rolled column tables and byte formatting.
//! Printing is the product here — everything else lives in ingress-core.

use ingress_core::fsck::FsckReport;
use ingress_core::status::{PhotoStatus, StatusReport};

pub fn print_json(value: &impl serde::Serialize) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).expect("report types serialize")
    );
}

/// Left-aligned columns sized to the widest cell.
pub fn table(headers: &[&str], rows: &[Vec<String>]) {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
    }
    let line = |cells: Vec<&str>| {
        let mut out = String::from("  ");
        for (i, cell) in cells.iter().enumerate() {
            out.push_str(&format!("{:<width$}  ", cell, width = widths[i]));
        }
        println!("{}", out.trim_end());
    };
    line(headers.to_vec());
    for row in rows {
        line(row.iter().map(String::as_str).collect());
    }
}

pub fn human_bytes(bytes: i64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn opt<T: std::fmt::Display>(v: &Option<T>) -> String {
    v.as_ref().map(T::to_string).unwrap_or_else(|| "-".into())
}

pub fn print_status(report: &StatusReport) {
    println!("LIBRARIES");
    let rows: Vec<Vec<String>> = report
        .libraries
        .iter()
        .map(|l| {
            vec![
                l.config.library_id.to_string(),
                l.config.display_name.clone(),
                l.stats.photos_active.to_string(),
                l.stats.photos_pending.to_string(),
                l.stats.tombstones.to_string(),
                // What the mesh has not been told. Both are held back from
                // reclamation until they propagate — tombstones from hard
                // delete, edits from spool eviction — so a number that
                // stops falling is the operator-visible symptom of a
                // daemon that cannot reach its node or no longer holds
                // responsibility for the scope.
                l.stats.tombstones_unpropagated.to_string(),
                l.stats.edits_unpropagated.to_string(),
                l.stats.blob_count.to_string(),
                human_bytes(l.stats.blob_bytes),
            ]
        })
        .collect();
    table(
        &[
            "ID", "NAME", "ACTIVE", "PENDING", "TOMB", "UNSENT-T", "UNSENT-E", "BLOBS", "SIZE",
        ],
        &rows,
    );

    let p = &report.pipeline;
    println!("\nPIPELINE");
    println!("  pending resources: {}", p.resources_pending);
    println!(
        "  awaiting retry:    {} (earliest {})",
        p.retries.awaiting_retry,
        opt(&p.retries.earliest_next_retry_at)
    );
    println!("  gave up:           {}", p.retries.gave_up);
    println!("  unmapped photos:   {}", p.unmapped_photos);
}

pub fn print_photo(view: &PhotoStatus) {
    let photo = &view.photo;
    println!("photo_id:     {}", photo.photo_id);
    println!("library:      {}", opt(&photo.library_id));
    println!("cloud_id:     {}", opt(&photo.cloud_id));
    println!("local_id:     {}", opt(&photo.local_id));
    println!("discovered:   {}", photo.discovered_at);
    println!("materialized: {}", opt(&photo.materialized_at));
    println!("deleted:      {}", opt(&photo.deleted_at));
    if let Some(group) = &photo.group_id {
        println!(
            "group:        {group} (index {}, pick {})",
            opt(&photo.group_index),
            photo.is_group_pick
        );
    }
    println!(
        "descriptor:   {}",
        if photo.descriptor_json.is_some() {
            "present"
        } else {
            "MISSING"
        }
    );

    println!("\nRESOURCES");
    let rows: Vec<Vec<String>> = view
        .resources
        .iter()
        .map(|r| {
            let state = if r.record.written_at.is_some() {
                "written".to_string()
            } else if r.record.next_retry_at.is_some() {
                format!("retrying ({})", r.record.retry_count)
            } else if r.record.content_hash.is_some() {
                "superseded-pending".to_string()
            } else {
                "pending".to_string()
            };
            let blob = match (&r.blob_path, r.blob_exists, r.evicted) {
                (Some(_), _, true) => "(evicted — in HopNet)".into(),
                (Some(p), Some(true), _) => p.display().to_string(),
                (Some(p), _, _) => format!("{} (MISSING)", p.display()),
                (None, ..) => "-".into(),
            };
            vec![
                r.record.resource_type.as_str().to_string(),
                state,
                r.record
                    .content_hash
                    .as_ref()
                    .map(|h| h.as_str()[..12].to_string())
                    .unwrap_or_else(|| "-".into()),
                blob,
            ]
        })
        .collect();
    table(&["TYPE", "STATE", "HASH", "BLOB"], &rows);

    if !view.events.is_empty() {
        println!("\nEVENTS (newest first)");
        for e in &view.events {
            println!(
                "  {}  {}  {}",
                e.at,
                e.event_type,
                e.detail.as_deref().unwrap_or("")
            );
        }
    }
}

pub fn print_fsck(report: &FsckReport) {
    if !report.missing_blobs.is_empty() {
        println!(
            "!!! BYTE LOSS: {} blob file(s) missing !!!",
            report.missing_blobs.len()
        );
        println!("    Not repairable from local state; re-fetch from PhotoKit if the");
        println!("    assets still exist there.");
        for m in &report.missing_blobs {
            println!("  {}  {}", m.library_id, m.expected_path.display());
        }
        println!();
    }
    section(
        "refcount drift",
        report.refcount_drift.len(),
        report.refcount_repaired.then_some("repaired"),
    );
    for d in &report.refcount_drift {
        println!("  {}  {}  {:?}", d.library_id, d.content_hash, d.kind);
    }
    section(
        "orphan blob files",
        report.orphan_blobs.len(),
        (report.orphans_deleted > 0).then_some("deleted under --repair"),
    );
    for o in &report.orphan_blobs {
        println!("  {}", o.path.display());
    }
    section("ext mismatches", report.ext_mismatches.len(), None);
    for e in &report.ext_mismatches {
        println!(
            "  {} row says .{}, file is {}",
            e.content_hash,
            e.row_ext,
            e.path.display()
        );
    }
    section(
        "foreign files in blob tree",
        report.foreign_files.len(),
        None,
    );
    for f in &report.foreign_files {
        println!("  {}", f.display());
    }
    if report.is_clean() {
        println!("clean");
    }
}

fn section(label: &str, count: usize, note: Option<&str>) {
    if count > 0 {
        match note {
            Some(note) => println!("{label}: {count} ({note})"),
            None => println!("{label}: {count}"),
        }
    }
}
