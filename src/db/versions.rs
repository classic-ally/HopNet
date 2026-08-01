//! Node version attestations (RFC-019 S3): reads and the consensus-apply
//! write for the three version columns on `nodes`.

use crate::db::DatabaseError;
use hopnet_common::height::{height_from_db, height_to_db};

use crate::upgrade::NodeStagedVersion;

/// Consensus-apply write: overwrite the submitter's three version columns.
/// Wholesale overwrite is the semantics — logical idempotence lives here
/// (nonce dedup is per-tx only), and a previously staged version that the
/// deployment can no longer reach disappears with the next attestation.
pub fn set_node_version_tx(
    db_tx: &rusqlite::Transaction,
    report: &NodeStagedVersion,
) -> Result<(), DatabaseError> {
    let updated = db_tx
        .execute(
            "UPDATE nodes SET running_version_code = ?1,
                              staged_version_code = ?2,
                              version_attested_height = ?3
             WHERE node_id = ?4",
            rusqlite::params![
                report.running_code,
                report.staged_code,
                height_to_db(report.attested_height),
                report.node_id,
            ],
        )
        .map_err(|_| DatabaseError::ProcessingError)?;
    if updated != 1 {
        // Unreachable in practice: the submitter was signature-verified
        // against nodes before dispatch. Fail loud rather than commit a
        // silent no-op.
        return Err(DatabaseError::ProcessingError);
    }
    Ok(())
}

/// The committed (running, staged) codes for one node — attested height
/// deliberately EXCLUDED so the emission job's convergence compare is not
/// poisoned by height motion. None = node row absent.
pub fn read_node_version(
    conn: &rusqlite::Connection,
    node_id: i32,
) -> Result<Option<(Option<u32>, Option<u32>)>, DatabaseError> {
    conn.query_row(
        "SELECT running_version_code, staged_version_code FROM nodes WHERE node_id = ?",
        [node_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .map(Some)
    .or_else(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => Ok(None),
        _ => Err(DatabaseError::RecallError),
    })
}

/// One registered node's committed version claims, for the advisory.
#[derive(Debug, Clone, PartialEq)]
pub struct MeshNodeVersions {
    pub node_id: i32,
    pub name: String,
    pub running_code: Option<u32>,
    pub staged_code: Option<u32>,
    pub attested_height: Option<u64>,
}

/// Every registered node with its committed attestation (or Nones for a
/// node that has never attested).
pub fn read_mesh_versions(
    conn: &rusqlite::Connection,
) -> Result<Vec<MeshNodeVersions>, DatabaseError> {
    let mut stmt = conn
        .prepare(
            "SELECT node_id, name, running_version_code, staged_version_code,
                    version_attested_height
             FROM nodes ORDER BY node_id",
        )
        .map_err(|_| DatabaseError::RecallError)?;
    let rows = stmt
        .query_map([], |row| {
            Ok(MeshNodeVersions {
                node_id: row.get(0)?,
                name: row.get(1)?,
                running_code: row.get(2)?,
                staged_code: row.get(3)?,
                attested_height: row.get::<_, Option<i64>>(4)?.map(height_from_db),
            })
        })
        .map_err(|_| DatabaseError::RecallError)?;
    rows.collect::<Result<_, _>>()
        .map_err(|_| DatabaseError::RecallError)
}
