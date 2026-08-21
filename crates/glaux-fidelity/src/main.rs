//! `glaux-fidelity` — the differential fidelity suite CLI.
//!
//! ```text
//! glaux-fidelity replay [--update]          offline: run the corpus on glaux and diff snapshots
//!                                           (--update writes missing/UNVERIFIED snapshots from glaux)
//! glaux-fidelity record [--profile P] [--region R] [--bucket B] [--keep] [--case NAME]...
//!                                           record snapshots against real AWS Athena
//! glaux-fidelity coverage                   regenerate docs/sql-coverage.md from the corpus results
//! ```

use std::process::ExitCode;

use glaux_fidelity::athena::{self, RecordOptions};
use glaux_fidelity::suite::{self, CaseResult};
use glaux_fidelity::{corpus, corpus_dir, coverage, firehose, repo_root, snapshot_dir};

fn usage() -> ExitCode {
    eprintln!(
        "usage:\n  glaux-fidelity replay [--update]\n  glaux-fidelity record [--profile P] \
         [--region R] [--bucket B] [--keep] [--case NAME]...\n  glaux-fidelity coverage"
    );
    ExitCode::from(2)
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = args.first() else {
        return usage();
    };
    let result = match command.as_str() {
        "replay" => replay(&args[1..]).await,
        "record" => record(&args[1..]).await,
        "coverage" => regenerate_coverage().await,
        _ => return usage(),
    };
    match result {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn replay(args: &[String]) -> glaux_fidelity::Result<ExitCode> {
    let update = match args {
        [] => false,
        [flag] if flag == "--update" => true,
        _ => return Ok(usage()),
    };
    let cases = corpus::load(&corpus_dir())?;
    if update {
        let written = suite::self_record(&cases, &snapshot_dir()).await?;
        println!(
            "wrote {} UNVERIFIED snapshot(s) from glaux into {}",
            written.len(),
            snapshot_dir().display()
        );
        let artifacts = firehose::self_record(&snapshot_dir()).await?;
        println!(
            "wrote {} UNVERIFIED Firehose artifact snapshot(s)",
            artifacts.len()
        );
    }
    let reports = suite::replay(&cases, &snapshot_dir()).await?;
    let mut failed = 0;
    for report in &reports {
        match &report.result {
            CaseResult::Match => {}
            CaseResult::MissingSnapshot => {
                failed += 1;
                println!(
                    "MISSING  {} (no snapshot; run `replay --update` or `record`)",
                    report.case.name
                );
            }
            CaseResult::Mismatch(differences) => {
                failed += 1;
                println!("MISMATCH {}", report.case.name);
                for d in differences {
                    println!("    {d}");
                }
            }
        }
    }
    for orphan in suite::orphans(&cases, &snapshot_dir())? {
        failed += 1;
        println!("ORPHAN   {orphan}.snap has no corpus case");
    }
    let firehose_reports = firehose::replay(&snapshot_dir()).await?;
    for report in &firehose_reports {
        if let CaseResult::Mismatch(differences) = &report.result {
            failed += 1;
            println!("MISMATCH {}", report.name);
            for d in differences {
                println!("    {d}");
            }
        } else if report.result == CaseResult::MissingSnapshot {
            failed += 1;
            println!("MISSING  {}", report.name);
        }
    }
    let verified = reports
        .iter()
        .filter(|r| r.provenance.as_ref().is_some_and(|p| p.is_verified()))
        .count();
    println!(
        "{} SQL case(s) + {} Firehose artifact(s): {} failed; {} of {} SQL snapshots verified against real Athena",
        reports.len(),
        firehose_reports.len(),
        failed,
        verified,
        reports.len()
    );
    Ok(if failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

async fn record(args: &[String]) -> glaux_fidelity::Result<ExitCode> {
    let mut options = RecordOptions::default();
    let mut only: Vec<String> = Vec::new();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--profile" => {
                options.profile = iter.next().cloned().ok_or_else(|| usage_err("--profile"))?
            }
            "--region" => {
                options.region = Some(iter.next().cloned().ok_or_else(|| usage_err("--region"))?)
            }
            "--bucket" => {
                options.bucket = Some(iter.next().cloned().ok_or_else(|| usage_err("--bucket"))?)
            }
            "--keep" => options.keep = true,
            "--case" => only.push(iter.next().cloned().ok_or_else(|| usage_err("--case"))?),
            _ => return Ok(usage()),
        }
    }
    let mut cases = corpus::load(&corpus_dir())?;
    if !only.is_empty() {
        cases.retain(|c| only.contains(&c.name));
        if cases.len() != only.len() {
            return Err(glaux_fidelity::HarnessError::new(format!(
                "unknown case(s) in --case: {only:?}"
            )));
        }
    }
    let recording = athena::record(&cases, &options).await?;
    for snap in &recording.snapshots {
        snap.write(&snapshot_dir())?;
    }
    println!(
        "recorded {} snapshot(s) against Athena ({}, {}) into {}",
        recording.snapshots.len(),
        recording.region,
        recording.engine,
        snapshot_dir().display()
    );
    Ok(ExitCode::SUCCESS)
}

fn usage_err(flag: &str) -> glaux_fidelity::HarnessError {
    glaux_fidelity::HarnessError::new(format!("{flag} needs a value"))
}

async fn regenerate_coverage() -> glaux_fidelity::Result<ExitCode> {
    let cases = corpus::load(&corpus_dir())?;
    let reports = suite::replay(&cases, &snapshot_dir()).await?;
    let firehose_reports = firehose::replay(&snapshot_dir()).await?;
    let path = repo_root().join("docs/sql-coverage.md");
    std::fs::write(
        &path,
        coverage::render_document(&reports, &firehose_reports),
    )?;
    println!("wrote {}", path.display());
    Ok(ExitCode::SUCCESS)
}
