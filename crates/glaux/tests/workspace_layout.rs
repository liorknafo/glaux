//! Guards the licensing layout from the v0.1 design spec: per-crate license
//! fields and LICENSE files, and the Apache/AGPL dependency boundary at the
//! manifest level (CI additionally enforces it over the full `cargo tree`).

use std::fs;
use std::path::{Path, PathBuf};

/// `(crate dir, expected SPDX license field, expected LICENSE heading)`
const EXPECTED: &[(&str, &str, &str)] = &[
    ("glaux-athena", "Apache-2.0", "Apache License"),
    ("glaux-firehose", "Apache-2.0", "Apache License"),
    ("glaux-catalog", "Apache-2.0", "Apache License"),
    ("glaux-server", "Apache-2.0", "Apache License"),
    (
        "glaux",
        "AGPL-3.0-only",
        "GNU AFFERO GENERAL PUBLIC LICENSE",
    ),
];

/// Crates that must never depend on fakecloud (everything except the AGPL
/// all-in-one binary, which is the only crate allowed to link it).
const FAKECLOUD_FREE: &[&str] = &[
    "glaux-athena",
    "glaux-firehose",
    "glaux-catalog",
    "glaux-server",
];

fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate lives in crates/")
        .to_path_buf()
}

fn manifest(crate_dir: &str) -> String {
    let path = crates_dir().join(crate_dir).join("Cargo.toml");
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

#[test]
fn every_crate_declares_the_license_from_the_spec() {
    for (dir, spdx, _) in EXPECTED {
        let manifest = manifest(dir);
        let expected_line = format!("license = \"{spdx}\"");
        assert!(
            manifest.contains(&expected_line),
            "{dir}/Cargo.toml must contain `{expected_line}`"
        );
    }
}

#[test]
fn every_crate_ships_the_matching_license_text() {
    for (dir, _, heading) in EXPECTED {
        let path = crates_dir().join(dir).join("LICENSE");
        let text =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        assert!(
            text.contains(heading),
            "{} must contain the heading {heading:?}",
            path.display()
        );
    }
}

#[test]
fn apache_crates_declare_no_fakecloud_dependencies() {
    for dir in FAKECLOUD_FREE {
        let manifest = manifest(dir);
        assert!(
            !manifest.to_lowercase().contains("fakecloud"),
            "{dir}/Cargo.toml must not reference fakecloud crates \
             (Apache-2.0 engine code must stay independent of the AGPL boundary)"
        );
    }
}

#[test]
fn workspace_members_match_the_spec_table() {
    let root = crates_dir()
        .parent()
        .expect("crates/ lives in workspace root")
        .join("Cargo.toml");
    let manifest = fs::read_to_string(&root).expect("reading workspace Cargo.toml");
    for (dir, _, _) in EXPECTED {
        assert!(
            manifest.contains(&format!("\"crates/{dir}\"")),
            "workspace members must include crates/{dir}"
        );
    }
}
