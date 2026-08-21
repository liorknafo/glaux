//! The scaffolding binary must refuse to start: exiting nonzero with an
//! explicit "not yet implemented" error is the honest behavior mandated by
//! the "never silently wrong" product rule.

use std::process::Command;

#[test]
fn scaffolding_binary_refuses_to_start_with_explicit_error() {
    let output = Command::new(env!("CARGO_BIN_EXE_glaux"))
        .output()
        .expect("failed to run glaux binary");

    assert!(
        !output.status.success(),
        "scaffolding binary must exit nonzero, got: {:?}",
        output.status
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not yet implemented"),
        "stderr must name the missing implementation explicitly, got: {stderr}"
    );
}
