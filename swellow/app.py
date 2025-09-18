import subprocess
import sys
import os


# Utility: find the Rust binary packaged with Python
def _swellow_bin():
    here = os.path.dirname(__file__)
    return os.path.join(here, "swellow")


def _run_swellow(*args):
    """Run the swellow Rust binary with args and forward exit code/stdout/stderr."""
    bin_path = _swellow_bin()
    cmd = [bin_path, *args]
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


def up(db, dir, current_version_id, target_version_id, plan, dry_run):
    args = [
        "--db", db,
        "--dir", dir,
        "up"
    ]
    if current_version_id is not None:
        args += ["--current-version-id", str(current_version_id)]
    if target_version_id is not None:
        args += ["--target-version-id", str(target_version_id)]
    if plan:
        args.append("--plan")
    if dry_run:
        args.append("--dry-run")
    _run_swellow(*args)


def down(db, dir, current_version_id, target_version_id, plan, dry_run):
    args = [
        "--db", db,
        "--dir", dir,
        "down"
    ]
    if current_version_id is not None:
        args += ["--current-version-id", str(current_version_id)]
    if target_version_id is not None:
        args += ["--target-version-id", str(target_version_id)]
    if plan:
        args.append("--plan")
    if dry_run:
        args.append("--dry-run")
    _run_swellow(*args)


def peck(db):
    _run_swellow("--db", db, "peck")


def snapshot(db, dir):
    _run_swellow("--db", db, "--dir", dir, "snapshot")
