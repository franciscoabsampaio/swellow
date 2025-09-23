from pathlib import Path
import platform
import subprocess
import sys
from typing import Optional


# Utility: find the Rust binary packaged with Python
def _swellow_bin() -> Path:
    system = platform.system()
    arch = platform.machine()

    current_directory = Path(__file__).parent

    if system == "Linux":
        return current_directory / f"bin/swellow-linux-{arch}/swellow"
    elif system == "Windows":
        return current_directory / f"bin/swellow-windows-{arch}/swellow.exe"
    elif system == "Darwin":
        return current_directory / f"bin/swellow-macos-{arch}/swellow"
    else:
        raise RuntimeError(f"Unsupported OS / architecture: {system} / {arch}")


def _run_swellow(*args):
    """Run the swellow Rust binary with args and forward exit code/stdout/stderr."""
    bin_path = _swellow_bin()
    cmd = [bin_path, *args]
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


def up(
    db: str,
    directory: str,
    current_version_id: Optional[int] = None,
    target_version_id: Optional[int] = None,
    plan: bool = False,
    dry_run: bool = False,
) -> None:
    """
    Apply migrations forward from the current to the target version.

    Args:
        db: Database connection string.
        directory: Path to the migration directory.
        current_version_id: The version ID currently applied (if known).
        target_version_id: The version ID to migrate up to (if specified).
        plan: If True, output the migration plan without applying changes.
        dry_run: If True, simulate the migration without modifying the database.
    """
    args = ["--db", db, "--dir", directory, "up"]
    if current_version_id is not None:
        args += ["--current-version-id", str(current_version_id)]
    if target_version_id is not None:
        args += ["--target-version-id", str(target_version_id)]
    if plan:
        args.append("--plan")
    if dry_run:
        args.append("--dry-run")
    _run_swellow(*args)


def down(
    db: str,
    directory: str,
    current_version_id: Optional[int] = None,
    target_version_id: Optional[int] = None,
    plan: bool = False,
    dry_run: bool = False,
) -> None:
    """
    Roll back migrations from the current to the target version.

    Args:
        db: Database connection string.
        directory: Path to the migration directory.
        current_version_id: The version ID currently applied (if known).
        target_version_id: The version ID to migrate down to (if specified).
        plan: If True, output the rollback plan without applying changes.
        dry_run: If True, simulate the rollback without modifying the database.
    """
    args = ["--db", db, "--dir", directory, "down"]
    if current_version_id is not None:
        args += ["--current-version-id", str(current_version_id)]
    if target_version_id is not None:
        args += ["--target-version-id", str(target_version_id)]
    if plan:
        args.append("--plan")
    if dry_run:
        args.append("--dry-run")
    _run_swellow(*args)


def peck(db: str, directory: str) -> None:
    """
    Verify connectivity to the database and migration directory.

    Args:
        db: Database connection string.
        directory: Path to the migration directory.
    """
    _run_swellow("--db", db, "--dir", directory, "peck")


def snapshot(db: str, directory: str) -> None:
    """
    Create a snapshot of the current migration directory state.

    Args:
        db: Database connection string.
        directory: Path to the migration directory.
    """
    _run_swellow("--db", db, "--dir", directory, "snapshot")
