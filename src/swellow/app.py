from pathlib import Path
import platform
import subprocess
from typing import Optional


# Custom error classes
class SwellowError(Exception):
    """Base exception for swellow errors."""
    pass


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



def _run_swellow(*args) -> int:
    """
    Run the swellow Rust binary with args, parse output, and raise custom errors.
    Returns exit code if successful, otherwise raises SwellowError or subclass.
    """
    bin_path = _swellow_bin()
    if not bin_path.exists():
        raise FileNotFoundError(f"Swellow binary not found at {bin_path}")
    cmd = [bin_path, *args]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    
    # Check for common errors in output
    if result.returncode != 0:
        # Missing .sql file
        msg=".sql\": No such file or directory"
        if msg in stdout or msg in stderr:
            raise FileNotFoundError(stdout + stderr)
        # Return a generic error
        raise SwellowError(stdout + stderr)
    
    return result.returncode


def resolve_directory(f):
    def wrapper(db, directory, **kwargs):
        return f(db, str(Path(directory).resolve()), **kwargs)
    
    return wrapper


@resolve_directory
def up(
    db: str,
    directory: str,
    current_version_id: Optional[int] = None,
    target_version_id: Optional[int] = None,
    plan: bool = False,
    dry_run: bool = False,
) -> int:
    """
    Apply migrations forward from the current to the target version.

    Args:
        db: Database connection string.
        directory: Path to the migration directory.
        current_version_id: The version ID currently applied (if known).
        target_version_id: The version ID to migrate up to (if specified).
        plan: If True, output the migration plan without applying changes.
        dry_run: If True, simulate the migration without modifying the database.

    Returns:
        int: The return code from the swellow CLI process. Error handling is performed by the caller.
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
    return _run_swellow(*args)


@resolve_directory
def down(
    db: str,
    directory: str,
    current_version_id: Optional[int] = None,
    target_version_id: Optional[int] = None,
    plan: bool = False,
    dry_run: bool = False,
) -> int:
    """
    Roll back migrations from the current to the target version.

    Args:
        db: Database connection string.
        directory: Path to the migration directory.
        current_version_id: The version ID currently applied (if known).
        target_version_id: The version ID to migrate down to (if specified).
        plan: If True, output the rollback plan without applying changes.
        dry_run: If True, simulate the rollback without modifying the database.

    Returns:
        int: The return code from the swellow CLI process. Error handling is performed by the caller.
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
    return _run_swellow(*args)


@resolve_directory
def peck(db: str, directory: str) -> int:
    """
    Verify connectivity to the database and migration directory.

    Args:
        db: Database connection string.
        directory: Path to the migration directory.

    Returns:
        int: The return code from the swellow CLI process. Error handling is performed by the caller.
    """
    return _run_swellow("--db", db, "--dir", directory, "peck")


@resolve_directory
def snapshot(db: str, directory: str) -> int:
    """
    Create a snapshot of the current migration directory state.

    Args:
        db: Database connection string.
        directory: Path to the migration directory.

    Returns:
        int: The return code from the swellow CLI process. Error handling is performed by the caller.
    """
    return _run_swellow("--db", db, "--dir", directory, "snapshot")
