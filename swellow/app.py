import json
from pathlib import Path
import platform
import subprocess
import sys


_parse_verbosity = lambda verbose: "" if verbose == 0 else ("-vv" if verbose > 1 else "-v")

# Custom error classes
class SwellowError(Exception):
    """Base class for all Swellow CLI errors."""
    exit_code = 1  # default

    def __init__(self, message: str = ""):
        super().__init__(message)
        self.message = message


class SwellowArgumentError(SwellowError):
    exit_code = 2


class SwellowEngineError(SwellowError):
    exit_code = 3


class SwellowFileNotFoundError(SwellowError):
    exit_code = 2


class SwellowIoError(SwellowError):
    exit_code = 4


class SwellowParserError(SwellowError):
    exit_code = 5


class SwellowVersionError(SwellowError):
    exit_code = 6


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


def _parse_error(stdout: str, stderr: str):
    try:
        result = json.loads(stdout)
    except json.JSONDecodeError:
        raise SwellowError(f"Non-JSON error output:\n{stderr or stdout}")

    if result.get("status") != "error":
        raise SwellowError(f"Unexpected non-error response: {result}")

    error = result.get("error") or {}
    err_type = error.get("type", "unknown")
    message = error.get("message", "Unknown error")

    if err_type == "argument":
        raise SwellowArgumentError(message)
    elif err_type == "engine":
        raise SwellowEngineError(message)
    elif err_type == "file_not_found":
        raise SwellowFileNotFoundError(message)
    elif err_type == "io":
        raise SwellowIoError(message)
    elif err_type == "parser":
        raise SwellowParserError(message)
    elif err_type == "version":
        raise SwellowVersionError(message)
    else:
        raise SwellowError(message)


def _run_swellow(*args, capture_output=True, parse_error=True) -> int:
    """
    Run the swellow Rust binary with args, parse output, and raise custom errors.
    Returns exit code if successful, otherwise raises SwellowError or subclass.
    """
    bin_path = _swellow_bin()
    if not bin_path.exists():
        raise FileNotFoundError(f"Swellow binary not found at {bin_path}")
    cmd = [bin_path, *args]
    result = subprocess.run(cmd, capture_output=capture_output, text=True, check=False)
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    
    # Handle errors
    if result.returncode != 0 and parse_error:
        _parse_error(stdout, stderr)
    
    return result.returncode


def up(
    db: str,
    directory: str,
    engine: str = "postgres",
    verbose: int = 0,
    quiet: bool = False,
    json: bool = False,
    current_version_id: int = None,
    target_version_id: int = None,
    plan: bool = False,
    dry_run: bool = False,
) -> int:
    """
    Apply migrations forward from the current to the target version.

    Args:
        db: Database connection string.
        directory: Path to the migration directory.
        engine: The database engine to use.
        verbose: Verbosity level (0-2).
        quiet: If True, suppress all output.
        json: If True, output in JSON format. Suppresses normal output.
        current_version_id: The version ID currently applied (if known).
        target_version_id: The version ID to migrate up to (if specified).
        plan: If True, output the migration plan without applying changes.
        dry_run: If True, simulate the migration without modifying the database.

    Returns:
        int: The return code from the swellow CLI process. Error handling is performed by the caller.
    """
    args = ["--db", db, "--dir", directory, "--engine", engine]

    if verbose:
        args.append(_parse_verbosity(verbose))
    if quiet:
        args.append("--quiet")
    if json:
        args.append("--json")

    args.append(sys._getframe().f_code.co_name)
    
    if current_version_id is not None:
        args += ["--current-version-id", str(current_version_id)]
    if target_version_id is not None:
        args += ["--target-version-id", str(target_version_id)]
    if plan:
        args.append("--plan")
    if dry_run:
        args.append("--dry-run")
    return _run_swellow(*args, capture_output=json, parse_error=json)


def down(
    db: str,
    directory: str,
    engine: str = "postgres",
    verbose: int = 0,
    quiet: bool = False,
    json: bool = False,
    current_version_id: int = None,
    target_version_id: int = None,
    plan: bool = False,
    dry_run: bool = False,
) -> int:
    """
    Roll back migrations from the current to the target version.

    Args:
        db: Database connection string.
        directory: Path to the migration directory.
        engine: The database engine to use.
        verbose: Verbosity level (0-2).
        quiet: If True, suppress all output.
        json: If True, output in JSON format. Suppresses normal output.
        current_version_id: The version ID currently applied (if known).
        target_version_id: The version ID to migrate down to (if specified).
        plan: If True, output the rollback plan without applying changes.
        dry_run: If True, simulate the rollback without modifying the database.

    Returns:
        int: The return code from the swellow CLI process. Error handling is performed by the caller.
    """
    args = ["--db", db, "--dir", directory, "--engine", engine]

    if verbose:
        args.append(_parse_verbosity(verbose))
    if quiet:
        args.append("--quiet")
    if json:
        args.append("--json")

    args.append(sys._getframe().f_code.co_name)

    if current_version_id is not None:
        args += ["--current-version-id", str(current_version_id)]
    if target_version_id is not None:
        args += ["--target-version-id", str(target_version_id)]
    if plan:
        args.append("--plan")
    if dry_run:
        args.append("--dry-run")
    return _run_swellow(*args, capture_output=json, parse_error=json)


def peck(
    db: str,
    directory: str,
    engine: str = "postgres",
    verbose: int = 0,
    quiet: bool = False,
    json: bool = False
) -> int:
    """
    Verify connectivity to the database and migration directory.

    Args:
        db: Database connection string.
        directory: Path to the migration directory.
        engine: The database engine to use.
        verbose: Verbosity level (0-2).
        quiet: If True, suppress all output.
        json: If True, output in JSON format. Suppresses normal output.

    Returns:
        int: The return code from the swellow CLI process. Error handling is performed by the caller.
    """
    args = ["--db", db, "--dir", directory, "--engine", engine]

    if verbose:
        args.append(_parse_verbosity(verbose))
    if quiet:
        args.append("--quiet")
    if json:
        args.append("--json")

    args.append(sys._getframe().f_code.co_name)
    
    return _run_swellow(*args, capture_output=json, parse_error=json)


def snapshot(
    db: str,
    directory: str,
    engine: str = "postgres",
    verbose: int = 0,
    quiet: bool = False,
    json: bool = False,
) -> int:
    """
    Create a snapshot of the current migration directory state.

    Args:
        db: Database connection string.
        directory: Path to the migration directory.
        engine: The database engine to use.
        verbose: Verbosity level (0-2).
        quiet: If True, suppress all output.
        json: If True, output in JSON format.

    Returns:
        int: The return code from the swellow CLI process. Error handling is performed by the caller.
    """
    args = ["--db", db, "--dir", directory, "--engine", engine]

    if verbose:
        args.append(_parse_verbosity(verbose))
    if quiet:
        args.append("--quiet")
    if json:
        args.append("--json")

    args.append(sys._getframe().f_code.co_name)

    return _run_swellow(*args, capture_output=json, parse_error=json)
