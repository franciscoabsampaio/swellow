from abc import ABC, abstractmethod
from pathlib import Path
import pytest
import shutil
import subprocess
import sys

import swellow
from swellow.app import _swellow_bin 


# -----------------------------------------------------------------------------
# Interface Adapter (Driver)
# -----------------------------------------------------------------------------

class SwellowInterface(ABC):
    """Abstract base class to standardize how we call swellow."""

    @abstractmethod
    def up(self, db, directory, engine, json=False, target_version_id=None, no_transaction=False):
        pass

    @abstractmethod
    def down(self, db, directory, engine, json=False, target_version_id=None, no_transaction=False):
        pass


# -----------------------------------------------------------------------------
# Implementations
# -----------------------------------------------------------------------------

class PythonPackageAdapter(SwellowInterface):
    """Calls the Python functions directly (import swellow)."""
    
    def up(self, **kwargs):
        swellow.up(**kwargs)

    def down(self, **kwargs):
        swellow.down(**kwargs)


class CliAdapter(SwellowInterface):
    """Base class for CLI-based adapters (Python CLI and Rust Binary)."""
    
    def __init__(self, executable_cmd):
        self.executable_cmd = executable_cmd

    def _run(self, command, db, directory, engine, json_out, target_version, no_transaction):
        # Build command args
        cmd = self.executable_cmd + [
            "--db", db,
            "--dir", directory,
            "--engine", engine
        ]
        if json_out:
            cmd.append("--json")
        cmd.append(command)
        
        if target_version:
            cmd.extend(["--target-version-id", str(target_version)])
        if no_transaction:
            cmd.append("--no-transaction")

        # Execute
        print(f"[DEBUG] Executing: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise RuntimeError(f"CLI Error: {result.stderr or result.stdout}")

    def up(self, db, directory, engine, json=False, target_version_id=None, no_transaction=False):
        self._run("up", db, directory, engine, json, target_version_id, no_transaction)

    def down(self, db, directory, engine, json=False, target_version_id=None, no_transaction=False):
        self._run("down", db, directory, engine, json, target_version_id, no_transaction)


class PythonCliAdapter(CliAdapter):
    """
    Invokes the Python CLI.
    1) Defaults to the 'venv/bin/swellow' binary (which runs the Python CLI);
    2) Fallbacks to 'python -m swellow.cli' (which is what the Python CLI uses).
    """
    def __init__(self):
        # 1. Default: Check venv/bin/swellow
        venv_bin = Path(sys.executable).parent
        swellow_bin = venv_bin / "swellow"

        if swellow_bin.exists() and swellow_bin.is_file():
            print(f"[DEBUG] Using venv CLI binary: {swellow_bin}")
            super().__init__([str(swellow_bin)])
            return

        # 2. Fallback: python -m swellow.cli
        try:
            import swellow.cli  # noqa: F401
            print("[DEBUG] Using python module fallback: python -m swellow.cli")
            super().__init__([sys.executable, "-m", "swellow.cli"])
            return
        except ImportError:
            pass

        # 3. Nothing worked
        raise RuntimeError(
            "PythonCliAdapter: could not find venv 'swellow' binary "
            "or import swellow.cli"
        )


class RustBinaryAdapter(CliAdapter):
    """
    Invokes the pre-built Rust binary.
    1) Defaults to the '/usr/local/bin/swellow' binary.
    2) Fallbacks to the bundled Python package binary.
    """
    def __init__(self):
        # 1. Default to '/usr/local/bin/swellow'
        system_bin = Path("/usr/local/bin/swellow")

        if system_bin.exists() and system_bin.is_file():
            print(f"[DEBUG] Using system Rust binary: {system_bin}")
            super().__init__([str(system_bin)])
            return

        # 2. Fallback to bundled Python package binary
        bundled = _swellow_bin()
        if bundled and bundled.exists():
            print(f"[DEBUG] Using bundled Rust binary: {bundled}")
            super().__init__([str(bundled)])
            return

        # 3. Nothing worked
        raise RuntimeError(
            "RustBinaryAdapter: could not find 'swellow' in /usr/local/bin "
            "or bundled Python package binary"
        )

# -----------------------------------------------------------------------------
# Parametrized Fixture
# -----------------------------------------------------------------------------
@pytest.fixture(params=["py_pkg", "py_cli", "rs_cli"])
def swellow_driver(request):
    """
    Parametrized fixture that returns an adapter for each form of Swellow.
    Runs the test once per adapter.
    """
    if request.param == "py_pkg":
        return PythonPackageAdapter()
    elif request.param == "py_cli":
        return PythonCliAdapter()
    elif request.param == "rs_cli":
        return RustBinaryAdapter()
