from abc import ABC, abstractmethod
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
        print(f"DEBUG Executing: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise RuntimeError(f"CLI Error: {result.stderr or result.stdout}")

    def up(self, db, directory, engine, json=False, target_version_id=None, no_transaction=False):
        self._run("up", db, directory, engine, json, target_version_id, no_transaction)

    def down(self, db, directory, engine, json=False, target_version_id=None, no_transaction=False):
        self._run("down", db, directory, engine, json, target_version_id, no_transaction)


class PythonCliAdapter(CliAdapter):
    """Invokes via `swellow`."""
    def __init__(self):
        super().__init__(["swellow"])


class RustBinaryAdapter(CliAdapter):
    """Invokes the raw Rust binary."""
    def __init__(self):
        # 1. Try to find 'swellow' on the system PATH (e.g. /usr/local/bin)
        binary = shutil.which("swellow")
        
        # 2. If not on PATH, try the binary bundled with the Python package
        if not binary:
            bundled = _swellow_bin()
            if bundled.exists():
                binary = str(bundled)

        if not binary:
            pytest.skip("Rust binary 'swellow' not found on PATH or in package.")
        
        super().__init__([binary])


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
