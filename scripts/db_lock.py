"""
Simple file-based locking for SQLite databases.

Prevents simultaneous writes to a shared database (e.g. SharePoint).
"""

from pathlib import Path
from datetime import datetime, timezone
import getpass                              # Get user ID
import socket                               # Get machine ID

# Raised when the database is already locked by another process.
class DatabaseLockedError(RuntimeError):  
    pass

### Internal Function: Given a database path, return the default lock file path
# (same directory, same stem, plus ".lock" extension).
def _default_lock_path(db_path: Path) -> Path:   
    return db_path.with_suffix(".lock")

### Public Function: Checks if .lock file exists
# Raises DatabaseLockedError if file exists
# If not: creates it and returns file Path
def acquire_lock(
    db_path: str | Path,                    # Path to the SQLite database file 
    lock_path: str | Path | None = None,    # Optional explicit lock file path
    purpose: str | None = None,             # Optional short description of ingest
) -> Path:

    db_path = Path(db_path)
    lock_file = Path(lock_path) if lock_path else _default_lock_path(db_path)

    # DB locked error message
    if lock_file.exists():
        message = lock_file.read_text(errors="ignore")
        raise DatabaseLockedError(
            f"Database is already locked.\n\n"
            f"Lock file: {lock_file}\n\n"
            f"{message}"
        )

    # Gather metadata for collaboration and debugging
    timestamp = datetime.now(timezone.utc).isoformat()
    user = getpass.getuser()
    host = socket.gethostname()

    # Builds lock file (plain text for human user)
    contents = [
        "DATABASE LOCK",
        f"Database: {db_path}",
        f"Locked by: {user}@{host}",
        f"Time (UTC): {timestamp}",
    ]
    # Option of including ingest purpose (future proofing)
    if purpose:
        contents.append(f"Purpose: {purpose}")

    # write to disk
    lock_file.write_text("\n".join(contents) + "\n")

    # Store Path and metadata for use outside of function
    return lock_file

### Public Function: Removes lock file and info
def release_lock(lock_file: str | Path) -> None:
    lock_file = Path(lock_file)

    if lock_file.exists():
        lock_file.unlink()
