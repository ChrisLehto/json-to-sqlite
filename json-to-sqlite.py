import sys
import sqlite3
import hashlib
from pathlib import Path

DB_PATH = Path("test.db")

schema = """
CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_key TEXT NOT NULL,
  version INTEGER NOT NULL,
  filename TEXT NOT NULL,
  checksum TEXT NOT NULL UNIQUE,
  json_content TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE (doc_key, version)
);
"""

def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(schema)

def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def store_JSON(JSON_PATH: Path, doc_key: str | None = None) -> None:
    dataBytes = JSON_PATH.read_bytes()
    checksum = sha256(dataBytes)

    if doc_key is None:
        doc_key = JSON_PATH.stem

    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT id, doc_key, version FROM files WHERE checksum=?", 
                           (checksum,)).fetchone()
        if row:
            print(f"[Skip] Identical content already stored as {row[1]} v{row[2]} (id={row[0]})")
            return
        
        last_version, last_checksum = last_version_checksum(conn, doc_key)
        
        if last_checksum == checksum:
            print(f"[Skip] Latest version for '{doc_key}' already has this checksum")
            return
        
        new_version = last_version + 1

        conn.execute("INSERT INTO files (doc_key, version, filename, checksum, json_content) VALUES (?,?,?,?,?)",
                     (doc_key, new_version, JSON_PATH.name, checksum, dataBytes.decode("utf-8")))
        
        conn.commit()
        print(f"[OK] Stored {doc_key} v{new_version} from {JSON_PATH.name} (checksum={checksum[:12]}...)")

def last_version_checksum(conn: sqlite3.Connection, doc_key: str):
    row = conn.execute("SELECT version, checksum FROM files WHERE doc_key=? ORDER BY version DESC LIMIT 1",
                       (doc_key,)).fetchone()
    return (row[0], row[1]) if row else(0, None)

def list_files() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """SELECT doc_key,
                      MAX(version) as latest_version,
                      COUNT(*) as total_versions,
                      MIN(created_at) as first_seen,
                      MAX(created_at) as last_seen
               FROM files
               GROUP BY doc_key
               ORDER BY last_seen DESC"""
        ).fetchall()

        if not rows:
            print("[empty] No files stored yet")
            return

        for row in rows:
            doc_key, latest_v, total_v, first_seen, last_seen = row
            print(f"{doc_key:20} v{latest_v:<3} ({total_v} versions)  first={first_seen}  last={last_seen}")

def list_versions(doc_key: str) -> None:
    """Print all versions stored for a given document key."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """SELECT version, checksum, filename, created_at
               FROM files
               WHERE doc_key=?
               ORDER BY version ASC""",
            (doc_key,)
        ).fetchall()

    if not rows:
        print(f"[empty] No versions found for '{doc_key}'")
        return

    for version, checksum, filename, created_at in rows:
        print(f"v{version:<3}  {checksum[:12]}…  {filename:20}  {created_at}")

def list_versions(doc_key: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """SELECT version, checksum, filename, created_at
               FROM files
               WHERE doc_key=?
               ORDER BY version ASC""",
            (doc_key,)
        ).fetchall()

    if not rows:
        print(f"[empty] No versions found for '{doc_key}'")
        return

    for version, checksum, filename, created_at in rows:
        print(f"v{version:<3}  {checksum[:12]}…  {filename:20}  {created_at}")

if __name__ == "__main__":
    init_db()

    if len(sys.argv) < 2:
        print("Usage:")
        print("  store <path/to/parsed.json> [doc_key]")
        print("  list")
        print("  versions <doc_key>")
        raise SystemExit(1)

    cmd = sys.argv[1].lower()

    if cmd == "store":
        if len(sys.argv) < 3:
            raise SystemExit("Missing JSON path.")
        json_path = Path(sys.argv[2])
        if not json_path.exists():
            raise SystemExit(f"File not found: {json_path}")
        # optional custom doc_key
        if len(sys.argv) >= 4:
            store_JSON(json_path, doc_key=sys.argv[3])
        else:
            store_JSON(json_path)

    elif cmd == "list":
        list_files()

    elif cmd == "versions":
        if len(sys.argv) < 3:
            raise SystemExit("Missing doc_key.")
        list_versions(sys.argv[2])

    else:
        raise SystemExit(f"Unknown command: {cmd}")
