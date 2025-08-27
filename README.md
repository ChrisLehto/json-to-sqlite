# JSON to SQLite

A simple Python tool that ingests JSON files into a SQLite database with:

- **Checksum deduplication** – skips identical files automatically  
- **Version tracking** – creates a new version whenever content changes  
- **Document keys** – groups versions by filename stem (or custom key)  

This demonstrates a clean, lightweight approach to **data persistence** using Python’s standard library only (no external dependencies). It’s a practical starting point for projects that need to track file history, build datasets for analysis, or serve as input for future AI/ML pipelines.

```bash
## Usage
# Store files into the database
python json-to-sqlite.py store filename.json
python json-to-sqlite.py store another.json

# List all documents with their latest versions
python json-to-sqlite.py list

# List all versions of a single document
python json-to-sqlite.py versions filename
```

### No JSON Files? Convert XML First
If you don’t have a JSON file available for testing,
you can use the Python XML Parser project:
https://github.com/ChrisLehto/python-xml-parser

It converts any XML file into JSON, which can then
be stored using this tool.
