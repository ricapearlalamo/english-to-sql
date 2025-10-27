import sqlite3, pathlib

db = 'retail_demo.sqlite'
p = pathlib.Path(db)
# Python 3.8+ supports missing_ok
p.unlink(missing_ok=True)

with sqlite3.connect(db) as con:
    with open('seed_sqlite.sql', 'r', encoding='utf-8') as f:
        con.executescript(f.read())

print(r"Created retail_demo.sqlite successfully in C:\streamlit\english_to_sql")
