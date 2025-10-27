import sqlite3, pathlib
db = 'retail_demo.sqlite'
p = pathlib.Path(db)
if p.exists(): p.unlink()
con = sqlite3.connect(db)
with open('seed_sqlite.sql','r',encoding='utf-8') as f:
    con.executescript(f.read())
con.commit()
# quick sanity check:
cur = con.cursor()
tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';").fetchall()
print("Tables:", tables)
con.close()
