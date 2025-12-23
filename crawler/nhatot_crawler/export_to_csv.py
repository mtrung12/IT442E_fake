import duckdb
import pandas as pd
from logger import log

def export_to_csv(db_file, table_name, csv_file):
    con = duckdb.connect(db_file, read_only=True)
    
    tables = con.execute("SHOW TABLES").fetchall()
    if ('ads',) not in tables:
        log.error(f"Table '{table_name}' doesn's exist!")
        con.close()
        return
    
    df = con.execute(f"SELECT * FROM {table_name} ORDER BY \"id\"").fetchdf()
    con.close()
    
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    log.info(f"Exported {len(df)} ads → {csv_file}")
    log.info(f"Columns ({len(df.columns)}): {list(df.columns)}")