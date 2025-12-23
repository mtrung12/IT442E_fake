import duckdb
def export_duckdb_to_csv(db_path, table_name, csv_path):
    con = None
    try:
        # Connect to the DuckDB database
        con = duckdb.connect(database=db_path, read_only=True)
        print(f"Connected to database: {db_path}")
        # Execute query to fetch all data from the table
        print(f"Fetching data from table: {table_name}")
        df = con.execute(f"SELECT * FROM {table_name}").fetch_df()
        if df.empty:
            print(f"Table '{table_name}' is empty. No CSV file will be created.")
            return
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"Successfully exported {len(df)} rows to {csv_path}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Ensure the connection is closed
        if con:
            con.close()
            print("Database connection closed.")
if __name__ == "__main__":
    DB_FILE = 'real_estate.db'
    TABLE_TO_EXPORT = 'mogi_listings'
    OUTPUT_CSV_FILE = 'mogi_export.csv'
    export_duckdb_to_csv(DB_FILE, TABLE_TO_EXPORT, OUTPUT_CSV_FILE)