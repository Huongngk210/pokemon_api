import requests
import pandas as pd
import duckdb

def extract_data(limit=10, last_id=None) -> tuple:
    """
    Extracts Pokemon data from PokeAPI and saves it as a Parquet file.
    Arguments:
        limit (int): Number of records to fetch per request. Default is 10.
        last_id (int): The ID to start fetching from. Default is None.
    Returns:
        tuple: (file_path, next_id)
    """
    conn = duckdb.connect("pokedex.duckdb")
    conn.execute("CREATE TABLE IF NOT EXISTS metadata (last_id INTEGER)")
    
    if last_id is None:
        last_id = conn.execute("SELECT COALESCE(MAX(last_id), 0) FROM metadata").fetchone()[0]
    conn.close()
    
    url = f"https://pokeapi.co/api/v2/pokemon?limit={limit}&offset={last_id}"
    print(f"Getting data from: {url}")
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data: {e}")
        raise
    
    data = response.json()
    df = pd.DataFrame(data["results"])
    table = duckdb.from_df(df).to_arrow_table()
    print("Preview extracted data:", table.to_pandas().head())
    
    file_path = f"pokedex_{str(last_id).zfill(3)}.parquet"
    duckdb.from_arrow(table).write_parquet(file_path)
    print(f"Saved to: {file_path}")
    
    return file_path, last_id + limit

def load_data(file_path, new_last_id):
    """Loads Pokemon data from a Parquet file into DuckDB."""
    conn = duckdb.connect("pokedex.duckdb")
    conn.execute(""" 
                    CREATE TABLE IF NOT EXISTS pokedex 
                    (last_id INTEGER, 
                    pokemon_id INTEGER, 
                    name TEXT, 
                    url TEXT)
                """)
    
    conn.execute("""
        INSERT INTO pokedex 
        SELECT 
            ?, 
            CAST(regexp_extract(url, '/pokemon/(\d+)/', 1) AS INTEGER), 
            name, 
            url
        FROM read_parquet(?)
    """, [new_last_id, file_path])
    
    preview_df = conn.execute("""
        SELECT * FROM pokedex WHERE last_id = ? LIMIT 3
    """, [new_last_id]).fetchdf()
    print("Preview loaded data:", preview_df)
    
    conn.execute("UPDATE metadata SET last_id = ? WHERE last_id IS NOT NULL", [new_last_id])
    if conn.execute("SELECT COUNT(*) FROM metadata").fetchone()[0] == 0:
        conn.execute("INSERT INTO metadata VALUES (?)", [new_last_id])
    
    print("Data loaded successfully")
    conn.close()

def transform_data():
    """Creates aggregated statistics for Pokemon data."""
    conn = duckdb.connect("pokedex.duckdb")
    conn.execute("""
        CREATE OR REPLACE TABLE pokemon_stats AS
        SELECT 
            COUNT(*) AS total_pokemon, 
            MIN(pokemon_id) AS first_id, 
            MAX(pokemon_id) AS last_id
        FROM pokedex
    """)
    
    df = conn.execute("SELECT * FROM pokemon_stats").fetchdf()
    print("Preview transformed data:", df)
    # return "pokemon_stats"

def main():
    print("Extracting data...")
    file_path_1, new_last_id_1 = extract_data(limit=30, last_id=None)
    file_path_2, new_last_id_2 = extract_data(limit=30, last_id=new_last_id_1)
    
    print("Loading data...")
    load_data(file_path_1, new_last_id_1)
    load_data(file_path_2, new_last_id_2)
  
    print("Transforming data...")
    transform_data()
  
    print("ELT Pipeline completed successfully!")

if __name__ == "__main__":
    main()