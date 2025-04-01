import requests
import pandas as pd
import duckdb

def extract_data(limit=10, last_id=None) -> tuple:
    """
    Extracts Pokemon data from the PokeAPI and saves it to a Parquet file.
    This function fetches Pokemon data from the PokeAPI, processes it into a DataFrame,
    and saves it as a Parquet file. It also maintains a record of the last extracted ID
    in a DuckDB database for pagination purposes.
    Args:
        limit (int, optional): Number of Pokemon records to fetch per request. Defaults to 10.
        last_id (int, optional): The ID to start fetching from. If None, retrieves last saved ID
            from metadata table. Defaults to None.
    Returns:
        tuple: A tuple containing:
            - str: Path to the saved Parquet file
            - int: Next ID to fetch from (current last_id + limit)
    Raises:
        requests.exceptions.RequestException: If the API request fails
    """
    conn = duckdb.connect("pokedex.duckdb")
    conn.execute("CREATE TABLE IF NOT EXISTS metadata (last_id INTEGER)")
    if last_id is None:
        last_id = conn.execute("SELECT COALESCE(MAX(last_id), 0) FROM metadata").fetchone()[0]
    conn.close()
    
    offset = last_id
    url = f"https://pokeapi.co/api/v2/pokemon?limit={limit}&offset={offset}"
    print("🌐 Getting data from: ", url)
    
    try:
        response = requests.get(url=url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch data: {e}")
        raise
    
    data = response.json()
    df = pd.DataFrame(data["results"])
    table = duckdb.from_df(df).to_arrow_table()
    print("👀 Preview extracted data:", table.to_pandas().head())
    
    file_path = f"pokedex_{str(last_id).zfill(10)}.parquet"
    duckdb.from_arrow(table).write_parquet(file_path)
    print("💾 Saved to: ", file_path)
    
    return file_path, last_id + limit

def load_data(file_path, new_last_id):
    """Loads pokemon data from a parquet file into a DuckDB database.
    This function creates a 'pokedex' table if it doesn't exist, loads data from a parquet file,
    and updates the metadata table with the last processed ID.
    Args:
        file_path (str): Path to the parquet file containing pokemon data
        new_last_id (int): ID to be used as the last_id for the newly loaded records
    Returns:
        None
    """
    conn = duckdb.connect("pokedex.duckdb")
    conn.execute("CREATE TABLE IF NOT EXISTS pokedex (last_id INTEGER, pokemon_id INTEGER, name TEXT, url TEXT)")
    
    conn.execute("""
        INSERT INTO pokedex 
        SELECT 
            ? as last_id,
            CAST(regexp_extract(url, '/pokemon/(\d+)/', 1) AS INTEGER) as pokemon_id,
            name,
            url 
        FROM read_parquet(?)
    """, [new_last_id, file_path])
    
    preview_df = conn.execute("""
        SELECT *
        FROM pokedex 
        WHERE last_id = ?
        LIMIT 5
    """, [new_last_id]).fetchdf()
    print("📊 Preview loaded data: ", preview_df)
    
    conn.execute("UPDATE metadata SET last_id = ? WHERE last_id IS NOT NULL", [new_last_id])
    result = conn.execute("SELECT COUNT(*) FROM metadata").fetchone()[0]
    if result == 0:
        conn.execute("INSERT INTO metadata VALUES (?)", [new_last_id])
    
    print("✅ Data loaded successfully")
    conn.close()

def transform_data():
    """
    Transform data from the raw pokedex table and create aggregated statistics.
    This function:
    1. Creates a new table 'pokemon_stats' with aggregated metrics
    2. Calculates total count of pokemon, minimum and maximum pokemon IDs
    3. Prints a preview of the transformed data
    Returns:
        str: Name of the created table ('pokemon_stats')
    """
    conn = duckdb.connect("pokedex.duckdb")
    conn.execute("""
        CREATE OR REPLACE TABLE pokemon_stats AS
        SELECT COUNT(*) AS total_pokemon, 
               MIN(pokemon_id) AS first_id, 
               MAX(pokemon_id) AS last_id 
        FROM pokedex
    """)
    
    df = conn.execute("SELECT * FROM pokemon_stats").fetchdf()
    print("📈 Preview transformed data: ", df)
    return "pokemon_stats"

def main():
    print("🚀 Extracting data...")
    file_path_1, new_last_id_1 = extract_data(limit=10, last_id=None)
    print("🔄 Extracting more data...")
    file_path_2, new_last_id_2 = extract_data(limit=10, last_id=new_last_id_1)
    
    print("📥 Loading data...")
    load_data(file_path_1, new_last_id_1)
    load_data(file_path_2, new_last_id_2)
  
    print("🔧 Transforming data...")
    transform_data()
  
    print("🎉 ELT Pipeline completed successfully!")

if __name__ == "__main__":
    main()