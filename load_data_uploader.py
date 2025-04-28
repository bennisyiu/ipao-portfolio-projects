# Run this file to upload SCOPUS data and OpenAlex data to the database
import psycopg2
import psycopg2.sql as sql
from psycopg2.extras import execute_values
import pandas as pd
import os
import glob
import numpy as np
import traceback
from dotenv import load_dotenv

# --- Configuration & Schema Definition ---

# Load Database Credentials from .env file
load_dotenv()
db_credentials = {
    'hostname': os.getenv('DB_HOSTNAME'),
    'database': os.getenv('DB_DATABASE'),
    'username': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT', 5432),  # Default port if not set
    'schema': os.getenv('DB_SCHEMA', 'public')  # Default schema if not set
}

# Check if all credentials are loaded
if not all(db_credentials.values()):
    raise ValueError(
        "Database credentials not fully set in .env file (DB_HOSTNAME, DB_DATABASE, DB_USERNAME, DB_PASSWORD, DB_PORT, DB_SCHEMA)")

INPUT_DATA_DIR = 'normalized_data_final'  # Directory with normalized CSVs
UPLOAD_CHUNK_SIZE = 500  # Number of rows to upload per transaction

# --- Define Table Schemas and File Mappings ---
# Structure: 'table_name': {'pattern': 'filename_pattern*.csv', 'columns': {'col_name': 'SQL_DATA_TYPE', ...}, 'pk': ['primary_key_col1', ...]}
# NOTE: Use TEXT for most string fields for flexibility, VARCHAR(N) if length is known/constrained.
#       Use INTEGER or BIGINT for counts, FLOAT for decimals, BOOLEAN for true/false, DATE or TIMESTAMP for dates.
TABLE_DEFINITIONS = {
    'raw_scopus_search': {
        'pattern': 'raw_scopus_search*.csv',
        'columns': {
            "prism_url": "TEXT", "dc_identifier": "TEXT", "prism_publicationname": "TEXT",
            "prism_coverdate": "DATE", "prism_doi": "TEXT", "citedby_count": "INTEGER",
            "subtype": "TEXT", "subtypedescription": "TEXT", "publication_year": "INTEGER",
            "publication_month": "INTEGER", "doi": "TEXT"
        },
        # Assuming Scopus EID is unique here based on user example
        'pk': ['dc_identifier']
                               # If not, reconsider PK, maybe add a unique row ID? Or use prism_doi if unique in this context.
    },
    'publications': {
        'pattern': 'publications*.csv',
        'columns': {
            "doi": "TEXT", "oa_id": "TEXT", "oa_doi": "TEXT", "oa_title": "TEXT", "oa_display_name": "TEXT",
            "oa_publication_year": "INTEGER", "oa_publication_date": "DATE", "oa_language": "VARCHAR(10)",
            "oa_type": "TEXT", "oa_cited_by_count": "INTEGER", "oa_fwci": "FLOAT", "oa_is_retracted": "BOOLEAN",
            "oa_updated_date": "TIMESTAMP", "oa_created_date": "DATE", "oa_primary_location_is_oa": "BOOLEAN",
            "oa_primary_location_landing_page_url": "TEXT", "oa_primary_location_source_id": "TEXT",
            "oa_primary_location_source_name": "TEXT", "oa_primary_location_source_issn_l": "TEXT",
            "oa_primary_location_source_is_oa": "BOOLEAN", "oa_primary_location_source_is_indexed_in_scopus": "BOOLEAN",
            # Stored as JSON string
            "oa_primary_location_source_host_org_name": "TEXT", "oa_primary_location_source_host_org_lineage_names": "TEXT",
            "oa_primary_location_source_type": "TEXT", "oa_biblio_volume": "TEXT", "oa_biblio_issue": "TEXT",
            "oa_biblio_first_page": "TEXT", "oa_biblio_last_page": "TEXT", "oa_primary_topic_id": "TEXT",
            "oa_primary_topic_name": "TEXT", "oa_primary_topic_score": "FLOAT", "oa_primary_topic_subfield_name": "TEXT",
            "oa_primary_topic_field_name": "TEXT", "oa_primary_topic_domain_name": "TEXT", "oa_cnp_value": "FLOAT",
            "oa_cnp_is_top_1_percent": "BOOLEAN", "oa_cnp_is_top_10_percent": "BOOLEAN", "oa_cbpy_min": "INTEGER",
            "oa_cbpy_max": "INTEGER", "oa_status": "TEXT"
        },
        'pk': ['doi']
    },
    'authors': {
        'pattern': 'authors*.csv',
        'columns': {
            "oa_author_id": "TEXT", "oa_author_name": "TEXT", "oa_author_orcid": "TEXT"
        },
        'pk': ['oa_author_id']
    },
    'institutions': {
        'pattern': 'institutions*.csv',
        'columns': {
            "oa_institution_id": "TEXT", "oa_institution_name": "TEXT", "oa_institution_ror": "TEXT",
            "oa_institution_country_code": "VARCHAR(10)", "oa_institution_type": "TEXT"
        },
        'pk': ['oa_institution_id']
    },
    'funders': {
        'pattern': 'funders*.csv',
        'columns': {
            "oa_funder_id": "TEXT", "oa_funder_name": "TEXT"
        },
        'pk': ['oa_funder_id']
    },
    'publication_authorships': {
        'pattern': 'publication_authorships*.csv',
        'columns': {
            "doi": "TEXT", "oa_author_id": "TEXT", "oa_author_position": "TEXT",
            "oa_author_is_corresponding": "BOOLEAN", "oa_author_raw_name": "TEXT"
        },
        'pk': ['doi', 'oa_author_id', 'oa_author_position']  # Composite PK
    },
    'authorship_institutions': {
        'pattern': 'authorship_institutions*.csv',
        'columns': {
            "doi": "TEXT", "oa_author_id": "TEXT", "oa_institution_id": "TEXT",
            "oa_raw_affiliation_string": "TEXT"
        },
        'pk': ['doi', 'oa_author_id', 'oa_institution_id']  # Composite PK
    },
    'authorship_countries': {
        'pattern': 'authorship_countries*.csv',
        'columns': {
            "doi": "TEXT", "oa_author_id": "TEXT", "oa_country_code": "VARCHAR(10)"
        },
        'pk': ['doi', 'oa_author_id', 'oa_country_code']  # Composite PK
    },
    'publication_funding': {
        'pattern': 'publication_funding*.csv',
        'columns': {
            "doi": "TEXT", "oa_funder_id": "TEXT", "oa_award_id": "TEXT"
        },
        # Composite PK, handle potential NULL award_id later if needed
        'pk': ['doi', 'oa_funder_id', 'oa_award_id']
                                                      # If award_id can be NULL and non-unique for a doi-funder pair, PK might be just (doi, oa_funder_id)
                                                      # Let's assume this combo is unique for now for upsert.
    },
    'publication_citation_counts': {
        'pattern': 'publication_citation_counts*.csv',
        'columns': {
            "doi": "TEXT", "year": "INTEGER", "cited_by_count": "INTEGER"
        },
        'pk': ['doi', 'year']  # Composite PK
    }
}

# --- Helper Functions ---


def ensure_dir(directory):
    """Creates a directory if it doesn't exist."""
    if not os.path.exists(directory):
        print(f"Creating output directory: {directory}")
        os.makedirs(directory)


def preprocess_dataframe(df, schema_columns):
    """Applies type conversions and handles NaNs for database upload."""
    print("Preprocessing DataFrame...")
    df_processed = df.copy()

    for col, dtype in schema_columns.items():
        if col not in df_processed.columns:
            print(
                f"  Warning: Column '{col}' defined in schema but not found in DataFrame. Skipping.")
            continue

        # Handle NaNs -> None (database NULL)
        # Use object type for columns that might mix types before converting NaNs
        if df_processed[col].isnull().any():
             try:
                 # Convert to object first to ensure NaNs are treated uniformly before replace
                 df_processed[col] = df_processed[col].astype(
                     object).where(pd.notnull(df_processed[col]), None)
             except Exception as e:
                  print(
                      f"  Warning: Could not replace NaN with None for column '{col}'. Error: {e}")

        # Type Conversions (handle errors gracefully)
        try:
            if 'INT' in dtype.upper():
                # Convert to float first to handle potential decimals from read_csv, then Int64 (nullable int)
                df_processed[col] = pd.to_numeric(
                    df_processed[col], errors='coerce').astype('Float64').astype('Int64')
            elif 'FLOAT' in dtype.upper() or 'REAL' in dtype.upper() or 'NUMERIC' in dtype.upper():
                 df_processed[col] = pd.to_numeric(
                     df_processed[col], errors='coerce').astype(float)
            elif 'DATE' in dtype.upper():
                # Coerce errors will turn unparseable dates into NaT (which becomes None)
                df_processed[col] = pd.to_datetime(
                    df_processed[col], errors='coerce').dt.date
            elif 'TIMESTAMP' in dtype.upper():
                df_processed[col] = pd.to_datetime(
                    df_processed[col], errors='coerce')
            elif 'BOOLEAN' in dtype.upper():
                 # Map common string representations of boolean to actual booleans
                 bool_map = {'true': True, 'false': False, 'TRUE': True, 'FALSE': False,
                     't': True, 'f': False, '1': True, '0': False, 1: True, 0: False}
                 # Apply map only if column is not already boolean, handle missing values
                 if not pd.api.types.is_bool_dtype(df_processed[col]):
                     df_processed[col] = df_processed[col].astype(str).str.lower().map(
                         bool_map).where(pd.notnull(df_processed[col]), None)
                 df_processed[col] = df_processed[col].astype(
                     'boolean')  # Use pandas nullable boolean
            # else: # TEXT, VARCHAR -> default string is usually fine
                # df_processed[col] = df_processed[col].astype(str) # Optional: ensure string type
                pass # Keep as object or let DB handle text conversion

        except Exception as e:
            print(f"  Warning: Could not apply type conversion for column '{col}' (Expected: {dtype}). Error: {e}")

    # Final check to replace any remaining pandas specific NA types with None
    df_processed = df_processed.replace({pd.NaT: None, pd.NA: None})
    print("Preprocessing finished.")
    return df_processed


# --- Main Upload Function ---

def upload_table_data(table_name, definition, input_dir, db_creds, chunk_size):
    """Finds CSVs, loads data, preprocesses, creates table, and upserts data."""
    print(f"\n--- Processing Table: {table_name} ---")
    pattern = definition['pattern']
    schema_columns = definition['columns']
    pk_columns = definition['pk']
    all_columns_list = list(schema_columns.keys()) # Get order from definition

    # 1. Find and Load Data
    input_files = glob.glob(os.path.join(input_dir, pattern))
    if not input_files:
        print(f"  No input files found for pattern '{pattern}'. Skipping table.")
        return

    print(f"  Found {len(input_files)} file parts. Loading...")
    try:
        df_list = []
        for f in input_files:
            df_list.append(pd.read_csv(f, low_memory=False, keep_default_na=True, na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null', 'None']))
        df_full = pd.concat(df_list, ignore_index=True)
        print(f"  Loaded total {len(df_full)} rows.")
    except Exception as e:
        print(f"  Error loading or concatenating files for {table_name}: {e}")
        traceback.print_exc()
        return

    # Check if DataFrame is empty
    if df_full.empty:
        print("  Loaded DataFrame is empty. Skipping upload.")
        return

    # Ensure all expected columns exist, add if missing (filled with NaN)
    for col in all_columns_list:
        if col not in df_full.columns:
             print(f"  Warning: Adding missing column '{col}' to DataFrame.")
             df_full[col] = np.nan # Add column filled with NaN

    # Reorder columns according to schema definition before preprocessing
    df_full = df_full[all_columns_list]

    # 2. Preprocess Data
    df_processed = preprocess_dataframe(df_full, schema_columns)

    # 3. Database Operations
    conn = None
    cursor = None
    try:
        print("  Connecting to database...")
        conn = psycopg2.connect(**db_creds)
        cursor = conn.cursor()
        print("  Setting schema search path...")
        cursor.execute(sql.SQL("SET search_path TO {};").format(sql.Identifier(db_creds['schema'])))

        # 4. Create Table If Not Exists
        print(f"  Ensuring table '{table_name}' exists...")
        create_table_sql = "CREATE TABLE IF NOT EXISTS {} (\n".format(sql.Identifier(table_name))
        col_defs = []
        for col, dtype in schema_columns.items():
            col_defs.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(dtype)).as_string(cursor)) # Use SQL() for data type part
        # Add Primary Key constraint
        if pk_columns:
            pk_constraint = ",\n    PRIMARY KEY ({})".format(sql.SQL(', ').join(map(sql.Identifier, pk_columns)).as_string(cursor))
            col_defs.append(pk_constraint)
        create_table_sql += ",\n    ".join(col_defs)
        create_table_sql += "\n);"
        # print(f"Debug CREATE TABLE SQL:\n{create_table_sql}\n") # Uncomment to debug SQL
        cursor.execute(create_table_sql)
        conn.commit() # Commit table creation separately

        # 5. Prepare and Execute Upsert
        print(f"  Preparing upsert query for {len(df_processed)} rows...")
        columns = df_processed.columns.tolist()
        
        # Handle composite primary key for conflict target
        conflict_target = sql.SQL(', ').join(map(sql.Identifier, pk_columns))
        
        # Create SET clause, excluding PK columns from update
        update_columns = [col for col in columns if col not in pk_columns]
        if not update_columns: # If only PK columns exist, just DO NOTHING
            upsert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO NOTHING").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                conflict_target
            )
        else:
            set_clause = sql.SQL(', ').join(
                sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(col))
                for col in update_columns
            )
            upsert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {}").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                conflict_target,
                set_clause
            )

        # Get data as tuples, ensuring correct order and None for NaNs
        values = [tuple(row) for _, row in df_processed.iterrows()]

        print(f"  Uploading data in chunks of {chunk_size}...")
        execute_values(cursor, upsert_sql, values, page_size=chunk_size)
        conn.commit() # Commit after all chunks are processed by execute_values

        print(f"  Successfully uploaded/updated data for {len(df_processed)} records to '{table_name}'.")

    except Exception as e:
        print(f"  ERROR during database operation for table {table_name}: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback() # Rollback transaction on error
    finally:
        print("  Closing database connection.")
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# --- Main Execution ---
if __name__ == "__main__":
    print("Starting data upload process...")
    start_time = time.time()

    # Iterate through all defined tables and upload
    for table, definition in TABLE_DEFINITIONS.items():
        upload_table_data(table, definition, INPUT_DATA_DIR, db_credentials, UPLOAD_CHUNK_SIZE)

    end_time = time.time()
    print(f"\n--- Total Upload Script Duration: {time.strftime('%H:%M:%S', time.gmtime(end_time - start_time))} ---")