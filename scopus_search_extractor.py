import pandas as pd
import json
import requests
import pytz
import psycopg2
import os
import time
import re
import json
import ast
from datetime import timedelta, datetime
from pandas import json_normalize
from dotenv import load_dotenv
from requests.exceptions import Timeout, RequestException
from psycopg2 import sql
from psycopg2.extras import execute_values


def get_credentials():
    """Load and validate credentials from environment variables."""

    print('get_credentials() method')
    load_dotenv()  # Load .env file

    # Facebook credentials
    scopus_api_key = os.getenv("SCOPUS_API_KEY")
    if not scopus_api_key:
        raise ValueError("SCOPUS_API_KEY is missing in .env!")

    scopus_credentials = {
        "access_token": scopus_api_key,
        "scopus_label": os.getenv("SCOPUS_LABEL")
    }

    # Database credentials
    db_credentials = {
        "hostname": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT")),  # Convert to integer
        "username": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "schema": os.getenv("DB_SCHEMA")
    }

    return scopus_credentials, db_credentials


def scopus_api_caller(url, params, headers, max_retries=3, timeout=20):
    print('scopus_api_caller() method')
    all_data = []
    retry_count = 0

    while url and retry_count < max_retries:
        try:
            print(f'Making request to URL: {url}')
            response = requests.get(
                url, params=params, headers=headers, timeout=timeout)
            print(f'Response status code: {response.status_code}')

            if response.status_code != 200:
                print(f'Error response content: {response.text}')

            response.raise_for_status()
            data = response.json()

            if 'search-results' in data and 'entry' in data['search-results']:
                all_data.extend(data['search-results']['entry'])
                print(
                    f"Collected {len(data['search-results']['entry'])} items. Total: {len(all_data)}")
            else:
                print("No data found in response")
                break

            # Check if there are more pages
            if 'link' in data['search-results']:
                next_link = next(
                    (link for link in data['search-results']['link'] if link['@ref'] == 'next'), None)
                if next_link:
                    url = next_link['@href']
                    params = {}  # Clear params as they're included in the next URL
                else:
                    url = None
                    print("No more pages")
            else:
                url = None
                print("No more pages")

            retry_count = 0  # Reset retry count on successful request

        except (Timeout, RequestException) as e:
            retry_count += 1
            print(
                f"Request failed: {e}. Retry attempt {retry_count} of {max_retries}")
            if retry_count == max_retries:
                print("Max retries reached. Exiting.")
                break
            time.sleep(2 ** retry_count)  # Exponential backoff

    print(f'Exiting scopus_api_caller. Total items collected: {len(all_data)}')
    return all_data


def scopus_search(scopus_credentials, query, start=0, count=25, sort='citedby-count', max_results=5000):
    print('scopus_search() method')

    scopus_api_key = scopus_credentials['access_token']
    url = 'https://api.elsevier.com/content/search/scopus'

    headers = {
        'X-ELS-APIKey': scopus_api_key,
        'Accept': 'application/json'
    }

    # list of fields
    fields = [
        'dc:identifier',  # Unique identifier
        'prism:doi',      # DOI for Abstract Retrieval API
        'prism:coverDate',  # Publication date
        'citedby-count',  # Citation count
        'prism:publicationName',  # Journal or conference name
        # Type of publication (e.g., Article, Conference Paper)
        'subtypeDescription',
    ]

    params = {
        'query': query,
        'field': ','.join(fields),
        'count': count,
        'start': start,
        'sort': sort
    }

    all_results = []
    total_results = None

    while len(all_results) < max_results:
        print(f'Full URL: {url}')
        print(f'Headers: {headers}')
        print(f'Params: {params}')

        # Fetch the data
        batch_results = scopus_api_caller(url, params, headers)

        if not batch_results:
            print("No results returned from API. Stopping search.")
            break

        all_results.extend(batch_results)

        # Check total number of results if not already set
        if total_results is None:
            total_results = int(batch_results[0].get(
                'search-results', {}).get('opensearch:totalResults', 0))
            print(f"Total results available: {total_results}")
            if total_results == 0:
                print("No results found for the given query.")
                break

        # Update start for the next page
        params['start'] = len(all_results)

        # Check if we've reached the end of results
        if len(all_results) >= total_results or len(all_results) >= max_results:
            print("All available results have been retrieved or max results reached.")
            break

        if len(batch_results) < count:
            print("Reached the end of available results.")
            break

    print(f'{len(all_results)} of SCOPUS data will be processed.')

    if not all_results:
        print("No results found for the given query.")

    return all_results


def process_scopus_search_results(all_data):
    """Process Scopus search results data
    Return a dataframe with selected columns and database-friendly names
    """
    if not all_data:
        print("No data received from Scopus API")
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.json_normalize(all_data)

    # Function to clean column names
    def clean_column_name(name):
        # Replace non-alphanumeric characters with underscores
        name = re.sub(r'[^a-zA-Z0-9]', '_', name)
        # Replace multiple underscores with a single underscore
        name = re.sub(r'_+', '_', name)
        # Remove leading or trailing underscores
        name = name.strip('_')
        # Convert to lowercase
        return name.lower()

    # Clean column names
    df.columns = [clean_column_name(col) for col in df.columns]

    # Ensure all fields from scopus_search() are present
    expected_fields = [
        'dc_identifier',
        'prism_doi',
        'prism_coverdate',
        'citedby_count',
        'prism_publicationname',
        'subtypedescription'
    ]

    for field in expected_fields:
        if field not in df.columns:
            df[field] = None
            print(
                f"Warning: '{field}' not found in API response. Added as empty column.")

    # Convert numeric fields
    if 'citedby_count' in df.columns:
        df['citedby_count'] = pd.to_numeric(
            df['citedby_count'], errors='coerce')

    # Convert date fields
    if 'prism_coverdate' in df.columns:
        df['prism_coverdate'] = pd.to_datetime(
            df['prism_coverdate'], errors='coerce')

    # Add a column for publication year
    if 'prism_coverdate' in df.columns:
        df['publication_year'] = df['prism_coverdate'].dt.year
    else:
        print(
            "Warning: 'prism_coverdate' not found in the data. Using 2100 as fallback year.")
        df['publication_year'] = 2100

    # Ensure publication_year is always an integer
    df['publication_year'] = df['publication_year'].fillna(2100).astype(int)

    # Print column names and their types for debugging
    print("Column names and types:")
    print(df.dtypes)

    # Print the first few rows for debugging
    print("First few rows of the processed dataframe:")
    print(df.head())

    return df


def process_list_item(item):
    if isinstance(item, dict):
        return [str(value) for value in item.values() if value]
    elif isinstance(item, str):
        return [item]
    elif isinstance(item, list):
        return [str(subitem) for subitem in item if subitem]
    else:
        return [str(item)] if item else []


def exclude_existing_results(new_results, existing_df):
    if existing_df.empty:
        return new_results

    existing_ids = set(existing_df['dc_identifier'].tolist())
    return [result for result in new_results if result.get('dc:identifier') not in existing_ids]


def scopus_search_data_uploader(db_credentials, df, table_name='scopus_search_raw'):
    """
    Upload Scopus data to Postgres database.
    Creates the table if it doesn't exist, then upserts data based on dc_identifier.
    """
    try:
        # Data preprocessing
        date_columns = ['prism_coverdate']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        df['publication_year'] = df['publication_year'].fillna(
            9999).astype(int)

        # Connect to the Postgres database
        conn = psycopg2.connect(
            host=db_credentials['hostname'],
            database=db_credentials['database'],
            user=db_credentials['username'],
            password=db_credentials['password'],
            port=db_credentials['port']
        )
        cursor = conn.cursor()

        # Set the schema
        cursor.execute(sql.SQL("SET search_path TO {};").format(
            sql.Identifier(db_credentials['schema'])
        ))

        # Check if table exists, if not create it
        cursor.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                fa BOOLEAN,
                prism_url TEXT,
                dc_identifier TEXT PRIMARY KEY,
                prism_publicationname TEXT,
                prism_coverdate DATE,
                prism_doi TEXT,
                citedby_count INTEGER,
                subtype TEXT,
                subtypedescription TEXT,
                publication_year INTEGER,
                publication_month INTEGER
            )
        """).format(sql.Identifier(table_name)))

        # Prepare the data for insertion
        columns = df.columns.tolist()

        # Construct the INSERT ... ON CONFLICT DO UPDATE query
        insert_query = sql.SQL("""
            INSERT INTO {} ({})
            VALUES %s
            ON CONFLICT (dc_identifier) DO UPDATE SET
            {}
        """).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(
                sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(col))
                for col in columns if col != 'dc_identifier'
            )
        )

        # Insert or update data in chunks
        chunk_size = 50
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            values = [tuple(row) for _, row in chunk.iterrows()]
            execute_values(cursor, insert_query, values)
            print(f"Processed chunk of {len(chunk)} records")

        conn.commit()
        print(f"Successfully uploaded/updated data for {len(df)} records")

    except Exception as e:
        print(f"Error uploading data to table {table_name}: {e}")
        conn.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Revised I - parameters: publication year + list of subtypedescription


def scopus_research_procedures(publication_year, document_types):
    """
    Fetches Scopus research output for PolyU for a specific year and
    list of document types, handling pagination via scopus_search,
    adds publication month, and saves to a year-specific CSV.

    Args:
        publication_year (int): The year to fetch publications for.
        document_types (list): A list of strings representing the
                                'subtypeDescription' values to query
                                (e.g., ['Article', 'Conference Paper']).

    Returns:
        pandas.DataFrame: A DataFrame containing the combined and deduplicated
                          results for the specified year and document types,
                          including a 'publication_month' column.
                          Returns an empty DataFrame on major failure.
    """
    try:
        # Assuming db_credentials not needed here
        scopus_credentials, _ = get_credentials()

        # Define the maximum results limit PER document type search for the given year
        # This limit is passed down to scopus_search
        max_results_per_type_query = 5000

        # Use a year-specific CSV file for loading/saving
        csv_file = f'polyu_research_output_{publication_year}.csv'
        existing_df = pd.DataFrame()
        try:
            existing_df = pd.read_csv(csv_file)
            # Ensure necessary columns exist from previous runs when loading
            # Note: process_scopus_search_results already adds publication_year
            # We need to potentially add publication_month if it's missing from old file
            if 'prism_coverdate' in existing_df.columns and 'publication_month' not in existing_df.columns:
                print(
                    f"Adding missing 'publication_month' column to existing data in {csv_file}")
                # Ensure prism_coverdate is datetime before extracting month
                existing_df['prism_coverdate'] = pd.to_datetime(
                    existing_df['prism_coverdate'], errors='coerce')
                existing_df['publication_month'] = existing_df['prism_coverdate'].dt.month.fillna(
                    0).astype(int)  # Use 0 for missing month
            print(
                f"Loaded {len(existing_df)} existing records from {csv_file}")
        except FileNotFoundError:
            print(
                f"No existing file found at {csv_file}. Starting fresh for year {publication_year}.")
        except Exception as e:
            print(
                f"Error reading CSV file {csv_file}: {e}. Starting fresh for year {publication_year}.")
            existing_df = pd.DataFrame()  # Ensure it's an empty DF if read fails

        # List to hold all *raw* results collected for this year across all specified types
        all_new_raw_results_for_year = []

        # --- Loop through each document type for the given year ---
        for doc_type in document_types:
            # Construct the Scopus query for the specific year and document type
            query = f'AFFIL("The Hong Kong Polytechnic University") AND PUBYEAR = {publication_year} AND SUBTYPE("{doc_type}")'
            print(
                f"\nExecuting Scopus search for Year: {publication_year}, Type: '{doc_type}'")
            print(f"Query: {query}")

            try:
                # Call your existing scopus_search function.
                # It internally handles pagination using scopus_api_caller up to max_results.
                # We start from 0 for each document type query.
                type_results = scopus_search(
                    scopus_credentials,
                    query,
                    start=0,
                    # count=25, # Use default count from scopus_search
                    # sort='citedby-count', # Use default sort from scopus_search
                    max_results=max_results_per_type_query  # Pass the limit
                )

                if type_results:
                    print(
                        f"Retrieved {len(type_results)} raw results for '{doc_type}'.")
                    # Extend the list of raw results for the year
                    all_new_raw_results_for_year.extend(type_results)
                else:
                    # scopus_search already prints messages if no results are found
                    print(
                        f"No results returned by scopus_search for '{doc_type}'.")

            except Exception as e:
                # Log error if scopus_search fails for a specific type, but continue
                print(
                    f"Error during scopus_search call for {doc_type} (Year: {publication_year}): {e}")
                print(f"Skipping document type '{doc_type}' and continuing...")
                # Optionally add more detailed logging here if needed
                # traceback.print_exc() # Uncomment for full traceback during debugging

        # --- Process all collected raw results for the year ---
        if not all_new_raw_results_for_year:
            print(
                f"\nNo new raw results collected for year {publication_year} across specified document types.")
            # Return the DataFrame loaded at the start (might be empty or contain previous data)
            return existing_df
        else:
            print(
                f"\nTotal new raw results collected across all types for {publication_year}: {len(all_new_raw_results_for_year)}")

            # --- Process the combined raw list using your existing function ---
            try:
                # process_scopus_search_results handles normalization, cleaning, and adds 'publication_year'
                new_df = process_scopus_search_results(
                    all_new_raw_results_for_year)

                if not new_df.empty:
                    # --- Add the new 'publication_month' column ---
                    if 'prism_coverdate' in new_df.columns:
                        # Your process_scopus_search_results already converts prism_coverdate to datetime
                        # Extract month, fill NaT/NaN with 0, convert to int
                        new_df['publication_month'] = new_df['prism_coverdate'].dt.month.fillna(
                            0).astype(int)
                        print("Added 'publication_month' column to new data.")
                    else:
                        # This case should ideally not happen if prism_coverdate is always requested and processed
                        print(
                            "Warning: 'prism_coverdate' column not found in processed DataFrame. Cannot add 'publication_month'.")
                        # Add column with default value
                        new_df['publication_month'] = 0

                    # --- Combine with existing data loaded earlier ---
                    # Ensure columns match if needed, but concat handles differences by creating NaNs
                    combined_df = pd.concat(
                        [existing_df, new_df], ignore_index=True)

                    # --- Deduplicate based on Scopus ID ---
                    # Use dc_identifier (cleaned name from process_scopus_search_results)
                    if 'dc_identifier' in combined_df.columns:
                        initial_rows = len(combined_df)
                        # Keep the 'last' entry, assuming newer fetches might have updated info (like citations)
                        combined_df.drop_duplicates(
                            subset='dc_identifier', keep='last', inplace=True)
                        dedup_rows = len(combined_df)
                        print(
                            f"Deduplicated records based on 'dc_identifier'. Kept {dedup_rows} out of {initial_rows} total records.")
                    else:
                        print(
                            "Warning: 'dc_identifier' column not found in combined DataFrame. Cannot deduplicate effectively.")

                    # --- Save updated data to the year-specific CSV ---
                    try:
                        combined_df.to_csv(
                            csv_file, index=False, encoding='utf-8-sig')
                        print(
                            f"\nResults saved to '{csv_file}'. Total records for {publication_year}: {len(combined_df)}")
                    except Exception as e:
                        print(f"Error saving CSV file '{csv_file}': {e}")
                        print("Returning in-memory DataFrame without saving.")

                    return combined_df  # Return the latest combined DataFrame for the year
                else:
                    print(
                        "Processing raw results returned an empty DataFrame. No new data to add.")
                    return existing_df  # Return the originally loaded data

            except Exception as e:
                print(
                    f"Error during process_scopus_search_results for year {publication_year}: {e}")
                traceback.print_exc()  # Print full traceback for processing errors
                # Return existing_df as processing failed, safer than returning partial/corrupt data
                print("Returning existing data due to processing error.")
                return existing_df

    except Exception as e:
        print(
            f"An unexpected error occurred in scopus_research_procedures: {e}")
        traceback.print_exc()  # Print full traceback for unexpected errors
        return pd.DataFrame()  # Return an empty DataFrame in case of major failure


def abstract_retrieval(scopus_credentials, doi):
    print(f'abstract_retrieval() method for DOI: {doi}')

    scopus_api_key = scopus_credentials['access_token']
    url = f'https://api.elsevier.com/content/abstract/doi/{doi}'

    headers = {
        'X-ELS-APIKey': scopus_api_key,
        'Accept': 'application/json'
    }

    response = requests.get(url, headers=headers)
    print(f"Response status code: {response.status_code}")
    response.raise_for_status()

    json_response = response.json()
    # Print first 500 characters
    print(
        f"Response structure: {json.dumps(json_response, indent=2)[:500]}...")

    return json_response


def process_abstract_retrieval_results(abstract_data):
    """Process Abstract Retrieval API results"""
    if not abstract_data:
        print("No data received from Abstract Retrieval API")
        return {}

    coredata = abstract_data.get(
        'abstracts-retrieval-response', {}).get('coredata', {})

    processed_data = {
        'dc:identifier': coredata.get('dc:identifier'),
        'dc:title': coredata.get('dc:title'),
        'prism:doi': coredata.get('prism:doi'),
        'prism:coverDate': coredata.get('prism:coverDate'),
        'citedby-count': coredata.get('citedby-count'),
        'prism:publicationName': coredata.get('prism:publicationName'),
        'subtypeDescription': coredata.get('subtypeDescription'),
        'prism:volume': coredata.get('prism:volume'),
        'prism:issueIdentifier': coredata.get('prism:issueIdentifier'),
        'prism:pageRange': coredata.get('prism:pageRange'),
        'openaccess': coredata.get('openaccess'),
        'pubmed-id': coredata.get('pubmed-id'),
    }

    # Process affiliation data
    affiliations = abstract_data.get(
        'abstracts-retrieval-response', {}).get('affiliation', [])
    processed_data['affiliations'] = [
        {
            'name': aff.get('affilname'),
            'city': aff.get('affiliation-city'),
            'country': aff.get('affiliation-country')
        }
        for aff in affiliations
    ]

    # Process author data
    authors = coredata.get('dc:creator', {}).get('author', [])
    processed_data['authors'] = [
        {
            'name': author.get('ce:indexed-name'),
            'affiliation': author.get('affiliation', {}).get('@id')
        }
        for author in authors
    ]

    return processed_data


def abstract_retrieval_data_uploader(db_credentials, df, table_name='abstract_retrieval_output'):
    """
    Upload Abstract Retrieval data to Postgres database.
    """
    try:
        # Connect to the Postgres database
        conn = psycopg2.connect(
            host=db_credentials['hostname'],
            database=db_credentials['database'],
            user=db_credentials['username'],
            password=db_credentials['password'],
            port=db_credentials['port']
        )
        cursor = conn.cursor()

        # Set the schema
        schema_query = f"SET search_path TO {db_credentials['schema']};"
        cursor.execute(schema_query)

        # Prepare the data for insertion
        columns = df.columns.tolist()

        # Insert data in chunks
        chunk_size = 50
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        for chunk in chunks:
            values_list = []
            for _, row in chunk.iterrows():
                values = []
                for col in columns:
                    value = row.get(col)
                    if pd.isnull(value):
                        values.append("NULL")
                    elif isinstance(value, str):
                        value = value.replace("'", "''")
                        values.append(f"'{value}'::text")
                    elif isinstance(value, (datetime, pd.Timestamp)):
                        values.append(f"'{value}'::timestamp")
                    elif isinstance(value, bool):
                        values.append(str(value).lower())
                    elif isinstance(value, (int, float)):
                        values.append(str(value))
                    else:
                        values.append(f"'{value}'::text")
                values_list.append(f"({','.join(values)})")

            column_names = ', '.join([f'"{col}"' for col in columns])
            insert_query = f"""
                INSERT INTO {table_name} ({column_names}) 
                VALUES {','.join(values_list)}
                ON CONFLICT (dc_identifier) DO UPDATE
                SET {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col != 'dc_identifier'])}
            """
            cursor.execute(insert_query)
            conn.commit()
            print(f"Uploaded chunk of {len(chunk)} records")

        print(f"Successfully uploaded data to {table_name}")

    except Exception as e:
        print(f"Error uploading data to table {table_name}: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def abstract_retrieval_procedures():
    try:
        scopus_credentials, db_credentials = get_credentials()

        # Load the CSV file with DOIs
        csv_file = 'polyu_research_output.csv'
        try:
            df = pd.read_csv(csv_file)
            print(f"Loaded {len(df)} records from {csv_file}")
        except FileNotFoundError:
            print(
                f"No file found at {csv_file}. Please run scopus_research_procedures first.")
            return
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return

        # Get unique DOIs
        dois = df['prism_doi'].dropna().unique()
        print(f"Found {len(dois)} unique DOIs to process")

        abstract_results = []
        for doi in dois:
            try:
                abstract_data = abstract_retrieval(scopus_credentials, doi)
                processed_abstract = process_abstract_retrieval_results(
                    abstract_data)
                abstract_results.append(processed_abstract)
                print(
                    f"Successfully retrieved and processed abstract for DOI: {doi}")
            except Exception as e:
                print(f"Error processing abstract for DOI {doi}: {e}")

        if not abstract_results:
            print("No abstract results to process.")
            return

        # Convert abstract results to DataFrame
        abstract_df = pd.DataFrame(abstract_results)

        # Print the first few rows of the abstract DataFrame
        print("\nFirst few rows of the abstract DataFrame:")
        print(abstract_df.head())

        # Print DataFrame info
        print("\nAbstract DataFrame info:")
        abstract_df.info()

        # Save abstract results to CSV
        abstract_csv_file = 'scopus_abstract_output.csv'
        abstract_df.to_csv(abstract_csv_file, index=False,
                           encoding='utf-8-sig')
        print(f"\nAbstract results saved to '{abstract_csv_file}'")

        # Upload abstract results to database
        try:
            abstract_retrieval_data_uploader(
                db_credentials, abstract_df, table_name='scopus_abstract_output')
            print("Abstract Retrieval Data has been uploaded to DB.")
        except Exception as e:
            print(f"Error uploading abstract data to database: {e}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()


def main():
    # publication year
    publication_year = 2020

    # List of document types to process
    doc_types_to_process = [
        "Article", "Book", "Book Chapter", "Conference Paper", "Data Paper",
        "Editorial", "Erratum", "Letter", "Note", "Retracted",
        "Review", "Short Survey"
    ]

    scopus_research_procedures(publication_year, doc_types_to_process)
    # abstract_retrieval_procedures()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Application failed: {str(e)}")
        import traceback
        traceback.print_exc()
