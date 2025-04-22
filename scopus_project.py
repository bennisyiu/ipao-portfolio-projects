import pandas as pd
import json
import requests
import pytz
import psycopg2
import os
import time
import re
from datetime import timedelta, datetime
from pandas import json_normalize
from dotenv import load_dotenv
from requests.exceptions import Timeout, RequestException


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

    # Include all available fields
    fields = [
        'dc:identifier', 'dc:title', 'dc:creator', 'prism:publicationName',
        'prism:isbn', 'prism:issn', 'prism:eIssn', 'prism:volume', 'prism:issueIdentifier',
        'prism:pageRange', 'prism:coverDate', 'prism:coverDisplayDate', 'prism:doi',
        'dc:description', 'citedby-count', 'prism:aggregationType', 'subtype',
        'subtypeDescription', 'source-id', 'openaccess', 'openaccessFlag',
        'freetoread', 'freetoreadLabel', 'eIssn', 'author', 'dc:contributor',
        'authkeywords', 'article-number', 'fund-acr', 'fund-no', 'fund-sponsor',
        'pubmed-id', 'orcid', 'eid', 'pii', 'prism:url', 'dc:publisher', 'affiliation',
        'prism:aggregationType', 'subtype', 'subtypeDescription', 'source-id', 'srctype',
        'citedby-count', 'prism:volume', 'prism:issueIdentifier', 'prism:pageRange',
        'subj'
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
    Return a dataframe with all available columns and database-friendly names
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

    # Convert numeric fields
    numeric_fields = ['citedby_count', 'openaccess', 'openaccessflag']
    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')

    # Convert date fields
    date_fields = ['prism_coverdate', 'prism_coverdisplaydate']
    for field in date_fields:
        if field in df.columns:
            df[field] = pd.to_datetime(df[field], errors='coerce')

    # Process list fields
    list_fields = ['author', 'affiliation', 'authkeywords',
                   'fund_sponsor', 'fund_acr', 'fund_no']
    for field in list_fields:
        if field in df.columns:
            df[field] = df[field].apply(
                lambda x: ', '.join(process_list_item(x)) if isinstance(x, list) else str(x) if x is not None else '')

    # Add a column for publication year
    if 'prism_coverdate' in df.columns:
        df['publication_year'] = df['prism_coverdate'].dt.year
    else:
        print("Warning: 'prism_coverdate' not found in the data. Using current year as fallback.")
        df['publication_year'] = datetime.now().year

    # Ensure publication_year is always an integer
    df['publication_year'] = df['publication_year'].fillna(
        datetime.now().year).astype(int)

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


def scopus_search_data_uploader(db_credentials, df, table_name='scopus_search_output'):
    """
    Upload Scopus data to Postgres database.
    Replaces existing data for the same publication year before inserting new data.
    """
    try:
        # Convert date columns to datetime
        date_columns = ['prism_coverdate']  # Updated column name
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Ensure publication_year is always an integer and replace NaN with a default value
        df['publication_year'] = df['publication_year'].fillna(
            9999).astype(int)

        # Extract publication year range from df
        min_year = df['publication_year'].min()
        max_year = df['publication_year'].max()

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

        # Delete existing data for the publication year range
        delete_query = f"""
            DELETE FROM {table_name}
            WHERE publication_year >= %s AND publication_year <= %s;
        """
        cursor.execute(delete_query, (min_year, max_year))

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
            """
            cursor.execute(insert_query)
            conn.commit()
            print(f"Uploaded chunk of {len(chunk)} records")

        print(
            f"Successfully uploaded data for publication years: {min_year} to {max_year}")

    except Exception as e:
        print(f"Error uploading data to table {table_name}: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def main():
    try:
        scopus_credentials, db_credentials = get_credentials()

        # Project 1: PolyU's research output
        query = "AFFIL(\"The Hong Kong Polytechnic University\") AND PUBYEAR = 2024"
        print(f"Executing Scopus search with query: {query}")

        csv_file = 'polyu_research_output.csv'
        existing_df = pd.DataFrame()
        try:
            existing_df = pd.read_csv(csv_file)
            print(
                f"Loaded {len(existing_df)} existing records from {csv_file}")
        except FileNotFoundError:
            print(f"No existing file found at {csv_file}. Starting fresh.")
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return

        max_results = 5000
        start = len(existing_df)  # Start from where we left off
        all_results = []

        while True:
            remaining_results = max_results - start
            if remaining_results <= 0:
                print("All available results have been retrieved in previous runs.")
                break

            polyu_results = scopus_search(
                scopus_credentials, query, start=start, max_results=remaining_results)

            if not polyu_results:
                print("No new results found.")
                break

            new_results = exclude_existing_results(polyu_results, existing_df)
            if not new_results:
                print("All results already exist in the CSV. Stopping search.")
                break

            all_results.extend(new_results)
            start += len(polyu_results)

            print(
                f"Retrieved {len(new_results)} new results. Total new results: {len(all_results)}")

            if len(polyu_results) < remaining_results:
                print("Reached the end of available results.")
                break

        if not all_results:
            print("No new results to process.")
            return

        # Process the results
        try:
            new_df = process_scopus_search_results(all_results)
        except Exception as e:
            print(f"Error processing search results: {e}")
            import traceback
            traceback.print_exc()
            return

        if new_df.empty:
            print("Processed dataframe is empty. No data to save.")
            return

        # Debugging: Print the first few rows of new_df
        print("First few rows of new_df:")
        print(new_df.head())

        # Combine with existing data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df.drop_duplicates(
            subset='dc_identifier', keep='last', inplace=True)

        # Save the results to a CSV file
        try:
            combined_df.to_csv(csv_file, index=False)
            print(
                f"\nResults appended to '{csv_file}'. Total records: {len(combined_df)}")
        except Exception as e:
            print(f"Error saving results to CSV: {e}")
            return

        # Display information about the data
        print(f"Number of new results: {len(new_df)}")
        print("\nDataframe columns:")
        print(new_df.columns)
        print("\nDataframe info:")
        new_df.info()

        # Uncomment the following lines if you want to upload to the database
        # try:
        #     scopus_search_data_uploader(
        #         db_credentials, new_df, table_name='scopus_search_output')
        #     print("SCOPUS Data has been uploaded to DB.")
        # except Exception as e:
        #     print(f"Error uploading data to database: {e}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Application failed: {str(e)}")
        import traceback
        traceback.print_exc()
