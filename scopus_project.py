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


def scopus_search_data_uploader(db_credentials, df, table_name='scopus_search_output'):
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
                publication_year INTEGER
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

    # Handle case where abstract_data is a list
    if isinstance(abstract_data, list):
        abstract_data = abstract_data[0] if abstract_data else {}

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
    if not isinstance(affiliations, list):
        affiliations = [affiliations] if affiliations else []
    processed_data['affiliations'] = [
        {
            'name': aff.get('affilname'),
            'city': aff.get('affiliation-city'),
            'country': aff.get('affiliation-country')
        }
        for aff in affiliations
    ]

    # Process author data
    authors = coredata.get('dc:creator', {})
    if isinstance(authors, dict):
        authors = authors.get('author', [])
    if not isinstance(authors, list):
        authors = [authors] if authors else []
    processed_data['authors'] = [
        {
            'name': author.get('ce:indexed-name'),
            'affiliation': author.get('affiliation', {}).get('@id') if isinstance(author.get('affiliation'), dict) else author.get('affiliation')
        }
        for author in authors
    ]

    return processed_data

# def process_abstract_retrieval_results(abstract_data):
#     """Process Abstract Retrieval API results"""
#     if not abstract_data:
#         print("No data received from Abstract Retrieval API")
#         return {}

#     coredata = abstract_data.get(
#         'abstracts-retrieval-response', {}).get('coredata', {})

#     processed_data = {
#         'dc:identifier': coredata.get('dc:identifier'),
#         'dc:title': coredata.get('dc:title'),
#         'prism:doi': coredata.get('prism:doi'),
#         'prism:coverDate': coredata.get('prism:coverDate'),
#         'citedby-count': coredata.get('citedby-count'),
#         'prism:publicationName': coredata.get('prism:publicationName'),
#         'subtypeDescription': coredata.get('subtypeDescription'),
#         'prism:volume': coredata.get('prism:volume'),
#         'prism:issueIdentifier': coredata.get('prism:issueIdentifier'),
#         'prism:pageRange': coredata.get('prism:pageRange'),
#         'openaccess': coredata.get('openaccess'),
#         'pubmed-id': coredata.get('pubmed-id'),
#     }

#     # Process affiliation data
#     affiliations = abstract_data.get(
#         'abstracts-retrieval-response', {}).get('affiliation', [])
#     processed_data['affiliations'] = [
#         {
#             'name': aff.get('affilname'),
#             'city': aff.get('affiliation-city'),
#             'country': aff.get('affiliation-country')
#         }
#         for aff in affiliations
#     ]

#     # Process author data
#     authors = coredata.get('dc:creator', {}).get('author', [])
#     processed_data['authors'] = [
#         {
#             'name': author.get('ce:indexed-name'),
#             'affiliation': author.get('affiliation', {}).get('@id')
#         }
#         for author in authors
#     ]

#     return processed_data


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


def scopus_research_procedures(years_to_process):
    try:
        scopus_credentials, db_credentials = get_credentials(
        )  # pylint: disable=unused-variable

        max_results_per_api_call = 5000

        csv_file = 'polyu_research_output.csv'
        existing_df = pd.DataFrame()
        try:
            existing_df = pd.read_csv(csv_file)
            if 'publication_year' not in existing_df.columns:
                print("Adding missing publication_year column to existing data")
                existing_df['publication_year'] = pd.to_datetime(
                    existing_df['prism_coverdate']).dt.year
            print(
                f"Loaded {len(existing_df)} existing records from {csv_file}")
        except FileNotFoundError:
            print(f"No existing file found at {csv_file}. Starting fresh.")
        except Exception as e:
            print(f"Error reading CSV file: {e}")

        all_new_results = []
        latest_df = existing_df.copy()  # Initialize latest_df with existing data

        for year in years_to_process:
            query = f"AFFIL(\"The Hong Kong Polytechnic University\") AND PUBYEAR = {year}"
            print(f"\nExecuting Scopus search with query: {query}")

            year_results = []
            start = 0
            while True:
                try:
                    polyu_results = scopus_search(
                        scopus_credentials, query, start=start, max_results=max_results_per_api_call)

                    if not polyu_results:
                        print(f"No more results found for {year}.")
                        break

                    year_results.extend(polyu_results)
                    start += len(polyu_results)

                    print(
                        f"Retrieved {len(polyu_results)} results for {year}. Total for {year}: {len(year_results)}")

                    if len(polyu_results) < max_results_per_api_call:
                        print(
                            f"Reached the end of available results for {year}.")
                        break

                except Exception as e:
                    print(f"Error during API call: {e}")
                    print("Saving current results and moving to next year.")
                    break

            # Process and save results for this year
            if year_results:
                try:
                    new_df = process_scopus_search_results(year_results)
                    if 'publication_year' not in new_df.columns:
                        # Ensure publication_year is added
                        new_df['publication_year'] = year

                    # Combine with existing data
                    latest_df = pd.concat(
                        [latest_df, new_df], ignore_index=True)
                    latest_df.drop_duplicates(
                        subset='dc_identifier', keep='last', inplace=True)

                    # Try to save the results to a CSV file
                    try:
                        latest_df.to_csv(csv_file, index=False)
                        print(
                            f"\nResults saved to '{csv_file}'. Total records: {len(latest_df)}")
                    except Exception as e:
                        print(f"Error saving CSV file: {e}")
                        print("Continuing with in-memory DataFrame.")

                except Exception as e:
                    print(f"Error processing results for {year}: {e}")

            all_new_results.extend(year_results)

        if not all_new_results:
            print("No new results to process across all years.")
        else:
            print(
                f"Total new results across all years: {len(all_new_results)}")

        return latest_df  # Return the latest DataFrame

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()  # Return an empty DataFrame in case of overall failure


def abstract_retrieval_procedures():
    try:
        scopus_credentials, db_credentials = get_credentials()

        # Test DOIs
        test_dois = [
            "10.1016/j.rcim.2023.102626",
            "10.1038/s41467-024-46022-3",
            "10.1038/s41586-024-07161-1",
            "10.1016/j.xinn.2024.100612",
            "10.1016/j.apcatb.2023.123312",
            "10.1016/j.apcatb.2023.123335",
            "10.1002/adma.202311970",
            "10.1109/JIOT.2024.3361173",
            "10.1016/j.engstruct.2023.117193",
            "10.1002/adma.202310918",
            "10.1002/adma.202307404",
            "10.1007/s00170-022-10767-2",
            "10.1038/s41560-023-01415-4",
            "10.1021/acsnano.3c10674",
            "10.1002/adma.202300034",
            "10.1021/jacs.3c10516",
            "10.1109/TEVC.2022.3215743",
            "10.1002/adma.202313548",
            "10.1016/j.joule.2023.12.009",
            "10.1016/j.knosys.2023.111158"
        ]

        print(f"Testing abstract retrieval for {len(test_dois)} DOIs")

        abstract_results = []
        for doi in test_dois:
            try:
                print(f"\nProcessing DOI: {doi}")
                abstract_data = abstract_retrieval(scopus_credentials, doi)
                processed_abstract = process_abstract_retrieval_results(
                    abstract_data)
                abstract_results.append(processed_abstract)
                print(
                    f"Successfully retrieved and processed abstract for DOI: {doi}")

                # Print some details of the processed abstract
                print("Abstract details:")
                print(f"Title: {processed_abstract.get('dc:title', 'N/A')}")
                print(
                    f"Publication Name: {processed_abstract.get('prism:publicationName', 'N/A')}")
                print(
                    f"Cover Date: {processed_abstract.get('prism:coverDate', 'N/A')}")
                print(
                    f"Cited by Count: {processed_abstract.get('citedby-count', 'N/A')}")
                print("---")
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
        abstract_csv_file = 'scopus_abstract_output_test.csv'
        abstract_df.to_csv(abstract_csv_file, index=False)
        print(f"\nAbstract results saved to '{abstract_csv_file}'")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

# def abstract_retrieval_procedures():
#     try:
#         scopus_credentials, db_credentials = get_credentials()

#         # Load the CSV file with DOIs
#         csv_file = 'polyu_research_output.csv'
#         try:
#             df = pd.read_csv(csv_file)
#             print(f"Loaded {len(df)} records from {csv_file}")
#         except FileNotFoundError:
#             print(
#                 f"No file found at {csv_file}. Please run scopus_research_procedures first.")
#             return
#         except Exception as e:
#             print(f"Error reading CSV file: {e}")
#             return

#         # Get unique DOIs
#         dois = df['prism_doi'].dropna().unique()
#         print(f"Found {len(dois)} unique DOIs to process")

#         abstract_results = []
#         for doi in dois:
#             try:
#                 abstract_data = abstract_retrieval(scopus_credentials, doi)
#                 processed_abstract = process_abstract_retrieval_results(
#                     abstract_data)
#                 abstract_results.append(processed_abstract)
#                 print(
#                     f"Successfully retrieved and processed abstract for DOI: {doi}")
#             except Exception as e:
#                 print(f"Error processing abstract for DOI {doi}: {e}")

#         if not abstract_results:
#             print("No abstract results to process.")
#             return

#         # Convert abstract results to DataFrame
#         abstract_df = pd.DataFrame(abstract_results)

#         # Print the first few rows of the abstract DataFrame
#         print("\nFirst few rows of the abstract DataFrame:")
#         print(abstract_df.head())

#         # Print DataFrame info
#         print("\nAbstract DataFrame info:")
#         abstract_df.info()

#         # Save abstract results to CSV
#         abstract_csv_file = 'scopus_abstract_output.csv'
#         abstract_df.to_csv(abstract_csv_file, index=False)
#         print(f"\nAbstract results saved to '{abstract_csv_file}'")

#         # Upload abstract results to database
#         try:
#             abstract_retrieval_data_uploader(
#                 db_credentials, abstract_df, table_name='scopus_abstract_output')
#             print("Abstract Retrieval Data has been uploaded to DB.")
#         except Exception as e:
#             print(f"Error uploading abstract data to database: {e}")

#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#         import traceback
#         traceback.print_exc()


def main():
    scopus_research_procedures([2022])
    # abstract_retrieval_procedures()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Application failed: {str(e)}")
        import traceback
        traceback.print_exc()
