import pyalex
from pyalex import Works
import pandas as pd
import time
import csv
import os
import json  # For converting complex fields to strings
import requests  # For handling potential request exceptions
import numpy as np  # For handling potential NaN values
import re  # For regular expression replacement
import traceback  # For detailed error printing

# --- Configuration ---

# Optional: Set your email for the OpenAlex polite pool
# !! IMPORTANT: Replace with your email !!
pyalex.config.email = "bennis.yiu@connect.polyu.hk"

# --- Input & Output Files ---
# For full run:
# <-- ADJUST RELATIVE PATH IF NEEDED
INPUT_CSV_FILE = 'extracted_data/raw_copy/scopus_search_polyu_publications_2020_2025.csv'
# For testing: Set this to None
# INPUT_CSV_FILE = None

# Column name in your input CSV containing DOIs like '10.xxx/...'
PRISM_DOI_COLUMN = 'prism_doi'
OUTPUT_CSV_FILE = 'openalex_enriched_combined_data_refined.csv'  # Output file name

# --- Control Parameters ---
BATCH_SIZE = 200            # Process and save every N DOIs
# Politeness delay in seconds after each API call (~6-7 req/sec)
SLEEP_TIME_PER_DOI = 0.15
SLEEP_TIME_AFTER_BATCH = 60  # Seconds to pause after saving a batch (1 minute)
RETRY_SLEEP_TIME = 15       # Seconds to wait after a non-fatal error before continuing

# --- Define fields to extract based PRECISELY on the provided JSON sample ---
# These will be ADDED to the original DataFrame columns, prefixed with 'oa_'
OPENALEX_FIELDS_TO_EXTRACT = [
    "id", "doi", "title", "display_name", "publication_year", "publication_date",
    "language", "type", "cited_by_count", "fwci", "is_retracted",
    "updated_date", "created_date",
    # Nested Dictionaries (will be flattened into specific columns based on sample)
    "primary_location",
    "biblio",
    "primary_topic",
    "citation_normalized_percentile",
    "cited_by_percentile_year",
    # Lists of Dictionaries (will be stored as JSON strings based on sample)
    "authorships",
    "grants",
    "counts_by_year",
]


# --- Function Definitions ---

def format_doi(p_doi):
    """Cleans and formats a DOI string to the standard https://doi.org/ structure."""
    if pd.isna(p_doi) or str(p_doi).strip() == '':
        return None
    p_doi_str = str(p_doi).strip()
    p_doi_str = re.sub(r'^https?://doi.org/', '',
                       p_doi_str, flags=re.IGNORECASE)
    if '/' not in p_doi_str or not p_doi_str.startswith('10.'):
        return None
    return f"https://doi.org/{p_doi_str.lower()}"


def load_and_prepare_input_df(filepath, prism_doi_col):
    """Loads CSV, verifies columns, and creates the formatted 'doi' column."""
    if not filepath or not os.path.exists(filepath):
        print(
            f"Info: Input file path not provided or not found ('{filepath}').")
        return None
    try:
        df = pd.read_csv(filepath, low_memory=False)
        print(f"Loaded DataFrame from '{filepath}' with shape {df.shape}")
        required_cols = [prism_doi_col]
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            print(f"Error: Input CSV missing required columns: {missing}")
            return None

        print(f"Creating formatted 'doi' column from '{prism_doi_col}'...")
        df['doi'] = df[prism_doi_col].apply(format_doi)

        valid_doi_count = df['doi'].notna().sum()
        print(f"Formatted {valid_doi_count} valid DOIs.")
        print(
            f"Note: {df['doi'].isna().sum()} rows have missing or invalid '{prism_doi_col}'. They will be skipped.")
        return df

    except Exception as e:
        print(
            f"Error loading or preparing input DataFrame from {filepath}: {e}")
        traceback.print_exc()
        return None


def get_processed_dois(csv_filename, doi_col='doi'):
    """Reads the formatted 'doi' column from the output CSV."""
    processed = set()
    if not os.path.exists(csv_filename):
        return processed
    try:
        df_out = pd.read_csv(csv_filename, usecols=[doi_col], low_memory=False)
        processed = set(df_out[doi_col].dropna().astype(str))
        print(
            f"Found {len(processed)} DOIs already processed in '{csv_filename}'.")
    except Exception as e:
        print(
            f"Warning: Error reading processed DOIs from '{csv_filename}': {e}. May re-process some DOIs.")
    return processed


def write_batch_to_csv(data_list, csv_filename, fieldnames, write_header):
    """Appends a batch of combined results (list of dicts) to a CSV file."""
    if not data_list:
        return False
    try:
        records_to_write = [
            {field: record.get(field) for field in fieldnames} for record in data_list]
        df_batch = pd.DataFrame(records_to_write)
        for col in df_batch.columns:
            if df_batch[col].apply(lambda x: isinstance(x, (list, dict))).any():
                try:
                    df_batch[col] = df_batch[col].apply(lambda x: json.dumps(
                        x, default=str) if isinstance(x, (list, dict)) else x)
                except Exception as json_e:
                    print(
                        f"Warning: JSON dump failed for column {col}. Storing as string. Error: {json_e}")
                    df_batch[col] = df_batch[col].astype(str)
        df_batch.to_csv(csv_filename, mode='a', header=write_header,
                        index=False, columns=fieldnames, encoding='utf-8-sig')
        print(
            f"Successfully appended {len(data_list)} records to {csv_filename}")
        return True
    except Exception as e:
        print(f"ERROR writing batch to CSV {csv_filename}: {e}")
        traceback.print_exc()
        return False


def extract_openalex_work_data(work):
    """Extracts predefined fields from a single OpenAlex Work object based on the sample JSON."""
    extracted = {}
    if not isinstance(work, dict):
        return extracted

    # --- Direct Fields from Sample ---
    direct_fields = [
        "id", "doi", "title", "display_name", "publication_year", "publication_date",
        "language", "type", "cited_by_count", "fwci", "is_retracted",
        "updated_date", "created_date"
    ]
    for field in direct_fields:
        if field in OPENALEX_FIELDS_TO_EXTRACT:  # Check if field is expected
            extracted[f"oa_{field}"] = work.get(field)

    # --- Nested Dictionaries (Extract only fields seen in sample) ---
    if "primary_location" in OPENALEX_FIELDS_TO_EXTRACT:
        loc = work.get('primary_location', {})
        if isinstance(loc, dict):
            extracted['oa_primary_location_is_oa'] = loc.get('is_oa')
            extracted['oa_primary_location_landing_page_url'] = loc.get(
                'landing_page_url')
            # Note: pdf_url, version, license were NOT in the sample, so not extracted here
            src = loc.get('source', {})
            if isinstance(src, dict):
                # Extract only source fields present in the sample
                extracted['oa_primary_location_source_id'] = src.get('id')
                extracted['oa_primary_location_source_name'] = src.get(
                    'display_name')
                extracted['oa_primary_location_source_issn_l'] = src.get(
                    'issn_l')
                extracted['oa_primary_location_source_is_oa'] = src.get(
                    'is_oa')
                extracted['oa_primary_location_source_is_indexed_in_scopus'] = src.get(
                    'is_indexed_in_scopus')
                extracted['oa_primary_location_source_host_org_name'] = src.get(
                    'host_organization_name')
                extracted['oa_primary_location_source_host_org_lineage_names'] = src.get(
                    'host_organization_lineage_names')  # List
                extracted['oa_primary_location_source_type'] = src.get('type')
                # Note: issn (list) was NOT in the sample source object

    if "biblio" in OPENALEX_FIELDS_TO_EXTRACT:
        bib = work.get('biblio', {})
        if isinstance(bib, dict):
            extracted['oa_biblio_volume'] = bib.get('volume')
            extracted['oa_biblio_issue'] = bib.get('issue')
            extracted['oa_biblio_first_page'] = bib.get('first_page')
            extracted['oa_biblio_last_page'] = bib.get('last_page')

    if "primary_topic" in OPENALEX_FIELDS_TO_EXTRACT:
        top = work.get('primary_topic', {})
        if isinstance(top, dict):
            # Extract only fields present in sample topic object
            # Although ID wasn't in sample, it's useful
            extracted['oa_primary_topic_id'] = top.get('id')
            extracted['oa_primary_topic_name'] = top.get('display_name')
            extracted['oa_primary_topic_score'] = top.get('score')
            # Extract only display names for subfield, field, domain as per sample
            extracted['oa_primary_topic_subfield_name'] = top.get(
                'subfield', {}).get('display_name')
            extracted['oa_primary_topic_field_name'] = top.get(
                'field', {}).get('display_name')
            extracted['oa_primary_topic_domain_name'] = top.get(
                'domain', {}).get('display_name')

    if "citation_normalized_percentile" in OPENALEX_FIELDS_TO_EXTRACT:
        cnp = work.get('citation_normalized_percentile', {})
        if isinstance(cnp, dict):
            extracted['oa_cnp_value'] = cnp.get('value')
            extracted['oa_cnp_is_top_1_percent'] = cnp.get(
                'is_in_top_1_percent')
            extracted['oa_cnp_is_top_10_percent'] = cnp.get(
                'is_in_top_10_percent')

    if "cited_by_percentile_year" in OPENALEX_FIELDS_TO_EXTRACT:
        cbpy = work.get('cited_by_percentile_year', {})
        if isinstance(cbpy, dict):
            extracted['oa_cbpy_min'] = cbpy.get('min')
            extracted['oa_cbpy_max'] = cbpy.get('max')

    # --- Lists of Dictionaries (Extract only lists present in sample) ---
    list_fields = ["authorships", "grants", "counts_by_year"]
    for field in list_fields:
        if field in OPENALEX_FIELDS_TO_EXTRACT:
            value = work.get(field, [])
            # We store the list directly; it will be stringified later.
            # The internal structure of the dicts within the list is preserved.
            extracted[f"oa_{field}"] = value

    return extracted


def get_all_fieldnames(original_columns, sample_oa_extraction_dict):
    """Generates a complete list of fieldnames for the output CSV based on extracted fields."""
    fieldnames = list(original_columns)
    if 'doi' not in fieldnames and PRISM_DOI_COLUMN in fieldnames:
        fieldnames.insert(fieldnames.index(PRISM_DOI_COLUMN) + 1, 'doi')
    if 'oa_status' not in fieldnames and 'doi' in fieldnames:
        fieldnames.insert(fieldnames.index('doi') + 1, 'oa_status')
    elif 'oa_status' not in fieldnames:
        fieldnames.append('oa_status')

    oa_keys = list(sample_oa_extraction_dict.keys())
    for key in oa_keys:
        if key not in fieldnames:
            fieldnames.append(key)

    # No need to sort here, keep original + oa field order generally
    return fieldnames


def fetch_and_process_data(input_df, output_csv_filename, batch_size=200, sleep_per_doi=0.15, sleep_after_batch=60):
    """Main function to fetch, process, combine, and save OpenAlex data."""
    original_columns = input_df.columns.tolist()
    if 'doi' not in original_columns:
        print(
            "Error: 'doi' column was not successfully created in input DataFrame. Exiting.")
        return

    all_output_fieldnames = []
    header_determined = False

    processed_dois_set = get_processed_dois(output_csv_filename, doi_col='doi')
    df_to_process = input_df[
        input_df['doi'].notna() &
        ~input_df['doi'].isin(processed_dois_set)
    ].copy()

    if df_to_process.empty:
        print("All valid DOIs from the input DataFrame are already processed or none were valid.")
        return

    print(
        f"Need to process {len(df_to_process)} new rows with valid, unprocessed DOIs.")
    results_this_run = []
    batch_results = []
    processed_count_this_run = 0
    total_new_to_process = len(df_to_process)
    write_header = not os.path.exists(
        output_csv_filename) or os.path.getsize(output_csv_filename) == 0

    print(f"Starting OpenAlex queries for {total_new_to_process} DOIs...")

    for index, row in df_to_process.iterrows():
        current_progress = processed_count_this_run + 1
        formatted_doi = row['doi']
        print(
            f"\n--- Processing Row Index {index} ({current_progress}/{total_new_to_process}), DOI: {formatted_doi} ---")

        combined_data = row.to_dict()
        for key, value in combined_data.items():
            if pd.isna(value):
                combined_data[key] = None
        combined_data["oa_status"] = "Processing Error - Unknown"

        try:
            work = Works()[formatted_doi]  # API call

            if not work or not isinstance(work, dict):
                combined_data["oa_status"] = "DOI Not Found (pyalex)"
                print(f"  DOI not found in OpenAlex: {formatted_doi}")
            else:
                # Call the REFINED extraction function
                openalex_data = extract_openalex_work_data(work)
                combined_data.update(openalex_data)
                combined_data["oa_status"] = "Success"
                print(
                    f"  Successfully processed: {combined_data.get('oa_title', 'N/A')[:70]}...")

                if not header_determined:
                    all_output_fieldnames = get_all_fieldnames(
                        original_columns, openalex_data)
                    header_determined = True
                    print(
                        f"Determined output CSV columns ({len(all_output_fieldnames)})")
                    if os.path.exists(output_csv_filename) and os.path.getsize(output_csv_filename) > 0:
                        write_header = False
                    else:
                        write_header = True

            batch_results.append(combined_data)
            processed_count_this_run += 1
            time.sleep(sleep_per_doi)

        # --- Error Handling ---
        # except pyalex.api.NotFound:
        #     combined_data["oa_status"] = "DOI Not Found (pyalex API)"
        #     print(f"  DOI not found via pyalex API: {formatted_doi}")
        #     batch_results.append(combined_data)
        #     processed_count_this_run += 1
        #     time.sleep(sleep_per_doi)
        # except pyalex.api.RateLimitError as e:
        #     print(
        #         f"  Rate limit error for DOI {formatted_doi}: {e}. Pausing for 60s...")
        #     time.sleep(60)
        #     combined_data["oa_status"] = "Rate Limit Error - Skipped"
        #     batch_results.append(combined_data)
        #     processed_count_this_run += 1
        # except requests.exceptions.RequestException as e:
        #     print(
        #         f"  Network error for DOI {formatted_doi}: {e}. Pausing {RETRY_SLEEP_TIME}s...")
        #     time.sleep(RETRY_SLEEP_TIME)
        #     combined_data["oa_status"] = f"Network Error - Skipped: {e}"
        #     batch_results.append(combined_data)
        #     processed_count_this_run += 1
        # except Exception as e:
        #     print(f"  UNEXPECTED ERROR processing DOI {formatted_doi}: {e}")
        #     traceback.print_exc()
        #     combined_data["oa_status"] = f"Processing Error - Skipped: {e}"
        #     batch_results.append(combined_data)
        #     processed_count_this_run += 1
        #     time.sleep(RETRY_SLEEP_TIME)

        # Catch specific HTTP errors first, especially 404 Not Found
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # This is the expected "Not Found" case for DOI lookups
                combined_data["oa_status"] = "DOI Not Found (API 404)"
                print(
                    f"  DOI not found via OpenAlex API (404): {formatted_doi}")
                batch_results.append(combined_data)
                processed_count_this_run += 1
                time.sleep(sleep_per_doi)  # Standard pause even for not found
            else:
                # Handle other HTTP errors (e.g., 403 Forbidden, 500 Server Error)
                print(
                    f"  HTTP error for DOI {formatted_doi}: {e}. Pausing {RETRY_SLEEP_TIME}s...")
                combined_data["oa_status"] = f"HTTP Error {e.response.status_code} - Skipped: {e}"
                batch_results.append(combined_data)
                processed_count_this_run += 1
                # Pause longer for unexpected HTTP errors
                time.sleep(RETRY_SLEEP_TIME)

        # Then catch other specific exceptions if needed
        except pyalex.api.RateLimitError as e:  # Keep this one as it's specific
            print(
                f"  Rate limit error for DOI {formatted_doi}: {e}. Pausing for 60s...")
            time.sleep(60)
            combined_data["oa_status"] = "Rate Limit Error - Skipped"
            batch_results.append(combined_data)
            processed_count_this_run += 1  # Count as processed (skipped)
        except requests.exceptions.RequestException as e:  # Catch general network issues
            print(
                f"  Network error for DOI {formatted_doi}: {e}. Pausing {RETRY_SLEEP_TIME}s...")
            time.sleep(RETRY_SLEEP_TIME)
            combined_data["oa_status"] = f"Network Error - Skipped: {e}"
            batch_results.append(combined_data)
            processed_count_this_run += 1
        except Exception as e:  # Catch any other unexpected errors
            print(f"  UNEXPECTED ERROR processing DOI {formatted_doi}: {e}")
            traceback.print_exc()
            combined_data["oa_status"] = f"Processing Error - Skipped: {e}"
            batch_results.append(combined_data)
            processed_count_this_run += 1
            time.sleep(RETRY_SLEEP_TIME)

        # --- Batch Saving Logic ---
        if len(batch_results) >= batch_size or current_progress == total_new_to_process:
            if not header_determined and batch_results:
                all_output_fieldnames = list(
                    original_columns) + ['doi', 'oa_status']
                print(
                    "Warning: Header determined from batch with errors/not found records.")

            print(
                f"\n--- Saving batch of {len(batch_results)} results (Processed {processed_count_this_run} total in run) ---")
            if not all_output_fieldnames:
                print(
                    "Error: Could not determine fieldnames for CSV header. Skipping save.")
                # Attempt to get fieldnames from the first record in the current batch as a last resort
                if batch_results:
                    try:
                        all_output_fieldnames = list(batch_results[0].keys())
                        print(
                            f"Recovered fieldnames from first batch record: {all_output_fieldnames}")
                    except:
                        print("Could not recover fieldnames. Saving failed.")
                        continue  # Skip saving this batch if header fails completely
                else:
                    continue  # Skip if batch is empty and header unknown

            write_success = write_batch_to_csv(
                batch_results, output_csv_filename, all_output_fieldnames, write_header)
            if write_success:
                results_this_run.extend(batch_results)
                batch_results = []
                write_header = False
                if current_progress < total_new_to_process:
                    print(
                        f"--- Pausing for {sleep_after_batch} seconds... ---")
                    time.sleep(sleep_after_batch)
            else:
                print("!!! CRITICAL: Failed to write batch. Stopping. !!!")
                return results_this_run

    print("\n--- Finished Processing All New DOIs ---")
    return results_this_run


# --- Main execution ---
if __name__ == "__main__":

    # --- Option 1: Load from CSV file (for full run) ---
    print("Attempting to load data from CSV...")
    input_dataframe = load_and_prepare_input_df(
        INPUT_CSV_FILE, PRISM_DOI_COLUMN)

    # --- Option 2: Create a small test DataFrame (for testing) ---
    if input_dataframe is None:
        print(
            "\n--- Input file not loaded or path set to None, creating TEST DataFrame ---")
        test_data = {
            'prism_url': ['url1', 'url2', 'url3', 'url4', 'url5', 'url6_nodoi', 'url7_baddoi'],
            'dc_identifier': ['scopus1', 'scopus2', 'scopus3', 'scopus4', 'scopus5', 'scopus6', 'scopus7'],
            'prism_publicationname': ['Journal A', 'Journal B', 'Journal C', 'Journal D', 'Journal E', 'Journal F', 'Journal G'],
            'prism_coverdate': ['2024-01-01', '2024-02-01', '2023-11-01', '2024-03-01', '2023-06-01', '2023-01-01', '2023-02-01'],
            PRISM_DOI_COLUMN: [
                "10.1177/10963480241229235", "10.1002/adfm.202413884",
                "10.1109/TNNLS.2023.3336563", "10.1016/j.esci.2024.100281",
                "10.1109/TEVC.2023.3278132", None, "invalid-doi-for-testing"
            ],
            'citedby_count': [5, 10, 15, 2, 8, 3, 1], 'subtype': ['ar']*7,
            'subtypedescription': ['Article']*7, 'publication_year': [2024, 2024, 2023, 2024, 2023, 2023, 2023],
            'publication_month': [1, 2, 11, 3, 6, 1, 2]
        }
        input_dataframe = pd.DataFrame(test_data)

        # Apply DOI formatting DIRECTLY to the test DataFrame
        print(
            f"Formatting 'doi' column for TEST DataFrame from '{PRISM_DOI_COLUMN}'...")
        input_dataframe['doi'] = input_dataframe[PRISM_DOI_COLUMN].apply(
            format_doi)  # Use the corrected function

        print(f"Test DataFrame prepared with {input_dataframe.shape[0]} rows.")
        if 'doi' in input_dataframe.columns:
            print(input_dataframe[[PRISM_DOI_COLUMN, 'doi']].head(
                10).to_string())
        else:
            print("Warning: 'doi' column not created in test DataFrame.")

    # --- Proceed if DataFrame is ready ---
    if input_dataframe is not None:
        # --- Call the main processing function ---
        retrieved_data_this_run = fetch_and_process_data(
            input_df=input_dataframe,
            output_csv_filename=OUTPUT_CSV_FILE,
            batch_size=BATCH_SIZE,
            sleep_per_doi=SLEEP_TIME_PER_DOI,
            sleep_after_batch=SLEEP_TIME_AFTER_BATCH
        )

        # --- Final Summary ---
        print(f"\n--- Script Summary ---")
        # Estimate processed count (may not be perfectly accurate if script stopped mid-batch)
        processed_estimate = len(
            retrieved_data_this_run) if 'retrieved_data_this_run' in locals() else 'N/A'
        print(
            f"Attempted processing for approx {processed_estimate} DOIs/Rows in this execution run.")
        final_total = 0
        if os.path.exists(OUTPUT_CSV_FILE):
            try:
                # Efficiently count rows using pandas chunking or just header check
                final_total = pd.read_csv(OUTPUT_CSV_FILE, usecols=[
                                          PRISM_DOI_COLUMN], low_memory=False).shape[0]
            except Exception as e:
                print(f"Could not read final row count from CSV: {e}")

        print(
            f"Total records (excluding header) currently estimated in '{OUTPUT_CSV_FILE}': {final_total}")
        print(f"Data saved to: {OUTPUT_CSV_FILE}")
    else:
        print("Could not load or prepare input DataFrame. Exiting.")
