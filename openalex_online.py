import requests
import pandas as pd
import time
import json
import sys
import os

# Keep state filename simple, as it's reset conceptually each year
STATE_FILENAME = "fetch_state_yearly.json"
DEFAULT_HEADERS = {
    # !! IMPORTANT: Replace with your email !!
    'User-Agent': 'PolyUAnalysisPortfolioProject/0.4 (mailto:bennisyiu@gmail.com)',
    'Accept': 'application/json'
}

# --- load_fetch_state, save_fetch_state remain the same ---


def load_fetch_state():
    # ... (same as before) ...
    if os.path.exists(STATE_FILENAME):
        try:
            with open(STATE_FILENAME, 'r') as f:
                state = json.load(f)
                # NEW: Also store the year the state belongs to
                if isinstance(state.get('last_successfully_saved_page'), int) and 'year' in state:
                    print(
                        f"Found state file for year {state.get('year', 'unknown')}. Resuming after page {state['last_successfully_saved_page']}.")
                    return state['last_successfully_saved_page'], state.get('year')
                else:
                    print(
                        f"Warning: State file {STATE_FILENAME} has invalid format. Starting from scratch.")
                    return 0, None
        except (json.JSONDecodeError, IOError) as e:
            print(
                f"Warning: Could not read state file {STATE_FILENAME}: {e}. Starting from scratch.")
            return 0, None
    else:
        print("No state file found. Starting fetch from the beginning (page 1).")
        return 0, None


def save_fetch_state(page_number, year):
    # ... (same as before, but include year) ...
    try:
        with open(STATE_FILENAME, 'w') as f:
            json.dump(
                {'last_successfully_saved_page': page_number, 'year': year}, f)
    except IOError as e:
        print(
            f"CRITICAL WARNING: Could not save state file {STATE_FILENAME}: {e}.")


# --- process_and_append_data remains largely the same ---
# Modified slightly to pass year for state saving
# Make sure these functions are defined elsewhere in your script:
# save_fetch_state(page_number, year)


def process_and_append_data(results_list, csv_filename, last_page_processed_in_batch, year):
    """
    Normalizes a list of OpenAlex results, extracts key fields, selects a
    comprehensive set of columns, appends the batch to a CSV file,
    updates the state file, and returns the processed DataFrame batch.

    Args:
        results_list (list): A list of dictionaries, where each dict is an OpenAlex work record.
        csv_filename (str): Path to the CSV file to append to.
        last_page_processed_in_batch (int): The page number of the last page included in this batch.
        year (int): The publication year this batch belongs to (for state saving).

    Returns:
        pandas.DataFrame: The processed DataFrame for this batch, or an empty DataFrame if errors occur
                          during critical processing steps or if the input list is empty.
    """
    if not results_list:
        print("Warning: process_and_append_data received an empty results list.")
        return pd.DataFrame()  # Return empty DataFrame if nothing to process

    print(
        f"\nProcessing {len(results_list)} results (up to page {last_page_processed_in_batch} for year {year})...")
    df_batch = pd.DataFrame()  # Initialize empty df for this batch

    try:
        # --- 1. Initial Normalization ---
        # Flatten the main structure; handles simple fields and nests simple dicts
        df_normalized = pd.json_normalize(results_list)
        print(
            f"Initial normalization resulted in {df_normalized.shape[1]} columns.")

        # --- 2. Custom Data Extraction & Simplification ---
        # These steps process complex fields (like lists of dicts) left by json_normalize

        # Extract author display names into a comma-separated string
        if 'authorships' in df_normalized.columns:
            try:
                df_normalized['author_names'] = df_normalized['authorships'].apply(
                    lambda authors: ', '.join(sorted(list(set(  # Get unique, sorted names
                        auth.get('author', {}).get('display_name', '')
                        # Check display_name exists
                        for auth in authors if isinstance(auth, dict) and auth.get('author', {}).get('display_name')
                    )))) if isinstance(authors, list) else None
                ).fillna('')  # Replace None/NaN with empty string
            except Exception as e:
                print(f"Warning: Error extracting author names: {e}")
                # Ensure column exists even on error
                df_normalized['author_names'] = ''
        else:
            # Create col if 'authorships' was missing
            df_normalized['author_names'] = ''

        # Extract institution display names from authorships
        if 'authorships' in df_normalized.columns:
            try:
                df_normalized['institution_names_str'] = df_normalized['authorships'].apply(
                    lambda authors: ', '.join(sorted(list(set(  # Get unique, sorted names
                        inst.get('display_name', '')
                        for auth in authors if isinstance(auth, dict) and isinstance(auth.get('institutions'), list)
                        # Check display_name exists
                        for inst in auth['institutions'] if isinstance(inst, dict) and inst.get('display_name')
                    )))) if isinstance(authors, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warning: Error extracting institution names: {e}")
                df_normalized['institution_names_str'] = ''
        else:
            # Create col if 'authorships' was missing
            df_normalized['institution_names_str'] = ''

        # Extract simplified OA status
        if 'open_access.oa_status' in df_normalized.columns:
            # Directly use the column if it exists after normalize, fill missing values
            df_normalized['oa_status'] = df_normalized['open_access.oa_status'].fillna(
                'unknown')
        else:
            # If the column wasn't created (e.g., open_access field missing in JSON), create it
            df_normalized['oa_status'] = 'unknown'

        # Extract primary source display name (Journal/Repo name)
        if 'primary_location.source.display_name' in df_normalized.columns:
            # Use the direct column, fill missing values
            df_normalized['source_display_name'] = df_normalized['primary_location.source.display_name'].fillna(
                '')
        # Fallback if source was missing inside primary_location
        elif 'primary_location' in df_normalized.columns:
            try:
                # Apply function to extract if source dict exists within primary_location dict
                df_normalized['source_display_name'] = df_normalized['primary_location'].apply(
                    lambda loc: loc.get('source', {}).get(
                        'display_name', '') if isinstance(loc, dict) else ''
                ).fillna('')
            except Exception as e:
                print(
                    f"Warning: Error extracting source display name via fallback: {e}")
                df_normalized['source_display_name'] = ''
        else:
            # If primary_location itself was missing
            df_normalized['source_display_name'] = ''

        # --- 3. Select Columns ---
        # Define the comprehensive list of columns you want in your final CSV
        core_columns = [
            # --- Basic Info ---
            'id',
            'doi',
            'title',
            'display_name',  # Often identical to title, uncomment if needed
            'publication_year',
            'publication_date',
            'language',
            'type',  # e.g., 'article', 'book-chapter'
            'type_crossref',  # More specific type from Crossref

            # --- Citation & Impact ---
            'cited_by_count',

            # --- Access Info ---
            'is_oa',  # Top-level Open Access flag (boolean)
            # Simplified OA status ('gold', 'green', 'hybrid', 'bronze', 'closed', 'unknown') - Extracted above
            'oa_status',
            'open_access.oa_url',  # Link to an OA version, if available

            # --- Primary Location (Journal/Repository Info) ---
            'primary_location.is_oa',  # Is the version at this specific location OA?
            'primary_location.landing_page_url',  # Link to article page
            'primary_location.pdf_url',  # Direct PDF link, if available
            'primary_location.version',  # e.g., 'publishedVersion', 'acceptedVersion'
            'primary_location.license',  # License string (e.g., 'cc-by')
            # OpenAlex ID of the source (journal, repo)
            'primary_location.source.id',
            'source_display_name',  # Cleaned source name - Extracted above

            # --- Bibliographic Info ---
            'biblio.volume',
            'biblio.issue',
            'biblio.first_page',
            'biblio.last_page',

            # --- Authorship Info ---
            'author_names',  # Comma-separated author names - Extracted above
            'institution_names_str',  # Comma-separated institution names - Extracted above

            # --- Other Flags/Metadata ---
            'is_retracted',
            'is_paratext',
            # This remains complex (list of dicts), uncomment if needed, or add specific concept extraction logic
            'concepts'
            # This remains complex (list of dicts), uncomment if needed
            'grants'

            # --- Other IDs ---
            # 'ids.openalex', # Redundant with top-level 'id'
            # 'ids.doi',      # Redundant with top-level 'doi'
            'ids.mag',      # MAG ID, if present
            'ids.pmid',     # PubMed ID, if present
            'ids.pmcid',    # PubMed Central ID, if present
        ]

        # Ensure we only try to select columns that actually exist in the DataFrame
        # Handles cases where some fields might be missing in some API responses
        existing_cols = [
            col for col in core_columns if col in df_normalized.columns]

        # Create the final DataFrame batch with only the selected columns
        # Use .copy() to avoid SettingWithCopyWarning
        df_batch = df_normalized[existing_cols].copy()

        print("Batch normalization, custom extraction, and column selection complete.")
        print("Batch DataFrame shape:", df_batch.shape)
        # Print the columns that will actually be saved for verification:
        print("Batch Columns to be saved:", df_batch.columns.tolist())

    except Exception as e:
        print(
            f"CRITICAL ERROR during batch normalization/processing for year {year}: {e}")
        # If normalization fails badly, return empty DF; state/CSV won't be updated
        return pd.DataFrame()

    # --- 4. Append to CSV ---
    try:
        # Check if file exists and is not empty to determine if header should be written
        file_exists = os.path.exists(csv_filename)
        # Write header only if file doesn't exist OR if it exists but is empty
        write_header = not file_exists or os.path.getsize(csv_filename) == 0

        print(
            f"Appending data batch to {csv_filename} (Header: {write_header})...")
        df_batch.to_csv(csv_filename, mode='a',
                        header=write_header, index=False, encoding='utf-8')
        print(f"Data batch successfully appended to {csv_filename}")

        # --- 5. Update State File ---
        # CRITICAL: Only save state *after* successfully writing the corresponding data
        # This ensures that if the script crashes between saving CSV and saving state,
        # it will re-process this batch correctly on the next run.
        save_fetch_state(last_page_processed_in_batch, year)
        # Optional print: print(f"Saved state: Successfully processed up to page {last_page_processed_in_batch} for year {year}.")

    except Exception as e:
        print(
            f"ERROR appending batch to CSV or saving state for year {year}: {e}")
        print("State file will NOT be updated for this batch to ensure reprocessing.")
        # Even if saving fails, return the processed batch DF; maybe it can be used elsewhere
        # Or consider returning pd.DataFrame() if you want to signal the save failure more strongly
        return df_batch

    # Return the processed DataFrame for this batch
    return df_batch


# --- Main function modified to loop through years ---
def fetch_data_for_years(base_filter, years_to_fetch, csv_filename, pages_per_pause=10, pause_duration_min=5):
    """
    Fetches data for a list of years, appending to a single CSV and handling resumes per year.
    """
    print(f"Starting yearly data fetch for years: {years_to_fetch}")
    print(f"Appending data to: {csv_filename}")
    print(f"State file: {STATE_FILENAME}")
    print("INFO: Using headers. Ensure 'your_email@example.com' is replaced in DEFAULT_HEADERS.")

    # Load initial state once
    last_saved_page_from_file, state_year = load_fetch_state()

    for year in years_to_fetch:
        print(f"\n===== Processing Year: {year} =====")

        # --- Determine starting page for THIS year ---
        start_page = 1
        # Check if the saved state belongs to the current year we are processing
        if state_year == year:
            start_page = last_saved_page_from_file + 1
            print(
                f"Resuming year {year} after page {last_saved_page_from_file}.")
        else:
            # If state is for a different year or no state, start year from page 1
            print(f"Starting year {year} from page 1.")
            # Reset conceptual state for the new year
            last_saved_page_for_current_year = 0

        page = start_page
        current_run_results = []
        per_page = 200
        pause_duration_sec = pause_duration_min * 60

        # Construct API URL for the current year
        yearly_api_url = f"https://api.openalex.org/works?filter={base_filter},publication_year:{year}&sort=publication_date:desc"
        print(f"API URL for year {year}: {yearly_api_url}")

        try:
            while True:
                # This inner loop fetches pages for the *current year*
                paginated_url = f"{yearly_api_url}&page={page}&per_page={per_page}"
                print(f"Fetching page {page} for year {year}...")

                try:
                    response = requests.get(
                        paginated_url, headers=DEFAULT_HEADERS, timeout=90)
                    response.raise_for_status()

                    if not response.content:
                        break  # Empty response
                    try:
                        data = response.json()
                    except json.JSONDecodeError:
                        print(
                            f"Error: JSON decode failed page {page}, year {year}. Skipping page.")
                        # Decide: break year? break script? or just skip page? Skipping page for now.
                        page += 1
                        continue

                    results = data.get('results', [])
                    if not results:
                        print(
                            f"No more results found for year {year} on page {page}.")
                        break  # End of data for this year

                    current_run_results.extend(results)
                    print(
                        f"Fetched {len(results)}. Batch size: {len(current_run_results)}.")

                    # --- Pause/Save Logic (within year loop) ---
                    if page % pages_per_pause == 0 and current_run_results:
                        print(
                            f"\n--- Checkpoint page {page}, year {year}. Processing batch. ---")
                        processed_batch_df = process_and_append_data(
                            current_run_results, csv_filename, page, year)
                        # Update the conceptual state for this year if save was successful
                        if not processed_batch_df.empty:  # Simple check if processing returned something
                            last_saved_page_for_current_year = page
                        current_run_results = []  # Clear batch
                        print(
                            f"--- Pausing for {pause_duration_min} minutes... ---")
                        time.sleep(pause_duration_sec)
                        print("--- Resuming fetch ---")

                    page += 1
                    time.sleep(0.25)

                except requests.exceptions.Timeout:
                    print(f"Timeout page {page}, year {year}. Retrying...")
                    time.sleep(20)
                    continue
                except requests.exceptions.RequestException as e:
                    print(f"\nNETWORK ERROR page {page}, year {year}: {e}")
                    print("Stopping fetch FOR THIS YEAR. Will process remaining batch.")
                    break  # Stop fetching for this year

            # --- End of inner while loop (for the current year) ---

        except KeyboardInterrupt:
            print("\n--- KeyboardInterrupt! Stopping fetch. ---")
            # Process remaining batch for the current year before exiting the script
            if current_run_results:
                print(f"--- Processing final batch for year {year}... ---")
                final_page_processed = page - 1
                if final_page_processed >= start_page:
                    process_and_append_data(
                        current_run_results, csv_filename, final_page_processed, year)
            print("Exiting script.")
            sys.exit(0)  # Clean exit

        except Exception as e:
            print(f"\nUNEXPECTED ERROR during year {year} (page {page}): {e}")
            print("Stopping fetch FOR THIS YEAR. Will process remaining batch.")
            # Break out of the while loop for this year

        # --- Process final batch for the current year (if any results left) ---
        if current_run_results:
            print(
                f"--- Processing final remaining batch ({len(current_run_results)}) for year {year}... ---")
            final_page_processed = page - 1
            if final_page_processed >= start_page:  # Check if we actually processed pages
                process_and_append_data(
                    current_run_results, csv_filename, final_page_processed, year)
                last_saved_page_for_current_year = final_page_processed  # Update conceptual state

        # --- Important: Update state_year for the next iteration of the outer loop ---
        # If we successfully processed this year, the next year should start fresh unless interrupted
        # Or, if the script finished all years, clear state? Or leave state for the last page of last year?
        # Let's update state_year to the year just completed IF it finished without error breaking the outer loop.
        # The state file itself is updated inside process_and_append_data.
        state_year = year  # Mark that the state file now potentially refers to this year

        print(f"===== Finished processing Year: {year} =====")
        # Optional short pause between years
        # time.sleep(5)

    # --- End of outer for loop (all years processed) ---

    print("\n--- All specified years processed. Loading final DataFrame from CSV... ---")
    final_df = pd.DataFrame()
    if os.path.exists(csv_filename):
        try:
            final_df = pd.read_csv(csv_filename, encoding='utf-8')
            if 'id' in final_df.columns:
                initial_rows = len(final_df)
                final_df = final_df.drop_duplicates(subset='id', keep='last')
                if len(final_df) < initial_rows:
                    print(
                        f"NOTE: Removed {initial_rows - len(final_df)} duplicate rows.")
            print(
                f"Successfully loaded final DataFrame with {len(final_df)} rows.")
        except Exception as e:
            print(f"ERROR: Failed to read final CSV {csv_filename}: {e}")
            final_df = pd.DataFrame()
    else:
        print("WARNING: Output CSV file does not exist.")

    return final_df


# --- How to Use (Option 2) ---
if __name__ == "__main__":
    # Define the base filter part (without the year)
    institution_filter = "institutions.id:https://openalex.org/I14243506"

    # Define the list of years you want to fetch
    # Start with just one year again to test the smaller batch size
    years = [2024]

    output_csv = "polyu_publications_yearly_accumulated.csv"

    # Call the new function
    final_dataframe = fetch_data_for_years(
        base_filter=institution_filter,
        years_to_fetch=years,
        csv_filename=output_csv,
        # --- REDUCED BATCH SIZE ---
        # Process 4 pages (4 * 200 = 800 results) before saving/pausing
        pages_per_pause=4,
        # --------------------------
        pause_duration_min=1  # Keep the pause duration
    )

    # --- Display final results ---
    print("\n--- Script Finished ---")
    # ...(rest of the script remains the same)...
