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
    Normalizes a list of OpenAlex results from the API, extracts key fields
    comprehensively, selects the desired columns, appends the batch to a CSV file,
    updates the state file, and returns the processed DataFrame batch.
    (This version mirrors the logic from normalize_local_json)

    Args:
        results_list (list): A list of dictionaries (OpenAlex work records).
        csv_filename (str): Path to the CSV file to append to.
        last_page_processed_in_batch (int): The page number of the last page in this batch.
        year (int): The publication year this batch belongs to.

    Returns:
        pandas.DataFrame: Processed DataFrame for this batch, or empty if errors.
    """
    if not results_list:
        print("Warning: process_and_append_data received an empty results list.")
        return pd.DataFrame()

    print(
        f"\nProcessing {len(results_list)} results (up to page {last_page_processed_in_batch} for year {year})...")
    df_batch = pd.DataFrame()  # Initialize final DataFrame for this batch

    try:
        # --- 1. Initial Normalization ---
        df_normalized = pd.json_normalize(results_list)
        print(
            f"Initial normalization resulted in {df_normalized.shape[1]} columns.")

        # --- 2. Custom Data Extraction & Simplification ---
        # --- Authorships ---
        if 'authorships' in df_normalized.columns and not df_normalized['authorships'].isnull().all():
            try:
                df_normalized['author_names'] = df_normalized['authorships'].apply(
                    lambda authors: '; '.join(sorted(list(set(
                        auth.get('author', {}).get('display_name', '')
                        for auth in authors if isinstance(auth, dict) and auth.get('author', {}).get('display_name')
                    )))) if isinstance(authors, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warn: Author names error: {e}")
                df_normalized['author_names'] = ''
            try:
                df_normalized['author_orcids'] = df_normalized['authorships'].apply(
                    lambda authors: '; '.join(sorted(list(set(
                        auth.get('author', {}).get('orcid', '') or ''
                        for auth in authors if isinstance(auth, dict) and auth.get('author') and auth.get('author').get('orcid')
                    )))) if isinstance(authors, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warn: Author ORCIDs error: {e}")
                df_normalized['author_orcids'] = ''
            try:
                df_normalized['institution_names_str'] = df_normalized['authorships'].apply(
                    lambda authors: '; '.join(sorted(list(set(
                        inst.get('display_name', '')
                        for auth in authors if isinstance(auth, dict) and isinstance(auth.get('institutions'), list)
                        for inst in auth['institutions'] if isinstance(inst, dict) and inst.get('display_name')
                    )))) if isinstance(authors, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warn: Institution names error: {e}")
                df_normalized['institution_names_str'] = ''
            try:
                df_normalized['institution_countries'] = df_normalized['authorships'].apply(
                    lambda authors: '; '.join(sorted(list(set(
                        inst.get('country_code', '')
                        for auth in authors if isinstance(auth, dict) and isinstance(auth.get('institutions'), list)
                        for inst in auth['institutions'] if isinstance(inst, dict) and inst.get('country_code')
                    )))) if isinstance(authors, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warn: Institution countries error: {e}")
                df_normalized['institution_countries'] = ''
            try:
                df_normalized['corresponding_author_ids'] = df_normalized['authorships'].apply(
                    lambda authors: '; '.join(sorted(list(set(
                        auth.get('author', {}).get('id', '')
                        for auth in authors if isinstance(auth, dict) and auth.get('is_corresponding') and auth.get('author', {}).get('id')
                    )))) if isinstance(authors, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warn: Corresponding author IDs error: {e}")
                df_normalized['corresponding_author_ids'] = ''
        else:
            df_normalized['author_names'] = ''
            df_normalized['author_orcids'] = ''
            df_normalized['institution_names_str'] = ''
            df_normalized['institution_countries'] = ''
            df_normalized['corresponding_author_ids'] = ''

        # --- Primary Location / Source ---
        if 'primary_location' in df_normalized.columns and not df_normalized['primary_location'].isnull().all():
            if 'primary_location.source.display_name' in df_normalized.columns:
                df_normalized['source_display_name'] = df_normalized['primary_location.source.display_name'].fillna(
                    '')
            else:
                try:
                    df_normalized['source_display_name'] = df_normalized['primary_location'].apply(lambda loc: loc.get(
                        'source', {}).get('display_name', '') if isinstance(loc, dict) else '').fillna('')
                except Exception as e:
                    print(f"Warn: Source name (fallback) error: {e}")
                    df_normalized['source_display_name'] = ''
        else:
            df_normalized['source_display_name'] = ''
        if 'primary_location.source.id' not in df_normalized.columns:
            df_normalized['primary_location.source.id'] = None
        if 'primary_location.source.issn_l' not in df_normalized.columns:
            df_normalized['primary_location.source.issn_l'] = None
        if 'primary_location.landing_page_url' not in df_normalized.columns:
            df_normalized['primary_location.landing_page_url'] = None
        if 'primary_location.pdf_url' not in df_normalized.columns:
            df_normalized['primary_location.pdf_url'] = None
        if 'primary_location.version' not in df_normalized.columns:
            df_normalized['primary_location.version'] = None
        if 'primary_location.license' not in df_normalized.columns:
            df_normalized['primary_location.license'] = None
        if 'primary_location.is_oa' not in df_normalized.columns:
            df_normalized['primary_location.is_oa'] = None

        # --- Open Access ---
        if 'open_access.oa_status' in df_normalized.columns:
            df_normalized['oa_status'] = df_normalized['open_access.oa_status'].fillna(
                'unknown')
        else:
            df_normalized['oa_status'] = 'unknown'
        if 'open_access.oa_url' not in df_normalized.columns:
            df_normalized['open_access.oa_url'] = None

        # --- Topics ---
        if 'topics' in df_normalized.columns and not df_normalized['topics'].isnull().all():
            if 'primary_topic.display_name' in df_normalized.columns:
                df_normalized['primary_topic_name'] = df_normalized['primary_topic.display_name'].fillna(
                    '')
            else:
                df_normalized['primary_topic_name'] = ''
            try:
                df_normalized['all_topic_names'] = df_normalized['topics'].apply(
                    lambda topics: '; '.join(sorted(list(set(
                        topic.get('display_name', '')
                        for topic in topics if isinstance(topic, dict) and topic.get('display_name')
                    )))) if isinstance(topics, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warn: All topic names error: {e}")
                df_normalized['all_topic_names'] = ''
        else:
            df_normalized['primary_topic_name'] = ''
            df_normalized['all_topic_names'] = ''

        # --- Concepts ---
        if 'concepts' in df_normalized.columns and not df_normalized['concepts'].isnull().all():
            try:
                df_normalized['concept_names_level0'] = df_normalized['concepts'].apply(
                    lambda concepts: '; '.join(sorted(list(set(
                        concept.get('display_name', '')
                        for concept in concepts if isinstance(concept, dict) and concept.get('level') == 0 and concept.get('display_name')
                    )))) if isinstance(concepts, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warn: Concepts (L0) error: {e}")
                df_normalized['concept_names_level0'] = ''
        else:
            df_normalized['concept_names_level0'] = ''

        # --- Keywords ---
        if 'keywords' in df_normalized.columns and not df_normalized['keywords'].isnull().all():
            try:
                df_normalized['keywords_str'] = df_normalized['keywords'].apply(
                    lambda keywords: '; '.join(sorted(list(set(
                        kw.get('display_name', '')
                        for kw in keywords if isinstance(kw, dict) and kw.get('display_name')
                    )))) if isinstance(keywords, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warn: Keywords error: {e}")
                df_normalized['keywords_str'] = ''
        else:
            df_normalized['keywords_str'] = ''

        # --- Grants ---
        if 'grants' in df_normalized.columns and not df_normalized['grants'].isnull().all():
            try:
                df_normalized['funder_names'] = df_normalized['grants'].apply(
                    lambda grants: '; '.join(sorted(list(set(
                        g.get('funder_display_name', '')
                        for g in grants if isinstance(g, dict) and g.get('funder_display_name')
                    )))) if isinstance(grants, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warn: Funder names error: {e}")
                df_normalized['funder_names'] = ''
            try:
                df_normalized['award_ids'] = df_normalized['grants'].apply(
                    lambda grants: '; '.join(sorted(list(set(
                        g.get('award_id', '') or ''
                        for g in grants if isinstance(g, dict) and g.get('award_id')
                    )))) if isinstance(grants, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warn: Award IDs error: {e}")
                df_normalized['award_ids'] = ''
        else:
            df_normalized['funder_names'] = ''
            df_normalized['award_ids'] = ''

        # --- Sustainable Development Goals ---
        if 'sustainable_development_goals' in df_normalized.columns and not df_normalized['sustainable_development_goals'].isnull().all():
            try:
                df_normalized['sdg_names'] = df_normalized['sustainable_development_goals'].apply(
                    lambda sdgs: '; '.join(sorted(list(set(
                        sdg.get('display_name', '')
                        for sdg in sdgs if isinstance(sdg, dict) and sdg.get('display_name')
                    )))) if isinstance(sdgs, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warn: SDG names error: {e}")
                df_normalized['sdg_names'] = ''
        else:
            df_normalized['sdg_names'] = ''

        # --- Counts by Year (Cited By) ---
        if 'counts_by_year' in df_normalized.columns and not df_normalized['counts_by_year'].isnull().all():
            try:
                df_normalized['counts_by_year_str'] = df_normalized['counts_by_year'].apply(
                    lambda x: json.dumps(x) if isinstance(x, list) else str(x)
                ).fillna('')
            except Exception as e:
                print(f"Warn: Counts by year error: {e}")
                df_normalized['counts_by_year_str'] = ''
        else:
            df_normalized['counts_by_year_str'] = ''

        # --- Ensure other potential direct columns exist ---
        # (Copy the block of 'if col not in df_normalized.columns:' checks from normalize_local_json here)
        if 'cited_by_count' not in df_normalized.columns:
            df_normalized['cited_by_count'] = None
        if 'fwci' not in df_normalized.columns:
            df_normalized['fwci'] = None
        if 'cited_by_percentile_year.min' not in df_normalized.columns:
            df_normalized['cited_by_percentile_year.min'] = None
        if 'cited_by_percentile_year.max' not in df_normalized.columns:
            df_normalized['cited_by_percentile_year.max'] = None
        if 'biblio.volume' not in df_normalized.columns:
            df_normalized['biblio.volume'] = None
        if 'biblio.issue' not in df_normalized.columns:
            df_normalized['biblio.issue'] = None
        if 'biblio.first_page' not in df_normalized.columns:
            df_normalized['biblio.first_page'] = None
        if 'biblio.last_page' not in df_normalized.columns:
            df_normalized['biblio.last_page'] = None
        if 'is_retracted' not in df_normalized.columns:
            df_normalized['is_retracted'] = None
        if 'is_paratext' not in df_normalized.columns:
            df_normalized['is_paratext'] = None
        if 'has_fulltext' not in df_normalized.columns:
            df_normalized['has_fulltext'] = None
        if 'ids.mag' not in df_normalized.columns:
            df_normalized['ids.mag'] = None
        if 'ids.pmid' not in df_normalized.columns:
            df_normalized['ids.pmid'] = None
        if 'ids.pmcid' not in df_normalized.columns:
            df_normalized['ids.pmcid'] = None
        if 'referenced_works_count' not in df_normalized.columns:
            df_normalized['referenced_works_count'] = None
        if 'updated_date' not in df_normalized.columns:
            df_normalized['updated_date'] = None
        if 'created_date' not in df_normalized.columns:
            df_normalized['created_date'] = None
        if 'cited_by_api_url' not in df_normalized.columns:
            df_normalized['cited_by_api_url'] = None
        # Also ensure top-level is_oa exists if you want it (might not be in API response)
        if 'is_oa' not in df_normalized.columns:
            df_normalized['is_oa'] = None

        # --- 3. Select Columns ---
        # Use the SAME comprehensive list as in normalize_local_json
        core_columns = [
            # Basic Info
            'id', 'doi', 'title', 'publication_year', 'publication_date', 'language', 'type', 'type_crossref',
            # Authorship (extracted)
            'author_names', 'author_orcids', 'institution_names_str', 'institution_countries', 'corresponding_author_ids',
            # Source/Location (extracted/direct)
            'source_display_name', 'primary_location.source.id', 'primary_location.source.issn_l', 'primary_location.landing_page_url', 'primary_location.pdf_url', 'primary_location.version',
            # Open Access (extracted/direct)
            'is_oa', 'oa_status', 'open_access.oa_url', 'primary_location.is_oa', 'primary_location.license',
            # Citation/Impact
            'cited_by_count', 'fwci', 'cited_by_percentile_year.min', 'cited_by_percentile_year.max',
            # Bibliographic Info
            'biblio.volume', 'biblio.issue', 'biblio.first_page', 'biblio.last_page',
            # Topics/Concepts/Keywords (extracted)
            'primary_topic_name', 'all_topic_names', 'concept_names_level0', 'keywords_str',
            # Funding (extracted)
            'funder_names', 'award_ids',
            # SDGs (extracted)
            'sdg_names',
            # Other Flags/Info
            'is_retracted', 'is_paratext', 'has_fulltext',
            # Other IDs
            'ids.mag', 'ids.pmid', 'ids.pmcid',
            # Counts
            'counts_by_year_str', 'referenced_works_count',
            # Dates
            'updated_date', 'created_date',
            # URLs for further queries
            'cited_by_api_url',
        ]

        # Filter to existing columns
        existing_cols = [
            col for col in core_columns if col in df_normalized.columns]
        # Create the final batch
        df_batch = df_normalized[existing_cols].copy()

        print("Batch normalization, custom extraction, and column selection complete.")
        print("Batch DataFrame shape:", df_batch.shape)
        print("Batch Columns to be saved:",
              df_batch.columns.tolist())  # Debug print

    except Exception as e:
        print(
            f"CRITICAL ERROR during batch normalization/processing for year {year}: {e}")
        print(traceback.format_exc())  # Print detailed traceback
        # Return empty DataFrame on critical processing error for this batch
        return pd.DataFrame()

    # --- 4. Append to CSV ---
    try:
        file_exists = os.path.exists(csv_filename)
        write_header = not file_exists or os.path.getsize(csv_filename) == 0
        print(
            f"Appending data batch to {csv_filename} (Header: {write_header})...")
        df_batch.to_csv(csv_filename, mode='a',
                        header=write_header, index=False, encoding='utf-8')
        print(f"Data batch successfully appended to {csv_filename}")

        # --- 5. Update State File ---
        # Only save state *after* successfully writing the corresponding data
        save_fetch_state(last_page_processed_in_batch, year)

    except Exception as e:
        print(
            f"ERROR appending batch to CSV or saving state for year {year}: {e}")
        print(traceback.format_exc())
        print("State file will NOT be updated for this batch to ensure reprocessing.")
        # Return the processed batch DF even if saving fails, but state wasn't updated
        return df_batch

    # Return the processed DataFrame for this batch
    return df_batch


# --- Main function modified to loop through years ---
def fetch_data_for_years(base_filter, years_to_fetch, csv_filename, pages_per_pause=4, pause_duration_min=1):
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
    years = [2020]

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
