import pandas as pd
import json
import os


def normalize_local_json(json_filepath, csv_filename="output_from_local.csv"):
    """
    Reads a local JSON file containing OpenAlex results (expected to have
    a 'results' key with a list of work objects), normalizes the data
    comprehensively, saves it to CSV, and returns a DataFrame.

    Args:
        json_filepath (str): Path to the local JSON file.
        csv_filename (str): Name for the output CSV file.

    Returns:
        pandas.DataFrame or None: The normalized DataFrame, or None if errors occur.
    """
    print(f"Loading JSON data from: {json_filepath}")
    try:
        # Use utf-8-sig to handle potential BOM (Byte Order Mark) if saved from some editors
        with open(json_filepath, 'r', encoding='utf-8-sig') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: JSON file not found at {json_filepath}")
        return None
    except json.JSONDecodeError as e:
        print(
            f"Error: Could not decode JSON from {json_filepath}. Invalid JSON format? Error: {e}")
        # Optionally print more details for debugging
        # print(f"Error details: {e.msg}, Line: {e.lineno}, Col: {e.colno}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while reading the JSON file: {e}")
        return None

    # Check if 'results' key exists and is a list
    if 'results' not in data or not isinstance(data.get('results'), list):
        print("Error: JSON file does not contain a 'results' key with a list.")
        return None

    results_list = data.get('results')
    if not results_list:
        print("Warning: The 'results' list in the JSON file is empty.")
        # Return an empty DataFrame if the list is empty but valid
        return pd.DataFrame()

    print(
        f"Successfully loaded {len(results_list)} records from the JSON file.")
    print("Starting normalization...")

    df_final = pd.DataFrame()  # Initialize final DataFrame

    try:
        # --- 1. Initial Normalization ---
        # This handles most flattening but leaves complex lists/dicts
        df_normalized = pd.json_normalize(results_list)
        print(
            f"Initial normalization resulted in {df_normalized.shape[1]} columns.")

        # --- 2. Custom Data Extraction & Simplification ---
        # Create new columns by processing complex fields

        # --- Authorships ---
        if 'authorships' in df_normalized.columns and not df_normalized['authorships'].isnull().all():
            # Author Names
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

            # Author ORCIDs
            try:
                df_normalized['author_orcids'] = df_normalized['authorships'].apply(
                    lambda authors: '; '.join(sorted(list(set(
                        auth.get('author', {}).get('orcid', '') or ''
                        # Only non-empty ORCIDs
                        for auth in authors if isinstance(auth, dict) and auth.get('author') and auth.get('author').get('orcid')
                    )))) if isinstance(authors, list) else None
                ).fillna('')
            except Exception as e:
                print(f"Warn: Author ORCIDs error: {e}")
                df_normalized['author_orcids'] = ''

            # Institution Names (from Authorships)
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

            # Institution Countries (from Authorships)
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

            # Corresponding Author IDs
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

        else:  # Create empty columns if 'authorships' is missing or all null
            df_normalized['author_names'] = ''
            df_normalized['author_orcids'] = ''
            df_normalized['institution_names_str'] = ''
            df_normalized['institution_countries'] = ''
            df_normalized['corresponding_author_ids'] = ''

        # --- Primary Location / Source ---
        # Check if the complex primary_location column exists and has data
        if 'primary_location' in df_normalized.columns and not df_normalized['primary_location'].isnull().all():
            # Check if the nested source exists within it
            if 'primary_location.source.display_name' in df_normalized.columns:
                df_normalized['source_display_name'] = df_normalized['primary_location.source.display_name'].fillna(
                    '')
            else:  # Fallback if direct path failed but primary_location exists
                try:
                    df_normalized['source_display_name'] = df_normalized['primary_location'].apply(lambda loc: loc.get(
                        'source', {}).get('display_name', '') if isinstance(loc, dict) else '').fillna('')
                except Exception as e:
                    print(f"Warn: Source name (fallback) error: {e}")
                    df_normalized['source_display_name'] = ''
        else:  # If primary_location doesn't exist or is all null
            df_normalized['source_display_name'] = ''
        # Ensure primary_location.source.id column exists even if source was missing
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
            # Primary Topic Name
            if 'primary_topic.display_name' in df_normalized.columns:
                df_normalized['primary_topic_name'] = df_normalized['primary_topic.display_name'].fillna(
                    '')
            else:
                df_normalized['primary_topic_name'] = ''  # Create if missing

            # All Topic Names
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
                        g.get('award_id', '') or ''  # Handle None award_ids
                        # Only non-empty IDs
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
                # Convert list of dicts to a string representation for CSV
                df_normalized['counts_by_year_str'] = df_normalized['counts_by_year'].apply(
                    lambda x: json.dumps(x) if isinstance(x, list) else str(x)
                ).fillna('')
            except Exception as e:
                print(f"Warn: Counts by year error: {e}")
                df_normalized['counts_by_year_str'] = ''
        else:
            df_normalized['counts_by_year_str'] = ''

        # --- Ensure other potential direct columns exist ---
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

        # --- 3. Select Columns ---
        # Define the comprehensive list of columns you want in your final CSV
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

        # Ensure we only try to select columns that actually exist
        existing_cols = [
            col for col in core_columns if col in df_normalized.columns]
        df_final = df_normalized[existing_cols].copy()  # Use .copy()

        print("Normalization, custom extraction, and column selection complete.")
        print("Final DataFrame shape:", df_final.shape)
        print("Final Columns:", df_final.columns.tolist())  # Debug print

    except Exception as e:
        print(f"CRITICAL ERROR during normalization/processing: {e}")
        import traceback
        print(traceback.format_exc())  # Print full traceback for debugging
        print("Returning DataFrame up to point of error if possible...")
        if 'df_normalized' in locals() and 'df_final' not in locals():
            return df_normalized  # Return partially processed if possible
        elif 'df_final' in locals():
            return df_final
        else:
            return None

    # --- 4. Save to CSV ---
    try:
        print(f"Saving data to {csv_filename}...")
        # Use utf-8 encoding for broad compatibility
        df_final.to_csv(csv_filename, index=False, encoding='utf-8')
        print(f"Data successfully saved to {csv_filename}")
    except Exception as e:
        print(f"ERROR attempting to save the CSV file: {e}")
        # Decide if you still want to return the DataFrame even if saving fails
        print("Returning the DataFrame even though CSV saving failed.")

    # --- 5. Return DataFrame ---
    return df_final


# --- How to Use ---
if __name__ == "__main__":
    # --- IMPORTANT: Set the correct path to YOUR saved JSON file ---
    # Make sure this file is in the same directory as the script, or provide the full path
    local_json_file = "data-20254251660.json"
    # --- Set the desired output CSV filename ---
    output_csv_file = "polyu_publications_2020_from_local.csv"

    # Check if the JSON file exists before trying to process
    if not os.path.exists(local_json_file):
        print(f"Error: Cannot find the specified JSON file: {local_json_file}")
        print("Please make sure the file exists and the path is correct.")
    else:
        # Call the function to load, normalize, save, and return the DataFrame
        normalized_dataframe = normalize_local_json(
            local_json_file, output_csv_file)

        if normalized_dataframe is not None:
            print("\n--- Script Finished ---")
            if not normalized_dataframe.empty:
                print("DataFrame created successfully:")
                normalized_dataframe.info()
                print("\n--- DataFrame Head (first 5 rows) ---")
                # Display more columns for verification if needed
                with pd.option_context('display.max_rows', 5, 'display.max_columns', 20):
                    print(normalized_dataframe.head())
            else:
                print("Script finished, but the returned DataFrame is empty (input JSON might have had no 'results' or processing failed early).")

        else:
            print("\n--- Script Finished ---")
            print(
                "Processing failed. No DataFrame was returned. Check error messages above.")
