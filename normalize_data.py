import pandas as pd
import json
import os
import glob  # To find split files
import traceback
import math  # For ceiling division in splitting

# --- Configuration ---
# Directory containing the split CSV files from enrichment script
INPUT_SPLIT_DIR = 'output_splits'
INPUT_FILE_PATTERN = 'polyu_data_part_*.csv'  # Pattern to match split files
# Subdirectory for the new relational CSVs
OUTPUT_NORMALIZED_DIR = 'normalized_data_final'

# --- Control Parameters for Splitting OUTPUT ---
MAX_ROWS_PER_NORMALIZED_FILE = 5000  # <<<--- ADJUSTED TO 5000 ROWS MAX
# How many rows to process at a time when WRITING large split tables (Less relevant now we build DF first)
CHUNKSIZE_FOR_WRITING_SPLITS = 50000

# --- Helper Functions ---


def parse_json_string(json_string, default=None):
    """Safely parse a JSON string, return default if invalid."""
    if default is None:
        default = []
    if pd.isna(json_string) or not isinstance(json_string, str):
        return default
    try:
        # Handle potential double-escaped quotes sometimes produced when pandas stringifies JSON
        if json_string.startswith('"[{') and json_string.endswith('}]"'):
            json_string = json_string[1:-1].replace('\\"', '"')
        elif json_string.startswith('"[') and json_string.endswith(']"'):
            json_string = json_string[1:-1].replace('\\"', '"')
        return json.loads(json_string)
    except (json.JSONDecodeError, TypeError):
        return default


def ensure_dir(directory):
    """Creates a directory if it doesn't exist."""
    if not os.path.exists(directory):
        print(f"Creating output directory: {directory}")
        os.makedirs(directory)


def split_and_save_dataframe(df, output_basepath, max_rows, chunk_size, columns_order):
    """Splits a DataFrame and saves it into multiple CSV files if it exceeds max_rows."""
    total_rows = len(df)
    if total_rows == 0:
        print(
            f"  DataFrame for {os.path.basename(output_basepath)} is empty. Skipping save.")
        return

    # Ensure columns are in the correct order before saving/splitting
    # Filter columns_order to only include columns actually present in df
    columns_to_save = [col for col in columns_order if col in df.columns]
    if len(columns_to_save) != len(columns_order):
        missing_expected = [
            col for col in columns_order if col not in df.columns]
        if missing_expected:  # Only print warning if expected columns are missing
            print(
                f"  Warning: Expected columns missing from DataFrame for {os.path.basename(output_basepath)}: {missing_expected}. Saving available columns.")

    # Use only the available columns in the specified order
    df = df[columns_to_save]

    if total_rows <= max_rows:
        # Save as a single file
        output_filepath = f"{output_basepath}.csv"
        print(f"  Saving {total_rows} rows to single file: {output_filepath}")
        try:
            df.to_csv(output_filepath, index=False, encoding='utf-8-sig')
        except Exception as e:
            print(f"  ERROR saving single file {output_filepath}: {e}")
            traceback.print_exc()
    else:
        # Split the DataFrame
        num_files = math.ceil(total_rows / max_rows)
        print(
            f"  DataFrame is large ({total_rows} rows). Splitting into {num_files} files (max {max_rows} rows each)...")

        for i in range(num_files):
            start_row = i * max_rows
            end_row = start_row + max_rows  # Slice will handle the end boundary correctly
            df_split = df.iloc[start_row:end_row]

            output_filepath = f"{output_basepath}_{i+1}.csv"

            print(
                f"    Saving part {i+1}/{num_files} ({len(df_split)} rows) to: {output_filepath}")
            try:
                # Write header for each split file since they are separate files
                df_split.to_csv(output_filepath, index=False,
                                encoding='utf-8-sig', header=True)
            except Exception as e:
                print(f"    ERROR saving split file {output_filepath}: {e}")
                traceback.print_exc()


# --- Main Normalization Function ---

def normalize_enriched_data(input_dir, file_pattern, output_dir):
    """
    Reads split enriched CSVs, normalizes nested data into relational tables,
    and saves them as separate CSV files, splitting large tables.
    """
    input_files = glob.glob(os.path.join(input_dir, file_pattern))
    if not input_files:
        print(
            f"Error: No input files found matching pattern '{file_pattern}' in directory '{input_dir}'")
        return

    print(f"Found {len(input_files)} input file parts to process.")
    ensure_dir(output_dir)

    # Data Storage
    raw_scopus_data = []
    publications_data = []
    authors_set = {}
    institutions_set = {}
    funders_set = {}
    publication_authorships_list = []
    authorship_institutions_list = []
    authorship_countries_list = []
    publication_funding_list = []
    publication_citation_counts_list = []

    # Define Columns for Output Tables
    raw_scopus_cols = [
        "prism_url", "dc_identifier", "prism_publicationname", "prism_coverdate",
        "prism_doi", "citedby_count", "subtype", "subtypedescription",
        "publication_year", "publication_month", "doi"
    ]
    publication_cols = [
        "doi", "oa_id", "oa_doi", "oa_title", "oa_display_name", "oa_publication_year",
        "oa_publication_date", "oa_language", "oa_type", "oa_cited_by_count",
        "oa_fwci", "oa_is_retracted", "oa_updated_date", "oa_created_date",
        "oa_primary_location_is_oa", "oa_primary_location_landing_page_url",
        "oa_primary_location_source_id", "oa_primary_location_source_name",
        "oa_primary_location_source_issn_l", "oa_primary_location_source_is_oa",
        "oa_primary_location_source_is_indexed_in_scopus",
        "oa_primary_location_source_host_org_name",
        "oa_primary_location_source_host_org_lineage_names",
        "oa_primary_location_source_type", "oa_biblio_volume", "oa_biblio_issue",
        "oa_biblio_first_page", "oa_biblio_last_page", "oa_primary_topic_id",
        "oa_primary_topic_name", "oa_primary_topic_score",
        "oa_primary_topic_subfield_name", "oa_primary_topic_field_name",
        "oa_primary_topic_domain_name", "oa_cnp_value", "oa_cnp_is_top_1_percent",
        "oa_cnp_is_top_10_percent", "oa_cbpy_min", "oa_cbpy_max", "oa_status"
    ]
    author_cols = ["oa_author_id", "oa_author_name", "oa_author_orcid"]
    institution_cols = ["oa_institution_id", "oa_institution_name",
                        "oa_institution_ror", "oa_institution_country_code", "oa_institution_type"]
    funder_cols = ["oa_funder_id", "oa_funder_name"]
    publication_authorship_cols = [
        "doi", "oa_author_id", "oa_author_position", "oa_author_is_corresponding", "oa_author_raw_name"]
    authorship_institution_cols = [
        "doi", "oa_author_id", "oa_institution_id", "oa_raw_affiliation_string"]
    authorship_country_cols = ["doi", "oa_author_id", "oa_country_code"]
    publication_funding_cols = ["doi", "oa_funder_id", "oa_award_id"]
    publication_citation_count_cols = ["doi", "year", "cited_by_count"]

    total_rows_processed = 0
    skipped_doi_count = 0
    print("Starting normalization process...")

    for file_part in input_files:
        print(f"  Processing file: {file_part}")
        try:
            # Read input CSV part
            df_part = pd.read_csv(file_part, low_memory=False, keep_default_na=True, na_values=[
                                  '', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null', 'None'])
            original_cols_present = [
                col for col in df_part.columns if col in raw_scopus_cols]  # Find original cols

            # --- Iterate through rows ---
            for index, row in df_part.iterrows():
                publication_doi = row.get('doi')
                if pd.isna(publication_doi):
                    skipped_doi_count += 1
                    continue

                # 1. Raw Scopus Data
                raw_data = {col: row.get(col) for col in original_cols_present}
                raw_scopus_data.append(raw_data)

                # 2. Publications Data
                pub_data = {col: row.get(col)
                            for col in publication_cols if col in row}
                if 'doi' not in pub_data or pd.isna(pub_data.get('doi')):
                    pub_data['doi'] = publication_doi
                publications_data.append(pub_data)

                # 3. Authorships Normalization
                authorships_list = parse_json_string(
                    row.get('oa_authorships'), default=[])
                for auth_ship in authorships_list:
                    if not isinstance(auth_ship, dict):
                        continue
                    author_info = auth_ship.get('author', {})
                    if not isinstance(author_info, dict):
                        author_info = {}
                    author_id = author_info.get('id')
                    if not author_id:
                        continue

                    if author_id not in authors_set:
                        authors_set[author_id] = {"oa_author_id": author_id, "oa_author_name": author_info.get(
                            'display_name'), "oa_author_orcid": author_info.get('orcid')}
                    publication_authorships_list.append({"doi": publication_doi, "oa_author_id": author_id, "oa_author_position": auth_ship.get(
                        'author_position'), "oa_author_is_corresponding": auth_ship.get('is_corresponding'), "oa_author_raw_name": auth_ship.get('raw_author_name')})

                    institutions_list = auth_ship.get('institutions', [])
                    if not isinstance(institutions_list, list):
                        institutions_list = []
                    raw_aff_strings = auth_ship.get(
                        'raw_affiliation_strings', [])
                    raw_aff_string = ", ".join(
                        raw_aff_strings) if raw_aff_strings else None

                    for inst in institutions_list:
                        if not isinstance(inst, dict):
                            continue
                        inst_id = inst.get('id')
                        if not inst_id:
                            continue
                        if inst_id not in institutions_set:
                            institutions_set[inst_id] = {"oa_institution_id": inst_id, "oa_institution_name": inst.get('display_name'), "oa_institution_ror": inst.get(
                                'ror'), "oa_institution_country_code": inst.get('country_code'), "oa_institution_type": inst.get('type')}
                        authorship_institutions_list.append(
                            {"doi": publication_doi, "oa_author_id": author_id, "oa_institution_id": inst_id, "oa_raw_affiliation_string": raw_aff_string})

                    countries_list = auth_ship.get('countries', [])
                    if not isinstance(countries_list, list):
                        countries_list = []
                    for country_code in set(countries_list):
                        if country_code:
                            authorship_countries_list.append(
                                {"doi": publication_doi, "oa_author_id": author_id, "oa_country_code": country_code})

                # 4. Grants Normalization
                grants_list = parse_json_string(
                    row.get('oa_grants'), default=[])
                for grant in grants_list:
                    if not isinstance(grant, dict):
                        continue
                    funder_id = grant.get('funder')
                    funder_name = grant.get('funder_display_name')
                    award_id = grant.get('award_id')
                    if not funder_id:
                        continue
                    if funder_id not in funders_set:
                        funders_set[funder_id] = {
                            "oa_funder_id": funder_id, "oa_funder_name": funder_name}
                    publication_funding_list.append(
                        {"doi": publication_doi, "oa_funder_id": funder_id, "oa_award_id": award_id})

                # 5. Counts By Year Normalization
                counts_list = parse_json_string(
                    row.get('oa_counts_by_year'), default=[])
                for count_entry in counts_list:
                    if not isinstance(count_entry, dict):
                        continue
                    year = count_entry.get('year')
                    cited_count = count_entry.get('cited_by_count')
                    if year is not None and cited_count is not None:
                        publication_citation_counts_list.append(
                            {"doi": publication_doi, "year": year, "cited_by_count": cited_count})

                total_rows_processed += 1
                if total_rows_processed % 10000 == 0:  # Log progress less frequently
                    print(
                        f"  ...processed {total_rows_processed} total rows from input files...")

        except Exception as e:
            print(f"Error processing file {file_part}: {e}")
            traceback.print_exc()

    print(
        f"\nFinished reading all parts. Total input rows processed: {total_rows_processed}")
    if skipped_doi_count > 0:
        print(
            f"Skipped {skipped_doi_count} rows due to missing formatted DOI.")
    print("Converting collected data into DataFrames and de-duplicating...")

    # Convert collected data to DataFrames and deduplicate
    df_raw_scopus = pd.DataFrame(
        raw_scopus_data).drop_duplicates(subset=['doi'])
    df_publications = pd.DataFrame(
        publications_data).drop_duplicates(subset=['doi'])
    df_authors = pd.DataFrame(list(authors_set.values())).drop_duplicates(
        subset=['oa_author_id'])
    df_institutions = pd.DataFrame(
        list(institutions_set.values())).drop_duplicates(subset=['oa_institution_id'])
    df_funders = pd.DataFrame(list(funders_set.values())).drop_duplicates(
        subset=['oa_funder_id'])
    df_pub_authorships = pd.DataFrame(publication_authorships_list).drop_duplicates(
        subset=['doi', 'oa_author_id', 'oa_author_position'])
    df_auth_inst = pd.DataFrame(authorship_institutions_list).drop_duplicates(
        subset=['doi', 'oa_author_id', 'oa_institution_id'])
    df_auth_country = pd.DataFrame(authorship_countries_list).drop_duplicates(
        subset=['doi', 'oa_author_id', 'oa_country_code'])
    df_pub_funding = pd.DataFrame(publication_funding_list).drop_duplicates(
        subset=['doi', 'oa_funder_id', 'oa_award_id'])
    df_pub_counts = pd.DataFrame(
        publication_citation_counts_list).drop_duplicates(subset=['doi', 'year'])

    print("DataFrames created. Saving to CSV files (splitting if needed)...")

    # Define table metadata and save/split
    tables_to_save = {
        'raw_scopus_search': (df_raw_scopus, raw_scopus_cols),
        'publications': (df_publications, publication_cols),
        'authors': (df_authors, author_cols),
        'institutions': (df_institutions, institution_cols),
        'funders': (df_funders, funder_cols),
        'publication_authorships': (df_pub_authorships, publication_authorship_cols),
        'authorship_institutions': (df_auth_inst, authorship_institution_cols),
        'authorship_countries': (df_auth_country, authorship_country_cols),
        'publication_funding': (df_pub_funding, publication_funding_cols),
        'publication_citation_counts': (df_pub_counts, publication_citation_count_cols)
    }

    for table_name, (df_table, columns) in tables_to_save.items():
        output_base = os.path.join(output_dir, table_name)
        print(f"\nHandling table: {table_name} (Shape: {df_table.shape})")
        # Use the predefined column list for consistent output structure
        split_and_save_dataframe(
            df=df_table,
            output_basepath=output_base,
            max_rows=MAX_ROWS_PER_NORMALIZED_FILE,  # Use the global config for max rows
            chunk_size=CHUNKSIZE_FOR_WRITING_SPLITS,
            columns_order=columns  # Pass the predefined columns list
        )

    print(f"\nAll normalized CSV files saved successfully to '{output_dir}'.")


# --- Main Execution ---
if __name__ == "__main__":
    # --- Run the normalization ---
    normalize_enriched_data(
        input_dir=INPUT_SPLIT_DIR,
        file_pattern=INPUT_FILE_PATTERN,
        output_dir=OUTPUT_NORMALIZED_DIR
    )
    print("\nNormalization script finished.")
