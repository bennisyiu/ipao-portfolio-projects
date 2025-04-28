import json
import pandas as pd
import os


def process_local_institution_json_all(json_filepath, csv_filename):
    """
    Reads an OpenAlex Institution JSON file locally, normalizes ALL data
    as much as possible, saves it to CSV, and returns a DataFrame.
    Complex lists/dicts may be stored as JSON strings in the CSV cells.

    Args:
        json_filepath (str): The path to the input JSON file.
        csv_filename (str): The desired path for the output CSV file.

    Returns:
        pandas.DataFrame or None: The processed DataFrame containing the
                                  institution profile, or None if an error occurs.
    """
    # --- 1. Read and Parse Local JSON File ---
    print(f"Reading local JSON file: {json_filepath}")
    if not os.path.exists(json_filepath):
        print(f"Error: Input JSON file not found at '{json_filepath}'")
        return None

    try:
        with open(json_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to decode JSON from file: {e}")
        return None
    except IOError as e:
        print(f"Error: Could not read file: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred reading the file: {e}")
        return None

    # --- 2. Extract the Institution Data Object ---
    results_list = data.get('results', [])
    if not results_list or not isinstance(results_list, list) or len(results_list) == 0:
        print("Error: JSON file does not contain a valid 'results' list or it's empty.")
        return None
    institution_data = results_list[0]
    if not isinstance(institution_data, dict):
        print("Error: First item in 'results' is not a valid dictionary.")
        return None

    print("Successfully loaded and parsed institution data from JSON.")
    print("Processing institution data...")

    try:
        # --- 3. Normalize using Pandas ---
        # This will handle nested dicts like summary_stats, ids, geo, international.display_name
        # It will leave lists of dicts (repositories, counts_by_year, topics, roles, x_concepts)
        # and lists of simple values (lineage, display_name_acronyms, display_name_alternatives)
        # mostly as objects in the dataframe initially.
        df = pd.json_normalize([institution_data])
        print(f"Initial normalization created {df.shape[1]} columns.")
        # print("Columns after initial normalize:", df.columns.tolist()) # Optional debug

        # --- 4. Convert Simple Lists to Strings ---
        # Explicitly join lists of simple strings/numbers for better readability in CSV
        simple_list_columns = ['display_name_acronyms',
                               'display_name_alternatives', 'lineage']
        for col in simple_list_columns:
            if col in df.columns:
                # Create a new column name to avoid overwriting original list object if needed later
                new_col_name = f'{col}_str'
                df[new_col_name] = df[col].apply(
                    lambda x: ', '.join(
                        map(str, x)) if isinstance(x, list) else x
                ).fillna('')
                # Optional: Drop the original list column if you only want the string version
                # df = df.drop(columns=[col])

        # Note: Columns containing lists of dictionaries (like 'repositories', 'counts_by_year', 'topics')
        # will likely be converted to their string representations automatically when saved to CSV.
        # If you wanted specific info (like ONLY topic names), custom extraction would be needed here,
        # but you asked for *everything*, so preserving the structure as a string is the way.

        print(f"Processing complete. Final DataFrame shape: {df.shape}")
        print("Final Columns:", df.columns.tolist())

    except Exception as e:
        print(f"CRITICAL ERROR during data processing: {e}")
        # Optionally print df.info() or df.head() here before erroring to debug
        return None

    # --- 5. Save ALL Columns to CSV ---
    try:
        print(f"Saving full institution profile to {csv_filename}...")
        # Change encoding to 'utf-8-sig' to add a BOM
        df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        print(
            f"Profile successfully saved to {csv_filename} (using utf-8-sig)")
    except Exception as e:
        print(f"ERROR attempting to save the CSV file: {e}")
        # Still return the DataFrame even if saving fails
        return df

    return df


# --- Main Execution ---
if __name__ == "__main__":
    # --- Configuration ---
    # IMPORTANT: Make sure this path points to your downloaded JSON file!
    # <-- CHANGE IF YOUR FILENAME IS DIFFERENT
    INPUT_JSON_FILE = "data-202542615535.json"
    OUTPUT_CSV_FILE = "polyu_institution_profile_all_local.csv"  # New output name

    # Process the local file
    profile_dataframe = process_local_institution_json_all(
        INPUT_JSON_FILE, OUTPUT_CSV_FILE)

    # Display results confirmation
    if profile_dataframe is not None:
        print("\n--- Script Finished Successfully ---")
        print(f"Full profile data saved to {OUTPUT_CSV_FILE}")
        print(f"DataFrame shape: {profile_dataframe.shape}")
        print("Columns included:", profile_dataframe.columns.tolist())
        # Displaying the full row might be too wide for the console
        # print("\nDataFrame Content (first few columns):")
        # print(profile_dataframe.iloc[:, :5].to_string()) # Print first 5 columns
    else:
        print("\n--- Script Finished with Errors ---")
        print("Failed to process the JSON file or save the CSV. Check logs above.")
