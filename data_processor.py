import pandas as pd
import os
import math
import traceback

# count_csv_rows function remains the same


def count_csv_rows(filepath, encoding='utf-8-sig'):
    """Counts rows in a CSV file efficiently, excluding the header."""
    try:
        row_count = 0
        # Try opening with specified encoding, fallback if necessary
        try:
            with open(filepath, 'r', encoding=encoding) as f:
                for _ in f:
                    row_count += 1
        except UnicodeDecodeError:
            print(
                f"Warning: Failed to decode {filepath} with {encoding}. Trying default encoding.")
            with open(filepath, 'r') as f:  # Fallback to system default
                for _ in f:
                    row_count += 1
        return max(0, row_count - 1) if row_count > 0 else 0
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return -1
    except Exception as e:
        print(f"Error counting rows in {filepath}: {e}")
        return -1


def split_csv_by_rows(input_filepath, output_basepath, rows_per_file=5000, chunk_size=50000, input_encoding='utf-8-sig'):
    """
    Splits a large CSV file into smaller files based on a maximum row count per file.
    Revised logic for more accurate splitting.

    Args:
        input_filepath (str): Path to the large input CSV file.
        output_basepath (str): Base path for the output files (e.g., 'split_output/data_part').
                               The script will append '_1.csv', '_2.csv', etc.
        rows_per_file (int): The maximum number of data rows (excluding header) per output file.
        chunk_size (int): Number of rows to read into memory at a time. Adjust based on RAM.
        input_encoding (str): Encoding of the input CSV file.
    """
    print(f"Starting CSV split process for: {input_filepath}")
    print(f"Target max rows per output file: {rows_per_file}")
    print(f"Reading in chunks of size: {chunk_size}")

    if not os.path.exists(input_filepath):
        print(f"Error: Input file not found at '{input_filepath}'")
        return 0  # Return 0 files created

    output_dir = os.path.dirname(output_basepath)
    if output_dir and not os.path.exists(output_dir):
        print(f"Creating output directory: {output_dir}")
        os.makedirs(output_dir)

    current_file_index = 1
    rows_written_to_current_file = 0
    header = None
    # Initialize first output path
    output_filepath = f"{output_basepath}_{current_file_index}.csv"
    total_rows_processed = 0
    files_created = []  # Keep track of files actually created

    try:
        print("Reading input file in chunks...")
        reader = pd.read_csv(input_filepath, chunksize=chunk_size,
                             encoding=input_encoding, low_memory=False)

        for chunk_df in reader:
            if header is None:
                header = chunk_df.columns.tolist()

            chunk_start_index = 0  # Index within the current chunk
            rows_in_chunk = len(chunk_df)

            while chunk_start_index < rows_in_chunk:
                # Path for the current output file
                output_filepath = f"{output_basepath}_{current_file_index}.csv"

                # Determine if header needs writing (only if starting a new file)
                write_header = (rows_written_to_current_file == 0)

                # How many more rows can fit in the current output file?
                rows_can_take = rows_per_file - rows_written_to_current_file

                # How many rows are actually left in this chunk?
                rows_left_in_chunk = rows_in_chunk - chunk_start_index

                # How many rows to take from this chunk for the current output file?
                rows_to_write_now = min(rows_can_take, rows_left_in_chunk)

                # Get the slice of data to write
                data_slice = chunk_df.iloc[chunk_start_index:
                                           chunk_start_index + rows_to_write_now]

                # Append the slice
                data_slice.to_csv(output_filepath,
                                  mode='a',
                                  header=write_header,
                                  index=False,
                                  encoding='utf-8-sig')

                if output_filepath not in files_created:
                    # Record file creation
                    files_created.append(output_filepath)

                # Update counters
                rows_written_to_current_file += rows_to_write_now
                chunk_start_index += rows_to_write_now
                total_rows_processed += rows_to_write_now

                # Check if the current output file is full
                if rows_written_to_current_file >= rows_per_file:
                    print(
                        f"--- Completed file {current_file_index} ({rows_written_to_current_file} rows). Moving to next file. ---")
                    current_file_index += 1
                    rows_written_to_current_file = 0  # Reset for the new file

            # Print progress after processing each chunk
            print(
                f"  Processed chunk. Total rows processed so far: {total_rows_processed}")

        print("\nFinished processing all chunks.")
        print(f"Total data rows processed and written: {total_rows_processed}")
        print(f"CSV split into {len(files_created)} files.")
        return len(files_created)  # Return number of files created

    except Exception as e:
        print(f"\nAn error occurred during the split process: {e}")
        traceback.print_exc()
        # Return count even if error occurred mid-way
        return len(files_created)
    finally:
        if 'reader' in locals() and reader is not None and hasattr(reader, 'close'):
            reader.close()


# --- Main Execution ---
if __name__ == "__main__":
    # --- Configuration ---
    INPUT_FILE = 'openalex_enriched_combined_data_refined.csv'  # Your large input file
    OUTPUT_BASE = 'output_splits/polyu_data_part'

    # --- Define Split Strategy ---
    MAX_ROWS_PER_FILE = 5000  # <<<--- SET TO 5000

    CHUNKSIZE_FOR_READING = 50000  # Keep this relatively large for read efficiency

    # --- Run the split ---
    num_created = split_csv_by_rows(
        input_filepath=INPUT_FILE,
        output_basepath=OUTPUT_BASE,
        rows_per_file=MAX_ROWS_PER_FILE,
        chunk_size=CHUNKSIZE_FOR_READING
    )

    print(f"\nScript finished. Created {num_created} output files.")

    # --- Optional: Verification Step ---
    print("\nVerifying row counts in output files...")
    total_rows_in_splits = 0
    if num_created > 0:
        all_files_found = True
        for i in range(1, num_created + 1):
            split_file_path = f"{OUTPUT_BASE}_{i}.csv"
            if os.path.exists(split_file_path):
                count = count_csv_rows(split_file_path)
                if count >= 0:
                    print(
                        f"  - File {i} ({os.path.basename(split_file_path)}): {count} rows")
                    total_rows_in_splits += count
                else:
                    print(
                        f"  - File {i} ({os.path.basename(split_file_path)}): Error counting rows.")
                    all_files_found = False  # Consider it an issue
            else:
                print(
                    f"  - File {i} ({os.path.basename(split_file_path)}): File NOT FOUND.")
                all_files_found = False

        if all_files_found:
            print(
                f"Total rows found across split files: {total_rows_in_splits}")
            # Compare with original count if needed (re-run count_csv_rows on input)
            original_count = count_csv_rows(INPUT_FILE)
            print(f"Original file row count: {original_count}")
            if total_rows_in_splits == original_count:
                print("Verification successful: Total rows match.")
            else:
                print(
                    f"!!! WARNING: Row count mismatch! Original={original_count}, Splits={total_rows_in_splits} !!!")
        else:
            print("Verification incomplete due to errors or missing files.")

    else:
        print("No files were created, skipping verification.")
