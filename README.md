# Scopus and OpenAlex Data Enrichment & Database Loading Pipeline

## 📌 Overview

This project implements an ETL (Extract, Transform, Load) pipeline designed to enrich a list of research publications (initially identified via DOIs, sourced from Scopus) with comprehensive metadata from the OpenAlex database. The pipeline processes the data in stages, normalizes it into a relational structure, and finally loads it into a PostgreSQL database, ready for analysis and visualization using tools like Power BI or Tableau.

The primary workflow is:

1.  **Input:** Start with calling SCOPUS API to retrieve publication DOIs (e.g., `prism_doi`) and other metadata from SCOPUS database
2.  **Enrichment:** Use the DOIs to query the OpenAlex API (`extract_opealex.py`) and retrieve rich work details (authorships, institutions, topics, funding, citations, etc.). This stage includes batch processing, pauses, and resume capabilities.
3.  **Splitting (optional):** Split the large, enriched CSV file into smaller, manageable chunks (`transform_split_data.py`).
4.  **Normalization:** Process the enriched data (either the single large file or the splits) to create multiple, normalized relational tables (authors, institutions, publications, link tables, etc.) based on a defined schema (`transform_normalize_data.py`). Large normalized tables can also be split.
5.  **Loading:** Upload the data from the final normalized CSV files into corresponding tables within a PostgreSQL database, using an upsert strategy (`load_data_uploader.py`).

## ✨ Features

- **OpenAlex Data Enrichment:** Fetches comprehensive publication metadata using DOIs via the OpenAlex API (using `pyalex`).
- **Relational Data Normalization:** Transforms nested OpenAlex data into a clean, relational schema suitable for database analysis.
- **Robust Extraction:** Handles large datasets through batch processing, polite API pauses, and error handling during OpenAlex queries.
- **Resume Capability:** The OpenAlex extraction script can resume from where it left off by checking previously processed DOIs in the output file.
- **Data Splitting:** Includes scripts to split large intermediate and final CSV files based on row count for easier handling.
- **Database Loading:** Efficiently uploads normalized data to PostgreSQL using an "upsert" (Insert on Conflict Update) strategy.
- **Secure Credential Management:** Uses `.env` files to manage database credentials and API emails securely.
- **Modular Design:** Scripts are separated by function (extract, split, normalize, load) for better maintainability.

## 📁 Project Structure

scopus\*openalex_projects/
├── extract_opealex.py
├── extract_scopus_search.py
├── transform_split_data.py
├── transform_normalize_data.py
├── load_data_uploader.py
├── fetch_state_yearly.json # (State file used by older versions - potentially removable)
├── requirements.txt
├── scopus_checkpoint.json # (State file used by older versions - potentially removable)
├── scopus_search_metadata.json # (State file used by older versions - potentially removable)
└── README.md

## ⚙️ Setup

### Prerequisites

- Python 3.8+
- PostgreSQL Database (ensure it's running and accessible)
- An **existing input CSV file** containing publication DOIs (place it in `extracted_data/raw_copy/`)

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd <your-repo-directory>
    ```
2.  **Create and activate a virtual environment:**

    ```bash
    # Linux/macOS
    python3 -m venv myenv
    source myenv/bin/activate

    # Windows (Command Prompt/PowerShell)
    python -m venv myenv
    .\myenv\Scripts\activate
    ```

3.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

    _(Ensure `requirements.txt` includes `pyalex`, `pandas`, `psycopg2-binary`, `python-dotenv`, `requests`, `numpy`)_

4.  **Set up Environment Variables:**
    Create a file named `.env` in the project root directory (`scopus_openalex_projects/`) and add your credentials:

    ```dotenv
    # .env file - MAKE SURE THIS FILE IS IN YOUR .gitignore!

    # OpenAlex Polite Pool Email (Required by extract_opealex.py)
    OPENALEX_EMAIL=your_email@example.com

    # PostgreSQL Database Credentials (Required by load_data_uploader.py)
    DB_HOSTNAME=localhost       # Or your DB host address
    DB_DATABASE=your_db_name    # Your database name
    DB_USERNAME=your_db_user    # Your database username
    DB_PASSWORD=your_db_password  # Your database password
    DB_PORT=5432                # Your database port
    DB_SCHEMA=public            # The target schema in your database
    ```

    **Replace the placeholder values with your actual credentials.**

## 🚀 Usage / Execution Workflow

Run the scripts from the project root directory (`scopus_openalex_projects/`) in the following order:

1.  **Configure `.env`:** Ensure your `.env` file is created and correctly populated.
2.  **Place Input Data:** Put your starting CSV file (containing the `prism_doi` column) into the `extracted_data/raw_copy/` directory. Update the `INPUT_CSV_FILE` variable inside `extract_opealex.py` if your filename is different.
3.  **Run OpenAlex Enrichment:**
    ```bash
    python extract_opealex.py
    ```
    - This script reads the input CSV, formats DOIs, queries OpenAlex for each _new_ DOI (it checks the output file to resume), extracts specified fields, and appends the combined original + OpenAlex data to the output CSV (defined by `OUTPUT_CSV_FILE` within the script, e.g., `openalex_enriched_combined_data_refined.csv`).
    - It processes in batches and pauses. This step can take a long time depending on the number of DOIs.
    - **For Testing:** You can modify `extract_opealex.py` to set `INPUT_CSV_FILE = None` to use the built-in small test dataset instead of reading your large file.
4.  **Split Enriched Data (Optional but Recommended):**
    ```bash
    python transform_split_data.py
    ```
    - This script reads the large enriched CSV generated in step 3.
    - It splits it into smaller files (e.g., max 5000 rows each) inside the `output_splits/` directory. Configure `INPUT_FILE`, `OUTPUT_BASE`, and `MAX_ROWS_PER_FILE` within the script if needed.
5.  **Normalize Data:**
    ```bash
    python transform_normalize_data.py
    ```
    - This script reads the split CSV files from `output_splits/`.
    - It parses the nested OpenAlex data (authorships, grants, etc.) and creates multiple normalized CSV files (one for each relational table: publications, authors, institutions, etc.) inside the `normalized_data_final/` directory.
    - It also splits these final normalized tables if they exceed the row limit defined within the script (`MAX_ROWS_PER_NORMALIZED_FILE`).
6.  **Upload to Database:**
    ```bash
    python load_data_uploader.py
    ```
    - This script reads the normalized CSV files from `normalized_data_final/`.
    - It connects to your PostgreSQL database using credentials from `.env`.
    - It creates the necessary tables (if they don't exist) based on the defined schema within the script.
    - It uploads the data using an efficient "upsert" method. Ensure the schema defined in `TABLE_DEFINITIONS` within this script matches the generated files and your database requirements.

## 📊 Database Schema

The `transform_normalize_data.py` script generates the following tables (schema defined within `load_data_uploader.py`):

1.  **`raw_scopus_search`**: Original Scopus metadata linked by `doi`.
2.  **`publications`**: Core OpenAlex publication details (PK: `doi`).
3.  **`authors`**: Unique authors (PK: `oa_author_id`).
4.  **`institutions`**: Unique institutions (PK: `oa_institution_id`).
5.  **`funders`**: Unique funders (PK: `oa_funder_id`).
6.  **`publication_authorships`**: Links publications to authors, includes position (PK: `doi`, `oa_author_id`, `oa_author_position`).
7.  **`authorship_institutions`**: Links specific authorships to institutions listed on that paper (PK: `doi`, `oa_author_id`, `oa_institution_id`).
8.  **`authorship_countries`**: Links specific authorships to countries listed (PK: `doi`, `oa_author_id`, `oa_country_code`).
9.  **`publication_funding`**: Links publications to funders and awards (PK: `doi`, `oa_funder_id`, `oa_award_id`).
10. **`publication_citation_counts`**: Yearly citation counts per publication (PK: `doi`, `year`).

_(Refer to the `TABLE_DEFINITIONS` dictionary in `load_data_uploader.py` for exact column names and data types)._
