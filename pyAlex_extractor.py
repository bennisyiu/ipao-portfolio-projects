import pyalex
from pyalex import Works
import pandas as pd
import time
import csv
import os  # To check if CSV exists

# Test run

# Optional: Set your email for the OpenAlex polite pool
pyalex.config.email = "bennis.yiu@connect.polyu.hk"

# List of DOIs to query
dois_to_query = [
    "10.1177/10963480241229235",
    "10.1002/adfm.202413884",
    "10.1109/TNNLS.2023.3336563",
    "10.1016/j.esci.2024.100281",
    "10.1109/TEVC.2023.3278132"
]


def get_openalex_data_for_dois(doi_list):
    """
    Retrieves publication data from OpenAlex for a list of DOIs.

    Args:
        doi_list: A list of DOI strings.

    Returns:
        A list of dictionaries, where each dictionary contains
        information for a successfully found DOI. Returns an empty
        list if no data is found or an error occurs.
    """
    results = []
    print(f"Querying OpenAlex for {len(doi_list)} DOIs...")

    for doi in doi_list:
        print(f"\n--- Processing DOI: {doi} ---")
        try:
            # Construct the full DOI URL for querying
            # pyalex often works better with the full URL format
            # Lowercase is good practice
            full_doi_url = f"https://doi.org/{doi.lower()}"

            # Query OpenAlex Works endpoint by DOI
            # Using dict-like access requires the full URL
            work = Works()[full_doi_url]

            if not work:
                print(f"  DOI not found in OpenAlex: {doi}")
                continue

            # --- Extract desired information ---
            # Basic Info
            title = work.get('title', 'N/A')
            pub_year = work.get('publication_year', 'N/A')
            openalex_id = work.get('id', 'N/A')
            cited_by_count = work.get('cited_by_count', 0)
            journal_name = work.get('host_venue', {}).get(
                'display_name', 'N/A')
            journal_issn = work.get('host_venue', {}).get(
                'issn_l', 'N/A')  # Linking ISSN

            # Authors and Affiliations
            authors_info = []
            if work.get('authorships'):
                for authorship in work['authorships']:
                    author_name = authorship.get(
                        'author', {}).get('display_name', 'N/A')
                    author_orcid = authorship.get(
                        'author', {}).get('orcid')  # Might be None
                    institutions = authorship.get('institutions', [])
                    institution_names = [
                        inst.get('display_name', 'N/A') for inst in institutions]
                    authors_info.append({
                        "name": author_name,
                        "orcid": author_orcid,
                        "institutions": institution_names
                    })

            # Concepts (Topics/Subjects) - Let's take the top 3
            concepts_info = []
            if work.get('concepts'):
                # Sort concepts by score (descending) and take top 3
                sorted_concepts = sorted(
                    work['concepts'], key=lambda x: x.get('score', 0), reverse=True)
                for concept in sorted_concepts[:3]:
                    concepts_info.append({
                        "name": concept.get('display_name', 'N/A'),
                        "level": concept.get('level', 'N/A'),
                        "score": concept.get('score', 'N/A')
                    })

            # Store extracted data
            extracted_data = {
                "doi": doi,
                "openalex_id": openalex_id,
                "title": title,
                "publication_year": pub_year,
                "cited_by_count": cited_by_count,
                "journal": journal_name,
                "journal_issn_l": journal_issn,
                "authors": authors_info,
                "concepts": concepts_info,
                # Add more fields here if needed by exploring the 'work' object
                # e.g., 'type', 'abstract_inverted_index', 'referenced_works', 'related_works'
            }
            results.append(extracted_data)

            # --- Print some key retrieved info ---
            print(f"  Title: {title}")
            print(f"  Journal: {journal_name} ({pub_year})")
            print(f"  Authors: {', '.join([a['name'] for a in authors_info])}")
            print(
                f"  Top Concepts: {', '.join([c['name'] for c in concepts_info])}")
            print(f"  Cited By: {cited_by_count}")
            print(f"  OpenAlex ID: {openalex_id}")

        except Exception as e:
            print(f"  Error processing DOI {doi}: {e}")
            # This could be a network error, API error, or the DOI truly not existing

    print("\n--- Finished Querying ---")

    return results


# --- Main execution ---
if __name__ == "__main__":
    # Call the function with the list of DOIs
    retrieved_data = get_openalex_data_for_dois(dois_to_query)

    # You can now work with the 'retrieved_data' list, which contains
    # dictionaries of information for each successfully processed DOI.
    print(
        f"\nSuccessfully retrieved data for {len(retrieved_data)} out of {len(dois_to_query)} DOIs.")

    # Example: Print the title of the first result if available
    if retrieved_data:
        print(
            f"\nExample - Title of first result: {retrieved_data[0].get('title', 'N/A')}")

    # Example: Print author names and institutions for the second result if available
    if len(retrieved_data) > 1:
        print("\nExample - Authors/Institutions of second result:")
        for author_info in retrieved_data[1].get('authors', []):
            print(
                f"  - {author_info['name']} ({', '.join(author_info['institutions'])})")
