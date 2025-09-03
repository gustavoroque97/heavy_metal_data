"""
Scraper for Death Metal and Black Metal bands from Wikipedia.

This script fetches band names from the following Wikipedia pages:
- Death Metal: !–K and L–Z
- Black Metal: 0–K and L–Z

It cleans the band names, removes section headers, merges into a single
DataFrame, and optionally filters out bands already present in a local dataset.

Author: Your Name
Date: 2025-09-03
"""

import requests
import re
import pandas as pd
from bs4 import BeautifulSoup

# ---------------------------
# Constants
# ---------------------------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/116.0 Safari/537.36"
    )
}

URLS = {
    "death_!K": "https://en.wikipedia.org/wiki/List_of_death_metal_bands,_!%E2%80%93K",
    "death_LZ": "https://en.wikipedia.org/wiki/List_of_death_metal_bands,_L%E2%80%93Z",
    "black_0K": "https://en.wikipedia.org/wiki/List_of_black_metal_bands,_0%E2%80%93K",
    "black_LZ": "https://en.wikipedia.org/wiki/List_of_black_metal_bands,_L%E2%80%93Z",
}

# ---------------------------
# Functions
# ---------------------------

def fetch_death_metal_bands(url: str) -> list:
    """
    Scrape Death Metal band names from a Wikipedia page using div-col structure.

    Args:
        url (str): Wikipedia page URL.

    Returns:
        list: List of band names as strings.
    """
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    bands = [
        li.get_text().split("[")[0].strip()
        for div in soup.find_all("div", class_="div-col")
        for li in div.find_all("li")
    ]
    return bands


def fetch_black_metal_bands(url: str) -> list:
    """
    Scrape Black Metal band names from a Wikipedia page.
    Filters out junk like section headers and navigation links later.

    Args:
        url (str): Wikipedia page URL.

    Returns:
        list: List of band names as strings.
    """
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    bands = [
        li.get_text().split("[")[0].strip()
        for li in soup.find_all("li")
    ]
    return bands


def clean_band_list(bands: list) -> list:
    """
    Clean a band list by removing section headers like "0–9", "A", "B", etc.,
    empty strings, and keeping only names starting with 0-9 or A-Z.

    Args:
        bands (list): List of band names.

    Returns:
        list: Cleaned list of band names.
    """
    cleaned = []
    for band in bands:
        band = band.strip()
        if not band:
            continue
        if band == "0–9":
            continue
        if re.fullmatch(r'[A-Z]', band):
            continue
        if re.match(r'^[0-9A-Z]', band):
            cleaned.append(band)
    return cleaned


def concat_death_black(death_metal: list, black_metal: list) -> pd.DataFrame:
    """
    Merge Death Metal and Black Metal lists into a single DataFrame.

    Args:
        death_metal (list): List of death metal band names.
        black_metal (list): List of black metal band names.

    Returns:
        pd.DataFrame: DataFrame with columns "Band" and "Genre".
    """
    df_death = pd.DataFrame({"Band": death_metal, "Genre": "Death Metal"})
    df_black = pd.DataFrame({"Band": black_metal, "Genre": "Black Metal"})
    return pd.concat([df_death, df_black], ignore_index=True)


def filter_existing_bands(bands_df: pd.DataFrame, training_data: pd.DataFrame) -> pd.DataFrame:
    """
    Remove bands that are already present in a local dataset CSV file.

    Args:
        bands_df (pd.DataFrame): DataFrame with scraped bands.
        training_data (pd.DataFrame): DataFrame used for training.

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    return bands_df[~bands_df["Band"].isin(training_data["Band"].to_list())]


# ---------------------------
# Main execution
# ---------------------------
if __name__ == "__main__":
    death_metal_bands = []
    black_metal_bands = []

    # Scrape Death Metal bands
    for key in ("death_!K", "death_LZ"):
        print(f"Scraping {key}...")
        death_metal_bands.extend(fetch_death_metal_bands(URLS[key]))
    death_metal_bands = sorted(set(death_metal_bands))

    # Scrape Black Metal bands
    for key in ("black_0K", "black_LZ"):
        print(f"Scraping {key}...")
        black_metal_bands.extend(fetch_black_metal_bands(URLS[key]))
    black_metal_bands = clean_band_list(black_metal_bands)
    black_metal_bands = sorted(set(black_metal_bands))

    # Merge into single DataFrame
    bands_df = concat_death_black(death_metal_bands, black_metal_bands)
    print(f"Total bands scraped: {len(bands_df)}")

    # Filter out bands already in AMG dataset (used for training)
    amg_data = pd.read_csv("../data/angrymetalguy_reviews_with_scores_clean.csv")
    df_test = filter_existing_bands(bands_df, amg_data)

    # Save to CSV
    df_test.to_csv("../data/df_test.csv", index=False)
    
    print("Scraping and cleaning complete!")