import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np


# ----------------------
# Constants
# ----------------------
SCORE_TAG_URLS = {
    'https://www.angrymetalguy.com/tag/50/': 5.0,
    'https://www.angrymetalguy.com/tag/45/': 4.5,
    'https://www.angrymetalguy.com/tag/40/': 4.0,
    'https://www.angrymetalguy.com/tag/35/': 3.5,
    'https://www.angrymetalguy.com/tag/30/': 3.0,
    'https://www.angrymetalguy.com/tag/2-5/': 2.5,
    'https://www.angrymetalguy.com/tag/20/': 2.0,
    'https://www.angrymetalguy.com/tag/15/': 1.5,
    'https://www.angrymetalguy.com/tag/1-0/': 1.0,
    'https://www.angrymetalguy.com/tag/0-5/': 0.5,
}


# ----------------------
# Scraper Class
# ----------------------
class AngryMetalGuyScraper:
    """Scraper for Angry Metal Guy album reviews."""

    def __init__(self, score_urls: dict, delay: float = 1.0):
        """
        Initialize the scraper.

        Args:
            score_urls (dict): Dictionary of tag URLs mapped to scores.
            delay (float): Delay in seconds between requests.
        """
        self.score_urls = score_urls
        self.delay = delay
        self.data = []

    def scrape(self):
        """Scrape all score tag pages."""
        for base_url, score in self.score_urls.items():
            page = 1
            while True:
                url = f"{base_url}page/{page}/"
                print(f"Scraping {url} (score {score})...")
                response = requests.get(url)
                if response.status_code != 200:
                    print(f"Page {page} returned status {response.status_code}, stopping for this score.")
                    break

                soup = BeautifulSoup(response.content, 'html.parser')
                reviews = soup.find_all('article', class_=lambda c: c and 'category-reviews' in c)

                if not reviews:
                    print("No more reviews found, moving to next score tag.")
                    break

                for review in reviews:
                    self.data.append(self._parse_review(review, score))

                page += 1
                time.sleep(self.delay)

    def _parse_review(self, review, score: float) -> dict:
        """Parse a single review HTML block."""
        # Band and album
        title_tag = review.find('h2', class_='entry-title')
        band_album = title_tag.get_text(strip=True) if title_tag else 'Unknown'
        if '–' in band_album:
            band, album = band_album.split('–', 1)
            band = band.strip()
            album = album.strip()
        else:
            band = band_album
            album = ''

        # Genres
        genre_links = review.find('div', class_='entry-meta').find_all('a')
        genres = [link.get_text(strip=True) for link in genre_links if '/tag/' in link.get('href', '')]

        return {
            'Band': band,
            'Album': album,
            'Genres': ', '.join(genres),
            'Score': score
        }

    def to_dataframe(self) -> pd.DataFrame:
        """Convert scraped data to a pandas DataFrame."""
        return pd.DataFrame(self.data)


# ----------------------
# Cleaner Class
# ----------------------
class AngryMetalGuyCleaner:
    """Data cleaner for Angry Metal Guy reviews."""

    @staticmethod
    def clean(df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the DataFrame:
            - Keep only the first genre
            - Remove "Review" from album names
            - Normalize Black Metal and Death Metal genres

        Args:
            df (pd.DataFrame): Raw scraped DataFrame.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        df = df.copy()
        # First genre only
        df['Genres'] = df['Genres'].apply(lambda x: x.split(',')[0] if pd.notna(x) else '')

        # Remove " Review" from album
        df['Album'] = df['Album'].str.replace(r'\s*Review$', '', regex=True)

        # Normalize genres
        df['Genres'] = np.where(
            df['Genres'].str.contains('Black Metal'), 'Black Metal',
            np.where(df['Genres'].str.contains('Death Metal'), 'Death Metal', df['Genres'])
        )

        return df


# ----------------------
# Main Execution
# ----------------------
if __name__ == '__main__':
    scraper = AngryMetalGuyScraper(SCORE_TAG_URLS)
    scraper.scrape()
    df_raw = scraper.to_dataframe()
    df_raw.to_csv('../data/angrymetalguy_reviews_with_scores_raw.csv', index=False)

    cleaner = AngryMetalGuyCleaner()
    df_clean = cleaner.clean(df_raw)
    df_clean.to_csv('../data/angrymetalguy_reviews_with_scores_clean.csv', index=False)

    print("Scraping and cleaning complete!")