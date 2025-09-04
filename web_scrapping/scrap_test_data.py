import re
from typing import List, Dict
import requests
import pandas as pd
from bs4 import BeautifulSoup


HEADERS = {"User-Agent": "Mozilla/5.0"}


class RedditDeathMetalScraper:
    """
    Scrapes a Reddit post containing a list of essential death metal albums.
    """

    def __init__(self, json_url: str):
        self.json_url = json_url

    def fetch_selftext(self) -> str:
        """Fetch the Reddit post selftext as a string."""
        res = requests.get(self.json_url, headers=HEADERS)
        res.raise_for_status()
        data = res.json()
        post_data = data[0]['data']['children'][0]['data']
        return post_data.get('selftext', '')

    def parse_albums(self) -> pd.DataFrame:
        """Parse the selftext and return a DataFrame with Band, Album, and Genre."""
        selftext = self.fetch_selftext()
        albums: List[Dict[str, str]] = []

        for line in selftext.splitlines():
            line = line.strip()
            if not line.startswith("*"):
                continue

            # Remove leading "* "
            line_content = line.lstrip("* ").strip()

            # Skip section headers like 1), 7a), etc.
            if re.match(r'^\d+[a-zA-Z]?\)', line_content):
                continue

            # Match Band - Album
            match = re.match(r'(.+?)\s*-\s*(.+)$', line_content)
            if match:
                band, album = match.groups()
                # Skip albums starting with numbers
                if re.match(r'^\d', album.strip()):
                    continue
                albums.append({
                    "Band": band.strip(),
                    "Album": album.strip(),
                    "Genre": "Death Metal"
                })

        return pd.DataFrame(albums)


class MetalAcademyBlackMetalScraper:
    """
    Scrapes a Metal Academy list of black metal albums.
    """

    def __init__(self, url: str):
        self.url = url

    def fetch_albums(self) -> pd.DataFrame:
        """Fetch and return a DataFrame with cleaned album names and genre."""
        res = requests.get(self.url, headers=HEADERS)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        release_divs = soup.find_all("div", class_="top-charts__release-name")
        albums: List[str] = []

        for div in release_divs:
            link = div.find("a")
            if link:
                albums.append(link.get_text(strip=True))

        # Remove years in parentheses
        cleaned_albums = [re.sub(r'\s*\(\d{4}\)$', '', album) for album in albums]

        return pd.DataFrame({
            "Album": cleaned_albums,
            "Genre": "Black Metal"
        })


def build_shuffled_dataset(death_df: pd.DataFrame, black_df: pd.DataFrame) -> pd.DataFrame:
    """
    Concatenate death metal and black metal DataFrames, drop Band column from death,
    shuffle rows, and reset index.
    """
    combined_df = pd.concat([death_df.drop(columns="Band"), black_df], ignore_index=True)
    return combined_df.sample(frac=1, random_state=42).reset_index(drop=True)


if __name__ == "__main__":
    # Scrape Reddit death metal albums
    reddit_url = "https://www.reddit.com/r/Deathmetal/comments/wo2870/a_guide_100_essential_death_metal_albums/.json"
    reddit_scraper = RedditDeathMetalScraper(reddit_url)
    death_albums_df = reddit_scraper.parse_albums()

    # Scrape Metal Academy black metal albums
    metalacademy_url = "https://metal.academy/lists/single/221"
    black_scraper = MetalAcademyBlackMetalScraper(metalacademy_url)
    black_albums_df = black_scraper.fetch_albums()

    # Combine and shuffle
    df_test = build_shuffled_dataset(death_albums_df, black_albums_df)

    # Optionally save to CSV
    df_test.to_csv("data/df_test.csv", index=False)
    print("Test dataset saved")