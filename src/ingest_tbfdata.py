import requests
from utils.logger_config import logger
from bs4 import BeautifulSoup

url = "https://data.bts.gov/d/kijm-95mr"


def get_download_links():
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup.find_all('a', href=True)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {url}: {e}")
        return []
    

if __name__ == "__main__":
    links = get_download_links()

