import requests
from bs4 import BeautifulSoup

url = "https://wikipedia.com"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

for link in soup.find_all("a", href=True):
    print(f"Text: {link.get_text(strip=True)} | URL: {link['href']}")
