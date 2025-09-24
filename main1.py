import requests
from bs4 import BeautifulSoup  # parsing HTML and getting data from HTML
import urllib.request  # getting data from URL

# make a script to get H1 tag text from wikipedia.com H1 text


url = "https://wikipedia.com"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

div_tag = soup.find("div")
if div_tag:
    print("Wikipedia.com <div> text:", div_tag.get_text(strip=True))
else:
    print("No <div> tag found on wikipedia.com.")
