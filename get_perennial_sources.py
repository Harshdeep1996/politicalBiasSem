import pandas as pd
import requests as r
from bs4 import BeautifulSoup

resp = r.get(
    'https://en.wikipedia.org/wiki/Wikipedia:'
    'Reliable_sources/Perennial_sources#Sources')
soup = BeautifulSoup(resp.text, 'html.parser')

tables = soup.find_all('table', 
    class_='wikitable sortable perennial-sources')

entry = []

for row in tables[0].tbody.findAll('tr'):
    name = labels = None
    if row.get('id'):
        name = row.get('id').replace('_', '')
    if row.findAll('td'):
        labels = [
            i.get('title').split('#')[1] 
            for i in row.findAll('td')[1].findAll('a')
        ]

    if name is None:
        continue

    entry.append([name, '\t'.join(labels)])

df = pd.DataFrame(entry, columns = ['Title', 'Labels'])
df.to_csv('perennial_sources.csv', index=False)
