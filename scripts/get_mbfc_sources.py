import pandas as pd
import requests as r
from tqdm import tqdm
from bs4 import BeautifulSoup

results = []
leaning = [
    'https://mediabiasfactcheck.com/center/',
    'https://mediabiasfactcheck.com/left/',
    'https://mediabiasfactcheck.com/leftcenter/',
    'https://mediabiasfactcheck.com/right-center/',
    'https://mediabiasfactcheck.com/right/',
    'https://mediabiasfactcheck.com/conspiracy/',
    'https://mediabiasfactcheck.com/fake-news/',
    'https://mediabiasfactcheck.com/pro-science/',
    'https://mediabiasfactcheck.com/satire/'
]

for index in tqdm(range(len(leaning))):
    resp = r.get(leaning[index])
    print('Getting resources: {}'.format(leaning[index]))
    soup = BeautifulSoup(resp.text, 'html.parser')

    table = soup.find('table', id='mbfc-table')
    rows = table.find_all('tr')

    for r_index in tqdm(range(len(rows))):
        title = rows[r_index].find('a').text
        source_url = rows[r_index].find('a')['href']
        source_resp = r.get(source_url)

        url = factual_status = ''

        try:
            url = source_resp.text.split('<p>Source:')[1].split('</a></p>')[0].split('>')[1]
        except Exception:
            url = 'Not Found URL'

        try:
            factual_status = source_resp.text.split('Factual Reporting:')[1][:16]
        except Exception:
            factual_status = 'Not Found Status'

        results.append([title, url, factual_status])

df = pd.DataFrame(results, columns=['Title', 'URL', 'Factual Label'])
df.to_csv('mbfc_data_scrapped.csv')
