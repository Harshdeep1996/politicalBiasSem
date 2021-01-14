from tqdm import tqdm
import json
import pandas as pd
from selenium import webdriver

tqdm.pandas()

citations_url_archive = pd.read_parquet('citations_url_archive.parquet/', engine='pyarrow')
archive_mapping = dict()
url_list = citations_url_archive['URL'].tolist()

for index_url in tqdm(range(len(url_list))):
    url_ = url_list[index_url]
    last_split = url_.split('/', 5)[-1]
    if len(last_split) == 5:
        archive_mapping[url_] = last_split
    else:
        try:
            driver = webdriver.Chrome()
            driver.get(url_)
            input_ = driver.find_element_by_name("q")
            archive_mapping[url_] = input_.get_attribute('value')
            print(input_.get_attribute('value'))
            driver.quit()
        except:
            archive_mapping[url_] = 'Not found\t' + url_
            print('Not found: ' + url_)

with open('archive_mapping.json', 'w') as f:
    json.dump(archive_mapping, f)
