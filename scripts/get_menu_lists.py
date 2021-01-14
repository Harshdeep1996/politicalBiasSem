import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver


# Initialize the driver
driver = webdriver.Chrome()
# Open provided link in a browser window using the driver
driver.get('https://mediabiasfactcheck.com/filtered-search/')

## Selecting the dropdown menu with "All Biases" (bias option)
driver.find_element_by_css_selector(
    "select#filter-bias > option").click()

FILTERING_REPORTING_OPTIONS = [
    'FAKE', 'LOW', 'MIXED', 'MOSTLY FACTUAL', 'HIGH', 'VERY HIGH']
all_data = []

for opt in FILTERING_REPORTING_OPTIONS:
    print('Start collecting data for the option: {}'.format(opt))
    driver.find_element_by_css_selector(
        "select#filter-reporting > option[value='{}']".format(opt)).click()

    soup = BeautifulSoup(driver.page_source,'html')
    ## Get all the rows of the mbfc table
    table = soup.find('table', id="mbfc-table")
    rows = table.findAll('tr')

    all_texts = []
    for r in rows:
        all_tds = r.findAll('td')
        if not all_tds:
            continue
        res = [
            all_tds[i].findAll('a')[0].text
            if i == 0 else all_tds[i].text for i in range(len(all_tds))
        ]
        all_texts.append(res)

    print('Got data for the option: {}'.format(opt))
    all_data.extend(all_texts)

## Storing the data from MBFC as CSV
df = pd.DataFrame(
    all_data,
    columns=['News Source', 'Bias', 'Reporting', 'Country', 'References']) 
df.to_csv('mbfc_data.csv', index=False)

## Closing the selenium connection
driver.quit()
