## Script to merge the scores for the URLs with common top level domain identifier
## this script requires the Media bias dataset
## scp -r ./bias_score_tld.parquet harshdee@hadoop.iccluster.epfl.ch:/home/harshdee/

import tldextract
import pandas as pd
from tqdm import tqdm

tqdm.pandas()

## Loading the dataset
political_bias_icwsm = pd.read_csv('political_bias_icwsm_2018.tsv', sep='\t')
print('Columns in the dataset: {}'.format(political_bias_icwsm.columns))

def get_top_domain(media_url):
   ext = tldextract.extract(media_url)
   return ext.domain.lower()

## Remove the entries which do not have any associated URL
political_bias_icwsm = political_bias_icwsm[political_bias_icwsm['url'] != '-']

## Get the top level domain for each associated URL
political_bias_icwsm['tld'] = political_bias_icwsm['url'].progress_apply(
    lambda x: get_top_domain(str(x)))
print('Columns in the dataset: {}'.format(political_bias_icwsm.columns))

## Group the results based on the TLD, and take the mean political bias score
non_aggregated_res = political_bias_icwsm[['tld', 'political_bias']]
non_aggregated_res = non_aggregated_res[non_aggregated_res['tld'] != '']
aggregated_res_by_tld = non_aggregated_res.groupby('tld').mean().reset_index()

## Save the file which can later be joined to the citation dataset file
aggregated_res_by_tld.to_parquet('bias_score_tld.parquet')
