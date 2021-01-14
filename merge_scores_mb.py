## Script to merge the scores for the URLs with common top level domain identifier
## this script requires the Media bias dataset
## scp -r ./bias_score_tld.parquet harshdee@hadoop.iccluster.epfl.ch:/home/harshdee/


import tldextract
import numpy as np
import pandas as pd
from tqdm import tqdm

tqdm.pandas()

## Loading the dataset
political_bias_icwsm = pd.read_csv('political_bias_icwsm_2018.tsv', sep='\t')
print('Columns in the dataset: {}'.format(political_bias_icwsm.columns))

def get_top_domain(media_url):
   ext = tldextract.extract(media_url)
   return ext.domain.lower()

def get_subdomain(media_url):
    ext = tldextract.extract(media_url)
    if ext.subdomain:
        subdomain = ext.subdomain.lower()
        if subdomain == 'www':
            return get_suffixes(ext, media_url)
        else:
            return subdomain
    else:
        return get_suffixes(ext, media_url)

def get_suffixes(ext, media_url):
    main_part = '.'.join(part for part in ext if part)
    suffixes = media_url.split(main_part)[1].split('/')
    if len(suffixes) >= 2:
        return suffixes[1]
    else:
        return ''

## Remove the entries which do not have any associated URL
political_bias_icwsm = political_bias_icwsm[political_bias_icwsm['url'] != '-']

## Get the top level domain for each associated URL
political_bias_icwsm['url'] = political_bias_icwsm['url'].progress_apply(
    lambda x: str(x).replace(',', ''))
political_bias_icwsm['url'] = political_bias_icwsm['url'].progress_apply(
    lambda x: str(x).replace(';', ''))
## Remove TV Shows
political_bias_icwsm = political_bias_icwsm[political_bias_icwsm['category'] != 'TV Show']
political_bias_icwsm['tld'] = political_bias_icwsm['url'].progress_apply(
    lambda x: get_top_domain(x))
## Removing the ones with NaN
political_bias_icwsm = political_bias_icwsm[political_bias_icwsm['url'] != 'nan']
## Remove the ones for which there is an empty string
political_bias_icwsm = political_bias_icwsm[political_bias_icwsm['url'] != '']
political_bias_icwsm = political_bias_icwsm[political_bias_icwsm['tld'] != '']
political_bias_icwsm['sub_dom'] = political_bias_icwsm['url'].progress_apply(
    lambda x: get_subdomain(x))
print('Columns in the dataset: {}'.format(political_bias_icwsm.columns))

## Group the results based on the TLD, and take the mean political bias score
non_aggregated_res_with_sub = political_bias_icwsm[['tld', 'sub_dom', 'political_bias']]
non_aggregated_res_with_sub.replace('', 'Not_Found_', inplace=True)
non_aggregated_res_with_sub.replace(np.nan, 'Not_Found_', inplace=True)
aggregated_res = non_aggregated_res_with_sub[
    ['tld', 'political_bias']].groupby('tld').mean().reset_index()
aggregated_res['sub_dom_parent'] = 'Not_Found_Parent'

## Join by TLD on the averaged score
res = non_aggregated_res_with_sub.merge(aggregated_res, how='left', on=['tld'])
## cnt_tld = non_aggregated_res['tld'].value_counts()
## For taking the mean of the same TLDs
# aggregated_res_by_tld = non_aggregated_res.groupby('tld').mean().reset_index()

## Save the file which can later be joined to the citation dataset file
res.to_parquet('bias_score_tld.parquet')
