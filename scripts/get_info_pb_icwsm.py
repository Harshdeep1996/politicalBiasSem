## Script to discover the Media bias dataset out of the UFMG BR paper

import pandas as pd
from tqdm import tqdm

## Based on the paper
## https://homepages.dcc.ufmg.br/~lucaslima/pdf/ribeiro2018media.pdf
## very liberal (−2), liberal (−1), moderate (0), conservative(1), very conservative(2)

## Loading the dataset
political_bias_icwsm = pd.read_csv('political_bias_icwsm_2018.tsv', sep='\t')
print('Columns in the dataset: {}'.format(political_bias_icwsm.columns))

all_newspaper = political_bias_icwsm[political_bias_icwsm['category'] == 'Newspaper']
print('Total number of newspaper: {}\n'.format(all_newspaper['interest_name'].nunique()))

def tending_towards(score):
    if score[0] > 0:
        return 'Towards conservative'
    if score[0] < 0:
        return 'Towards liberal'

## From Newspaper, washington post, -0.5549
washington_post_score = political_bias_icwsm[
    political_bias_icwsm['interest_name'] == 'The Washington Post']
print('The {} is {} with inclination towards: {}'.format(
    list(washington_post_score['interest_name'])[0],
    list(washington_post_score['political_bias'])[0],
    tending_towards(list(washington_post_score['political_bias']))))

## The Guardian, -1.0838
## Taking the mean score since some there are mutliple entries from the same outlet
## with all of them having negative values
guardian_outlets = political_bias_icwsm[
    political_bias_icwsm['url'].apply(lambda x: 'theguardian' in str(x))]
print('The Guardian is has the score: {} and is tending: {}'.format(
    guardian_outlets['political_bias'].mean(),
    tending_towards([guardian_outlets['political_bias'].mean()])))

## New York Times, liberal leaning
nyt_outlets = political_bias_icwsm[
    political_bias_icwsm['url'].apply(lambda x: 'nytimes' in str(x))]
print('New York Times is has the score: {} and is tending: {}'.format(
    nyt_outlets['political_bias'].mean(),
    tending_towards([nyt_outlets['political_bias'].mean()])))


## breitbart.com - conservative/right leaning 1.5155
breitbart_score = list(political_bias_icwsm[
    political_bias_icwsm['url'].apply(lambda x: 'breitbart' in str(x))]['political_bias'])
print('The Brietbart is has the score: {} and is tending: {}'.format(
    breitbart_score[0], tending_towards(breitbart_score)))

## FOX news
foxnews_outlets = political_bias_icwsm[
    political_bias_icwsm['url'].apply(lambda x: 'foxnews' in str(x))]
print('The Fox News has the score: {} and is tending: {}'.format(
    foxnews_outlets['political_bias'].mean(),
    tending_towards([foxnews_outlets['political_bias'].mean()])))

## Data entries leaning towards the conservativism
cons_bias = political_bias_icwsm[political_bias_icwsm['political_bias'] > 0.5]
print(cons_bias.head())




