## Command to run on Cluster:
## PYSPARK_PYTHON=/home/harshdee/mypython/bin/python spark-submit  --master yarn --driver-memory 32G --num-executors 20 --executor-memory 16G --executor-cores 6 --py-files ./dependencies.zip get_tld_for_main.py

import glob
import json
import tldextract

from pyspark.sql import Row
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import udf, lit, col, length, expr
from pyspark.sql.types import ArrayType, StringType


INPUT_DATA = 'hdfs:///user/harshdee/dataset.parquet'
OUTPUT_DATA = 'hdfs:///user/harshdee/citations_url_archive.parquet'
f = open('archive_mapping.json')
archive_mapping = json.load(f)

sc = SparkContext()
sqlContext = SQLContext(sc)
sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')
citations_separated = sqlContext.read.parquet(INPUT_DATA)

## Get only the citations which have the URL as NULL since it
## will act as the mapping between our dataset and political bias dataset
citations_separated = citations_separated.where(col("URL").isNotNull())

def get_top_domain(citation_url):
   ext = tldextract.extract(citation_url)
   return ext.domain.lower()

def get_sub_domain(citation_url):
    ext = tldextract.extract(citation_url)
    if not ext.domain:
        return ''

    if ext.subdomain:
        subdomain = ext.subdomain.lower()
        if subdomain == 'www':
            return get_suffixes(ext, citation_url)
        else:
            return subdomain
    else:
        return get_suffixes(ext, citation_url)

def get_suffixes(ext, citation_url):
    main_part = '.'.join(part for part in ext if part).encode('utf-8')
    citation_url = citation_url.encode('utf-8')
    suffixes = citation_url.split(main_part)[1].split('/')
    if len(suffixes) >= 2:
        return suffixes[1]
    else:
        return ''

def get_url_web_archive(citation_url):
    ## do 7 splits and take the last one
    if 'web.archive.org/' in citation_url: ## 36495
        return citation_url.split('/', 5)[-1]
    return citation_url

def get_url_archive_is(citation_url): ## 1689
    if (not (('archive.is/' in citation_url) or ('archive.li/' in citation_url))):
        return citation_url
    last_split = citation_url.split('/', 5)[-1]
    if len(last_split) != 5:
        return last_split
    else:
        return archive_mapping[citation_url]

## Get the dataset and extract TLDs for the URLs and get the necessary columns
topdomain_udf = udf(get_top_domain)
subdomain_udf = udf(get_sub_domain)
suffixes = udf(get_suffixes)
url_web_archive = udf(get_url_web_archive)
url_archive_is = udf(get_url_archive_is)

## look for webarchive expressions in the URL
## searchfor = ['archive.is/', 'archive.li/']
## citations_separated[citations_separated['URL'].rlike('|'.join(searchfor))]

## Get main URL from archive URLs and update the URL columns
citations_separated = citations_separated.withColumn('URL', url_archive_is('URL'))
citations_separated = citations_separated.withColumn('URL', url_web_archive('URL'))

citations_separated = citations_separated.withColumn('tld', topdomain_udf('URL'))
citations_separated = citations_separated.withColumn('sub_domain', subdomain_udf('URL'))
citations_separated = citations_separated.select([
    'URL', 'tld', 'citations', 'Title', 'sub_domain',
    'sections', 'type_of_citation', 'ID_list', 'id', 'r_id', 'r_parentid'])

citations_separated.write.mode('overwrite').parquet(OUTPUT_DATA)

