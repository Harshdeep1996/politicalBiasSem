## pyspark --packages com.databricks:spark-csv_2.10:1.5.0 --archives /home/harshdee/tldextract.zip#ENV_1
## hadoop fs -copyToLocal /user/harshdee/top_tlds_domain.csv .

import glob
import tldextract
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.sql import Row
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import udf, lit, col, length, expr
from pyspark.sql.types import ArrayType, StringType


INPUT_DATA = 'hdfs:///user/harshdee/citations_separated.parquet'
# OUTPUT_DATA = 'hdfs:///user/harshdee/newspapers_citations.parquet'

sc = SparkContext()
sqlContext = SQLContext(sc)
sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')
citations_separated = sqlContext.read.parquet(INPUT_DATA)

citations_separated = citations_separated.where(col("URL").isNotNull())

def get_top_domain(citation_url):
   ext = tldextract.extract(citation_url)
   return ext.domain.lower()

topdomain_udf = udf(get_top_domain)
citations_separated = citations_separated.withColumn('tld', topdomain_udf('URL'))

### FOR ALL URLs TLDs ###############

top_tlds_domain = citations_separated.groupby(
    citations_separated.tld).count().sort(col("count").desc())
top_tlds_domain.write.format('com.databricks.spark.csv').save('top_tlds_domain.csv')

## Continuing this with Pandas ##

li = []
all_files = [
  'top_tlds_domain.csv/part-00000-866084c7-32ea-4fc6-a2af-ed09bc5f89b8-c000.csv',
  'top_tlds_domain.csv/part-00001-866084c7-32ea-4fc6-a2af-ed09bc5f89b8-c000.csv',
  'top_tlds_domain.csv/part-00002-866084c7-32ea-4fc6-a2af-ed09bc5f89b8-c000.csv',
  'top_tlds_domain.csv/part-00003-866084c7-32ea-4fc6-a2af-ed09bc5f89b8-c000.csv',
]
for filename in all_files:
    # print(filename)
    df = pd.read_csv(filename, header=None, error_bad_lines=False)
    li.append(df)
tlds_pd  = pd.concat(li, axis=0, ignore_index=True).rename(columns={0:'tld_name', 1: 'count'})

## Getting the top 50 TLDs by domain
tlds_pd.nlargest(50, ['count'])[1:25] ## Top 25 -- remove Google
tlds_pd.nlargest(50, ['count'])[25:] ## From 25 to 50

## Settings for matplotlib
plt.rcParams.update({'font.size': 8})
mng = plt.get_current_fig_manager()
mng.resize(1300, 1024)

locs, labels = plt.xticks()
plt.setp(labels, rotation=45)

ax = sns.barplot(x="tld_name", y="count", data=tlds_pd.nlargest(50, ['count'])[1:25])
plt.show()
ax_two = sns.barplot(x="tld_name", y="count", data=tlds_pd.nlargest(50, ['count'])[25:])
plt.show()

### ONLY FOR SELECTED NEWSPAPERS ####

NEWSPAPERS = {'nytimes', 'bbc', 'washingtonpost', 'cnn', 'theguardian', 'huffingtonpost', 'indiatimes'}
newspaper_citations = citations_separated.where(col("tld").isin(NEWSPAPERS))

## Get the top 10 newspaper based on what we have
newspaper_citations.groupby(newspaper_citations.tld).count().sort(col("count").desc()).show()
