## Command to run on Cluster:
## PYSPARK_PYTHON=/home/harshdee/mypython/bin/python spark-submit 
## --master yarn --driver-memory 32G --num-executors 20 --executor-memory 16G 
## --executor-cores 6 --py-files ./dependencies.zip get_tld_for_main.py

import glob
import tldextract

from pyspark.sql import Row
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import udf, lit, col, length, expr
from pyspark.sql.types import ArrayType, StringType


INPUT_DATA = 'hdfs:///user/harshdee/dataset.parquet'
OUTPUT_DATA = 'hdfs:///user/harshdee/citations_with_tld.parquet'

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

## Get the dataset and extract TLDs for the URLs and get the necessary columns
topdomain_udf = udf(get_top_domain)
citations_separated = citations_separated.withColumn('tld', topdomain_udf('URL'))
citations_separated = citations_separated.select([
    'URL', 'tld', 'citations', 'Title',
    'sections', 'type_of_citation', 'ID_list', 'id', 'r_id', 'r_parentid'])

citations_separated.write.mode('overwrite').parquet(OUTPUT_DATA)

