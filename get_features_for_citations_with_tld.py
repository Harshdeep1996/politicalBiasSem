## Command to run on Cluster:
## spark-submit --master yarn --driver-memory 32G --num-executors 20 --executor-memory 16G 
## --executor-cores 6 get_features_for_citations_with_tld.py

from pyspark.sql.functions import col
from pyspark import SparkContext, SQLContext


INPUT_BASE_FEATURES = 'hdfs:///user/harshdee/base_features.parquet'
CITATIONS_WITH_TLD  = 'hdfs:///user/harshdee/citations_with_tld.parquet'

OUTPUT_DATA = 'hdfs:///user/harshdee/citations_tld_with_features.parquet'

sc = SparkContext()
sqlContext = SQLContext(sc)
sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

## Loading the features dataset with citations from TLD
base_features = sqlContext.read.parquet(INPUT_BASE_FEATURES)
citations_with_tld = sqlContext.read.parquet(CITATIONS_WITH_TLD)

## Get only the features which we are interested in
base_features = base_features.select(
    'page_title',
    col('id').alias('page_id'),
    col('citations_features._1').alias('retrieved_citation'),
    col('citations_features._2').alias('ref_index'),
    col('citations_features._3').alias('total_words'),
    col('citations_features._4._1').alias('neighboring_words'),
    col('citations_features._4._2').alias('neighboring_tags')
)

filtered = citations_with_tld.join(
    base_features,
    (base_features.page_id == citations_with_tld.id) &
        (base_features.retrieved_citation == citations_with_tld.citations),
    how='inner'
)

# Drop the column since there are 2 columns with citations
filtered = filtered.drop('retrieved_citation')
filtered.write.mode('overwrite').parquet(OUTPUT_DATA)
