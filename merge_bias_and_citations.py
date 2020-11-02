## Command to run on Cluster:
## spark-submit --master yarn --driver-memory 32G --num-executors 20 --executor-memory 16G --executor-cores 6 merge_bias_and_citations.py

from pyspark.sql.functions import col
from pyspark import SparkContext, SQLContext


BIAS_SCORE_WITH_TLD = 'hdfs:///user/harshdee/bias_score_tld.parquet'
CITATION_TLD_WITH_FEATURES  = 'hdfs:///user/harshdee/citations_tld_with_features.parquet'

OUTPUT_DATA = 'hdfs:///user/harshdee/citations_bias_features.parquet'

sc = SparkContext()
sqlContext = SQLContext(sc)
sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

## Loading the bias tld score dataset and citation with tld dataset
bias_score_with_tld = sqlContext.read.parquet(BIAS_SCORE_WITH_TLD)
citation_tld_with_features = sqlContext.read.parquet(CITATION_TLD_WITH_FEATURES)

## Rename since we have 2 TLD columns
bias_score_with_tld = bias_score_with_tld.select(
    col("tld").alias("retrieved_tld"), col("political_bias").alias("bias_score")
)

filtered = citation_tld_with_features.join(
    bias_score_with_tld,
    bias_score_with_tld.retrieved_tld == citation_tld_with_features.tld,
    how='inner'
)

# Drop the column since there are 2 columns with citations
filtered = filtered.drop('retrieved_tld')
filtered.write.mode('overwrite').parquet(OUTPUT_DATA)
