## Command to run on Cluster:
## spark-submit --master yarn --driver-memory 32G --num-executors 20 --executor-memory 16G --executor-cores 6 merge_bias_and_citations.py

from pyspark.sql.functions import col, lit
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
    col("tld").alias("retrieved_tld"), 
    col("political_bias").alias("bias_score"),
    col("sub_dom").alias("sub_dom")
)

join_condition_subdom = (
    (bias_score_with_tld.retrieved_tld == citation_tld_with_features.tld)
    & (bias_score_with_tld.sub_dom == citation_tld_with_features.sub_domain)
)
## Joining first on TLD and subdom ## 934549
filtered_subdom = citation_tld_with_features.join(
    bias_score_with_tld, join_condition_subdom, how='inner')
filtered_subdom = filtered_subdom.withColumn('has_subdom', lit('yes'))

## Subtract these rows from the main dataframe
citations_with_no_match_subdom = citation_tld_with_features.subtract(
    filtered_subdom.select(citation_tld_with_features.columns))
citations_with_no_match_subdom = citations_with_no_match_subdom.withColumn(
    'sub_domain', lit(''))
join_condition_no_subdom = (
    (bias_score_with_tld.retrieved_tld == citations_with_no_match_subdom.tld)
    & (bias_score_with_tld.sub_dom == citations_with_no_match_subdom.sub_domain)
)
filtered_no_subdom = citations_with_no_match_subdom.join(
    bias_score_with_tld, join_condition_no_subdom, how='inner')
filtered_no_subdom = filtered_no_subdom.withColumn('has_subdom', lit('no'))

# Drop the column since there are 2 columns with citations
filtered_subdom = filtered_subdom.drop('retrieved_tld')
filtered_no_subdom = filtered_no_subdom.drop('retrieved_tld')
result = filtered_subdom.union(filtered_no_subdom)

## Get columns which are not joined
citations_with_no_match = citation_tld_with_features.subtract(
    result.select(citation_tld_with_features.columns))
print('\n\n')
print(citations_with_no_match.count())
print(citations_with_no_match.take(20))
print('\n\n')

# result.write.mode('overwrite').parquet(OUTPUT_DATA)
