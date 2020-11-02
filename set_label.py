## Command to run on Cluster:
## spark-submit --master yarn --driver-memory 32G --num-executors 20 --executor-memory 16G --executor-cores 6 set_label.py

from pyspark.sql.functions import col, udf
from pyspark import SparkContext, SQLContext


INPUT_DATA = 'hdfs:///user/harshdee/citations_bias_features.parquet'
OUTPUT_DATA = 'hdfs:///user/harshdee/mini_citations_bias_features.parquet'

sc = SparkContext()
sqlContext = SQLContext(sc)
sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

citations_bias_features = sqlContext.read.parquet(INPUT_DATA)

def get_label(bias_score):
   if bias_score > 0.5:
       return 'CONS'
   elif bias_score < -0.5:
       return 'LIBR'
   else:
       return 'MODR'

## Get the dataset and label it based on the political score
get_label_udf = udf(get_label)
citations_bias_features = citations_bias_features.withColumn(
    'label', get_label_udf('bias_score'))

## Get counts for each of the labels (PySpark)
## citations_bias_features.groupby(citations_bias_features['label']).count()
## Since the count of conservative is less, we take 220000 of each so that we have
## an equal dataset in terms of the class and not an imbalance
# +-----+-------+                                                                 
# |label|  count|
# +-----+-------+
# | MODR|3800812|
# | CONS| 220070|
# | LIBR|3841186|
# +-----+-------+

## Sample 220000 citations, sampling ratio calculate by 220,000/size
cons_citations = citations_bias_features.filter(
    citations_bias_features['label'] == 'CONS').sample(False, 0.9996819)
modr_citations = citations_bias_features.filter(
    citations_bias_features['label'] == 'MODR').sample(False, 0.0588823)
libr_citations = citations_bias_features.filter(
    citations_bias_features['label'] == 'LIBR').sample(False, 0.0582739)

## Merging the 3 datasets together
concat_result = cons_citations.union(modr_citations).union(libr_citations)
concat_result.write.mode('overwrite').parquet(OUTPUT_DATA)
