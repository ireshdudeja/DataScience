'''
Task Description:

How many available (unavailable) URLs exist per domain in relation to all the
URLS of a domain?
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, BooleanType
from urlparse import urlparse
import pandas
import requests


spark =  SparkSession.builder.getOrCreate();
df = spark.read.csv('/data/gdelt/events_2013-04-01_TO_2015-10-18.tsv',sep="\t");
urls_df = df.select("_c57")
valid_urls_df = urls_df.filter(col("_c57").like("http%")).dropDuplicates()
valid_urls_df = valid_urls_df.limit(1000)
#print(valid_urls_df.count())
# Count: 27793154

def get_domain(url):
	return urlparse(url).netloc

domain_name_udf = udf(get_domain, StringType())

valid_urls_df = valid_urls_df.select(domain_name_udf("_c57").alias("domain"), "_c57")

urls_and_status_df = valid_urls_df.withColumn('status', lit(""))
urls_and_status_df.printSchema()

pdDF = urls_and_status_df.toPandas()

def is_available(url):
	request = requests.head(url)
	if request.status_code == 200:
		return True;
	else:
		return False;

for index, row in pdDF.iterrows():
    if is_available(row['_c57']):
        row['status'] = "True"
    else:
        row['status'] = "False"

#print(pdDF)

#pdDF = pdDF.groupBy("domain").count().sort(asc("domain"))
#pdDF.groupby(['domain','status'])['status'].count()
newPDFP = pdDF.groupby(['domain','status']).size().reset_index(name='counts')

print(newPDFP)

newPDFP.to_csv("availablity.csv", sep='\t')

'''
Output: /home/iresh11/availablity.csv
'''
