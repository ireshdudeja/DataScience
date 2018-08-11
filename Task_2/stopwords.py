'''
Task Description:

Removing Stopwords
'''

from pyspark.sql import SparkSession
import pandas

spark =  SparkSession.builder.getOrCreate();
df = spark.read.json("/data/wikipedia/articles.json")

df = df.limit(1000)
pdDF = df.toPandas()
#print(pdDF['text'])
stop = open('stopwords.txt').read().splitlines()
#hdfs://172.21.249.73/data/stopwords.txt

pdDF['text'] = pdDF['text'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))
#print(pdDF['text'])
pdDF.to_json('stopwords_removed.json', orient='records')
