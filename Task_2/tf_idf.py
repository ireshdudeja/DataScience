'''
Task Description:

Compute the TF-IDF for the top X frequent words.
'''
import pandas as pd
from pyspark.sql import SparkSession
from sklearn.feature_extraction.text import TfidfVectorizer #downloaded locally

topX = 50
spark =  SparkSession.builder.getOrCreate();
#To read from localfile system rather than hdfs for spark function
df = spark.read.json("file:///home/iresh11/lemmatized.json")

#df = df.limit(10)
pdDF = df.toPandas()

vectorizer = TfidfVectorizer()

matrix = vectorizer.fit_transform(pdDF['text']).todense()
matrixDF = pd.DataFrame(matrix, columns=vectorizer.get_feature_names())
#print matrixDF
#axis=0 means along the rows, axis=1 means along the col
top_words = matrixDF.sum(axis=0).sort_values(ascending=False).head(topX)
#print top_words
#saving output to json file
top_words.to_csv("tf_idf.csv", sep=',', encoding='utf-8')
