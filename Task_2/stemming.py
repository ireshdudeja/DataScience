'''
Task Description:

Stemming and Lemmatization
'''
from pyspark.sql import SparkSession
import pandas
from nltk.stem.lancaster import LancasterStemmer
from nltk.stem import WordNetLemmatizer #copied folder nltk_data to home/iresh11

spark =  SparkSession.builder.getOrCreate();
#To read from localfile system rather than hdfs for spark function
df = spark.read.json("file:///home/iresh11/stopwords_removed.json")

#df = df.limit(10)
pdDF = df.toPandas()

stemmer = LancasterStemmer()
lemmatizer = WordNetLemmatizer()

#tokenizing
pdDF['text'] = pdDF['text'].str.split()
#stemming
pdDF['text'] = pdDF['text'].apply(lambda x: [stemmer.stem(y) for y in x])
#lemmatizng
pdDF['text'] = pdDF['text'].apply(lambda x: ' '.join([lemmatizer.lemmatize(y, pos='v') for y in x]))
#saving output to json file
pdDF.to_json('lemmatized.json', orient='records')
#print pdDF['text']
