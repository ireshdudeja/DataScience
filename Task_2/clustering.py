'''
Task Description:

K-means Clustering
'''
import pandas as pd
from pyspark.sql import SparkSession
from sklearn.feature_extraction.text import TfidfVectorizer #downloaded locally
from sklearn.cluster import KMeans
import numpy as np

spark =  SparkSession.builder.getOrCreate();
#To read from localfile system rather than hdfs for spark function
df = spark.read.json("file:///home/iresh11/lemmatized.json")

#df = df.limit(10)
pdDF = df.toPandas()

vectorizer = TfidfVectorizer()
km = KMeans(n_clusters = 3, init = 'k-means++', max_iter = 100, n_init = 1, verbose = True)

matrix = vectorizer.fit_transform(pdDF['text'])

#print "matrix"
km.fit(matrix)

print np.unique(km.labels_, return_counts=True)


# Getting the cluster centers
centroids = km.cluster_centers_ # km.cluster_centers_[0]
print "Cluster Centeroids"
print centroids


'''
// output
Initialization complete
Iteration  0, inertia 1901.631
Iteration  1, inertia 955.251
Iteration  2, inertia 951.967
Iteration  3, inertia 950.648
Iteration  4, inertia 949.936
Iteration  5, inertia 948.564
Iteration  6, inertia 946.264
Iteration  7, inertia 942.606
Iteration  8, inertia 938.121
Iteration  9, inertia 937.682
Iteration 10, inertia 937.115
Iteration 11, inertia 936.424
Iteration 12, inertia 935.793
Iteration 13, inertia 935.393
Iteration 14, inertia 935.153
Iteration 15, inertia 935.025
Iteration 16, inertia 934.944
Iteration 17, inertia 934.931

(array([0, 1, 2], dtype=int32), array([ 28, 644, 328]))

Cluster Centeroids
[[0.00000000e+00 0.00000000e+00 0.00000000e+00 ... 0.00000000e+00
  0.00000000e+00 0.00000000e+00]
 [5.82177804e-04 3.57038620e-03 0.00000000e+00 ... 0.00000000e+00
  2.22326810e-05 0.00000000e+00]
 [6.65194174e-04 9.11472739e-03 2.06793046e-05 ... 4.59920008e-05
  0.00000000e+00 2.00265954e-05]]
'''
