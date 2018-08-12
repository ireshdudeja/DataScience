# DataScience
Tasks under "Data Science Lab Training" course, RCSE

### Working Environment:
- Hadoop Installation Mode: Fully Distributed Mode
- Nodes: A cluster of 16 nodes.
- File System: HDFS
- Distrubuted Computing Engine: Spark
- Languages: Scala, Python, R


## Task 1 - Data Cleaning & Analyzing
The event data for this task is extracted from the article texts by the GDELT project. News articles from various sources report about all types of events around the globe: from wars and political tension to music concerts and sport events. A subset (45 GB) of the available data is imported into the cluster.
### Task Description:
1. In a first step, columns should be analyzed. Are there columns which are empty in all rows?
2. What is the min/max/avg value for numeric columns (for reasonable columns, e.g. of the average tone)?
3. From thr field with a URL to the original article, What are the top 10 domains present in the data set?
4. How many available (unavailable) URLs exist per domain in relation to all the URLS of a domain?

## Task 2 – Text Mining
For this task a collection of documents is provided in a single JSON file. The file contains the texts of Wikipedia articles – one JSON object per line. One JSON object represents a Wikipedia article with its URL and text.
### Task Description:
1. In a first step the article texts need to be pre-processed to remove so-called stopwords.
2. To improve further analysis, one should lemmatize those words to their stem. (Stemming and Lemmatization)
3. Then compute the TF-IDF for the top X frequent words. Here X stands for some value that can be decided depending on the performance of the program.
4. After this, transform TF-IDF values for each document into a vector that can be clustered using k-Means.

## Task 3 – Geospatial Data Processing
In this task, geospatial data should be processed. For this purpose, two datasets with spatial objects are provided.
1. The first data set is the GDELT event dataset from previous task. This datasets contains location information for Actor1 (fields 46 and 47) and Actor2.
2. The second dataset was extracted from the OpenStreetMap.org project and contains border information for different countries in the world.
The file’s schema is as follows: id;level;relation_id;parent_id;name;poly. For this task, only id and/or name and poly are interesting. poly is a WKT String representing the border of the state.
Spark itself has no special understanding and methods to process spatial data. For this task, we are using STARK framework: https://github.com/dbis-ilm/stark/

### Task Description:
1. What countries are crossed by a line connecting the two points A and B?
* A. (1.4143211, 42.5378868)
* B. (14.3443989,55.1476253)

2. The given spatial points are:
* A (9.363038, 56.079044)
* B (-3.385057, 48.347321)
* C (15.641353, 40.924231)
* D (31.569410, 38.767526)
* E (33.466313, 49.500775)  
What is the best traversing pattern visiting the fewest amount of countries? What is the number?

