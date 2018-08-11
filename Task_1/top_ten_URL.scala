/*
Task Description:

What are the top 10 domains present in the data set?
*/

import java.net.URL
import util.Try
import org.apache.spark.sql.functions.udf


var df = spark.read.option("sep", "\t").csv("/data/gdelt/events_2013-04-01_TO_2015-10-18.tsv")

var tempDF = df.filter($"_c57".like("http%"))


def getDomainNames(s: String): String = {

  new URL(s).getHost
}
val getDomainNamesUDF = udf[String, String](getDomainNames)


var tempDF2 = tempDF.select(getDomainNamesUDF(col("_c57"))).toDF("Domains")

tempDF2.groupBy(col("Domains")).count().orderBy(desc("count")).limit(10).show

//tempDF2.write.format("csv").save("/home/iresh11/top_domains.csv")

//tempDF2.write.csv("/home/iresh11/top_domains.csv")

System.exit(0)


/*
Output
+--------------------+-------+
|             Domains|  count|
+--------------------+-------+
|       allafrica.com|1334516|
| www.dailymail.co.uk|1021494|
|timesofindia.indi...| 720496|
|www.washingtonpos...| 641273|
|      article.wn.com| 620679|
| www.theguardian.com| 610445|
|      news.yahoo.com| 576750|
|  www.thenews.com.pk| 547390|
|www.business-stan...| 504327|
|    www.thehindu.com| 477455|
+--------------------+-------+

 */
