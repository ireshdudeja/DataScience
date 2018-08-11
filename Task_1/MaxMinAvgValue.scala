/*
Task Description:

What is the min/max/avg value for numeric columns (for reasonable columns, e.g. of the average tone)?
*/


var df = spark.read.option("sep", "\t").csv("/data/gdelt/events_2013-04-01_TO_2015-10-18.tsv")
//df.select("_c31", "_c32", "_c33", "_c34").show

var tempDF = df.select(df("_c31").cast("double"), df("_c32").cast("double"), df("_c33").cast("double"), df("_c34").cast("double"))
println(tempDF)
tempDF.printSchema()

// First Method
tempDF.describe("_c31", "_c32", "_c33", "_c34").show()


/*
// Second Method
val col1 = tempDF.select(mean("_c31"), min("_c31"), max("_c31"))
val col2 = tempDF.select(mean("_c32"), min("_c32"), max("_c32"))
val col3 = tempDF.select(mean("_c33"), min("_c33"), max("_c33"))
val col4 = tempDF.select(mean("_c34"), min("_c34"), max("_c34"))

var appended = col1.union(col2).union(col3).union(col4)

appended.toDF("Avg", "Min", "Max").show()
*/


System.exit(0)



/*
Output

+-------+------------------+------------------+------------------+------------------+
|summary|              _c31|              _c32|              _c33|              _c34|
+-------+------------------+------------------+------------------+------------------+
|  count|         127609835|         127609835|         127609835|         127609835|
|   mean|11.913727637058695|2.1018866766813074|11.643408895560441|0.8449689722037458|
| stddev| 63.06367421619985| 6.813362104849029| 60.03589235110214| 3.378494670726511|
|    min|               1.0|               1.0|               1.0| -53.9629005059022|
|    max|           72158.0|            1290.0|           72158.0|  80.2215189873418|
+-------+------------------+------------------+------------------+------------------+

 */
