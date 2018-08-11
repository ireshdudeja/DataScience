/*
Task Description:

Find the columns which are empty in all rows.
*/

val df = spark.read.option("sep", "\t").csv("/data/gdelt/events_2013-04-01_TO_2015-10-18.tsv")

for( colNumber <- 0 to df.columns.size - 1){
  var tempDF = df.where(col("_c" + colNumber).isNotNull)
  tempDF = tempDF.limit(1)
  //println(tempDF.show)
  if (tempDF.count == 0)
    println("_c" + colNumber + "is null column")
}

System.exit(0)


// Output
// No column is empty
