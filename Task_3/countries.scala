/*
Task Description:

Find all countries that are crossed by a straight line directly connecting two points (Geospatial data processing).
*/

import dbis.stark._
import dbis.stark.spatial.SpatialRDD._

val allCountries = sc.textFile("/data/world_level2.csv").map(line => line.split(';')).filter(arr => arr.length==6).map(arr => (STObject(arr(5)), (arr(0).toInt, arr(4)) ))
val coveredCountries = allCountries.intersects(STObject("LINESTRING ( 1.4143211 42.5378868, 14.3443989 55.1476253 )")).map(arr => (arr._2._2) )
coveredCountries.take(10).foreach(println)
/*
Andorra
Danmark
Espa?a
Deutschland
*/


val pointA = "9.363038 56.079044"
val pointB = "-3.385057 48.347321"
val pointC = "15.641353 40.924231"
val pointD = "31.569410 38.767526"
val pointE = "33.466313 49.500775"
var minCountriesCrossed : Long = 0;
var minCountriesNames = "";
var shortestPath = "";
val paths = List(pointA, pointB, pointC, pointD, pointE).permutations.toList
//println(paths.map(_.mkString(",")).mkString("\n"))
//path.mkString(",")

for (path <- paths) {
    var traversedCountries = allCountries.intersects(STObject("LINESTRING ("+path.mkString(",")+")")).map(arr => (arr._2._2) )
    var currentPath = path.mkString(" => ")
    var coveredCountriesCount = traversedCountries.count()
    var crossedCountriesList = traversedCountries.collect().mkString(",")

    if(minCountriesCrossed == 0 || coveredCountriesCount < minCountriesCrossed) {
        shortestPath = currentPath
        minCountriesCrossed = coveredCountriesCount
        minCountriesNames = crossedCountriesList
    }
}

println("Shortest Path is: "+ shortestPath)
println("Countries crossed : "+minCountriesCrossed)
println("Countries : "+ minCountriesNames)
//System.exit(0)

/*
Shortest Path is: 9.363038 56.079044 => 33.466313 49.500775 => 31.569410 38.767526 => 15.641353 40.924231 => -3.385057 48.347321
Countries crossed : 8
Countries : Danmark,Shqip?ria,Polska,??????,T?rkiye,????????,???????,Italia
*/
