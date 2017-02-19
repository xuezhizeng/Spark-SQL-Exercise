from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import time

sc = SparkContext("local", "myapp")
sq = SQLContext(sc)

# Reading a csv file to DataFrame
# Schema can be predefined or inferred.Check the syntax from official python Spark documentation

childbirthDF = sq.read.format('com.databricks.spark.csv') \
    .options(header='true') \
    .load('NationalNames.csv')
childbirthDF.printSchema()

# Creating a view on DataFrame for Spark SQL operation
childbirthDF.createOrReplaceTempView("Births")

# Running SQL query
# MAX year = 2014, MIN year = 1880
year = input("The year (from 1880 to 2014): ")
start = time.time()
totalBirthDF = sq.sql("Select SUM(Count) from Births WHERE Year='" + str(year) + "'")
# totalBirthDF.show()
totalBirthDF.repartition(1).\
    write.\
    mode("overwrite").\
    format("com.databricks.spark.csv").\
    option("header", "true").\
    save("query1")
end = time.time()
print("query1 time: " + str(end - start))

start = time.time()
totalBirthByGenderDF = sq.sql("Select Gender, SUM(Count) from Births WHERE Year='" + str(year) + "' GROUP BY Gender")
totalBirthByGenderDF.repartition(1). \
    write. \
    mode("overwrite"). \
    format("com.databricks.spark.csv"). \
    option("header", "true"). \
    save("query2")
# totalBirthByGenderDF.show()
end = time.time()
print("query2 time: " + str(end - start))

start = time.time()
top5DF = sq.sql(
    """
    SELECT Name, SUM(Count) AS NUM FROM Births WHERE Year=""" + str(year) + """ GROUP BY Name ORDER BY NUM DESC LIMIT 5
    """
)
top5DF.repartition(1). \
    write. \
    mode("overwrite"). \
    format("com.databricks.spark.csv"). \
    option("header", "true"). \
    save("query3")
# top5DF.show()
end = time.time()
print("query3 time: " + str(end - start))

start = time.time()
Name = input("The name (like Mary, John): ")
oneNameDF = sq.sql("SELECT Name, SUM(CAST(Count AS INT)) FROM Births WHERE Name='" + Name\
                   + "' GROUP BY Name")
oneNameDF.repartition(1). \
    write. \
    mode("overwrite"). \
    format("com.databricks.spark.csv"). \
    option("header", "true"). \
    save("query4")
# oneNameDF.show()
end = time.time()
print("query3 time: " + str(end - start))
