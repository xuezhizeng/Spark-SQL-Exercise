import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc = SparkContext("local", "myapp")
sq = SQLContext(sc)

# Reading a csv file to DataFrame
rowDF = sq.read.format('com.databricks.spark.csv') \
    .options(header='true') \
    .load('NYPD_Motor_Vehicle_Collisions.csv')

# DROP
rowDF = rowDF.drop("TIME","ZIP CODE", "LATITUDE", "LONGITUDE", "LOCATION",\
                   "ON STREET NAME", "CROSS STREET NAME",\
                   "OFF STREET NAME", "NUMBER OF PEDESTRIANS INJURED",\
                   "NUMBER OF CYCLIST INJURED", "NUMBER OF MOTORIST INJURED",\
                   "NUMBER OF PEDESTRIANS KILLED", "NUMBER OF CYCLIST KILLED",\
                   "NUMBER OF MOTORIST KILLED")

# re-type
rowDF = rowDF.withColumn("PERSONS_INJURED", rowDF["NUMBER OF PERSONS INJURED"].cast(IntegerType()))
rowDF = rowDF.withColumn("PERSONS_KILLED", rowDF["NUMBER OF PERSONS KILLED"].cast(IntegerType()))
rowDF = rowDF.withColumn("KEY", rowDF["UNIQUE KEY"].cast(IntegerType()))
rowDF = rowDF.withColumn("DATE", (from_unixtime(unix_timestamp(rowDF["DATE"], format='MM/dd/yyyy'))).cast(DateType()))
rowDF = rowDF.drop("NUMBER OF PERSONS INJURED","NUMBER OF PERSONS KILLED", \
                   "UNIQUE KEY")

# Creating a view on DataFrame for Spark SQL operation
rowDF.createOrReplaceTempView("Collisions")

rowDF.printSchema()
# rowDF.select("*").limit(3).show()
# query1
# sq.sql("""
#        SELECT KEY, PERSONS_INJURED, PERSONS_KILLED FROM Collisions
#        """).write.csv(os.path.join("query1"), mode="overwrite")
#
# query 2
# query2 = rowDF.select(year(rowDF["DATE"]).alias('Year'), \
#              (rowDF["PERSONS_INJURED"] + rowDF["PERSONS_KILLED"]).alias("Incidents"))\
#     .groupBy('Year').sum("Incidents")
# print(type(query2))
# query2.\
#     repartition(1).\
#     write.\
#     mode("overwrite").\
#     format("com.databricks.spark.csv").\
#     option("header", "true").\
#     save("query2.csv")

# query 3
query3 = rowDF.select(year(rowDF["DATE"]).alias('Year'),\
                      quarter(rowDF["DATE"]).alias('Quarter'),\
             (rowDF["PERSONS_INJURED"] + rowDF["PERSONS_KILLED"]).alias("Incidents"))\
    .groupBy('Year', 'Quarter').sum("Incidents")
query3.\
    repartition(1).\
    write.\
    mode("overwrite").\
    format("com.databricks.spark.csv").\
    option("header", "true").\
    save("query3")

# query 4
query4 = rowDF.select(year(rowDF["DATE"]).alias('Year'),\
                      rowDF["Borough"].alias('Borough'),\
                      month(rowDF["DATE"]).alias('Month'),\
             (rowDF["PERSONS_INJURED"] + rowDF["PERSONS_KILLED"]).alias("Incidents"))\
    .groupBy('Borough', 'Year', 'Month').sum("Incidents")
query4.\
    repartition(1).\
    write.\
    mode("overwrite").\
    format("com.databricks.spark.csv").\
    option("header", "true").\
    save("query4")
