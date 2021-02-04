# Case 1 #

SparkConf().setAppName("Spark_FTP")

#Imports
from pyspark import SparkFiles

#Get FTP File
spark.sparkContext.addFile("ftp://user:xxx/data.zip")

df_data_path = SparkFiles.get("data.zip")

#Unzip File
%sh
unzip /tmp/data.zip

#Read File
df = spark.read.format("csv")
               .option("inferSchema", "true")
               .option("header","true")
               .load("/tmp/data.csv")





