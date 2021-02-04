# Case 2 #

SparkConf().setAppName("Spark_DF")

#imports
from pyspark.sql import Window
from pyspark.sql.functions import col, rank

#Creating Df
df = spark.createDataFrame(
    [
        (1,'São Paulo', 'SP', '01/01/2021'),
        (2,'São Paulo', 'SP', '02/01/2021'),
        (3,'Guarulhos', 'SP', '03/01/2021'),
        (4,'Guarulhos', 'SP', '04/01/2021'),
        (5,'Campinas', 'SP', '05/01/2021'),
        (6,'Campinas', 'SP', '07/01/2021'),
        (7,'São Paulo', 'SP', '08/01/2021'),
        (8,'Guarulhos', 'SP', '10/01/2021'),
        (9,'Campinas', 'SP', '15/01/2021')
    ],
    ['Transacao','Municipio', 'Estado', 'DtAtualizacao']
)

# ranking transactions by municipio considering date
window = Window.partitionBy("Municipio").orderBy(col("DtAtualizacao").asc())
df1 = df.withColumn("OrdemTransacao",rank().over(window))
