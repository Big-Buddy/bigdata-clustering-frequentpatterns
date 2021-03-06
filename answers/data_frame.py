import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import monotonically_increasing_id as mii

file_name = sys.argv[1]
desired_rows = int(sys.argv[2])

spark = SparkSession.builder.master("local").appName("lab1").getOrCreate()

lines = spark.read.text(file_name).rdd
parts = lines.map(lambda row: row.value.split(","))
plantRDD = parts.map(lambda p: Row(plant=p[0], items=p[1:]))

plants = spark.createDataFrame(plantRDD)
plants = plants.withColumn("id", mii())

reordered_plants = plants.select("id", "plant", "items")

reordered_plants.show(desired_rows)
