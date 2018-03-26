import sys
from pyspark.sql import SparkSession

file_name = sys.argv[1]
desired_rows = sys.argv[2]

spark = SparkSession.builder.master("local").appName("lab1").getOrCreate()

lines = spark.read.text(file_name).rdd
parts = lines.map(lambda row: row.value.split(","))
plantRDD = parts.map(lambda row: plant=row[0], items=row[1:])

plants = spark.createDataFrame(plantRDD)
plants = planets.withColumn("id", monotonically_increasing_id())

reordered_plants = plants.select("id", "plant", "items")

reordered_plants.show(desired_rows)