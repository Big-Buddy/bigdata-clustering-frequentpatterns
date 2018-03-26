import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import monotonically_increasing_id as mii, size
from pyspark.ml.fpm import FPGrowth

file_name = sys.argv[1]
desired_rows = int(sys.argv[2])
desired_supp = float(sys.argv[3])
desired_conf = float(sys.argv[4])

spark = SparkSession.builder.master("local").appName("lab1").getOrCreate()

lines = spark.read.text(file_name).rdd
parts = lines.map(lambda row: row.value.split(","))
plantRDD = parts.map(lambda p: Row(plant=p[0], items=p[1:]))

plants = spark.createDataFrame(plantRDD)
plants = plants.withColumn("id", mii())

reordered_plants = plants.select("id", "plant", "items")

fpGrowth = FPGrowth(itemsCol="items", minSupport=desired_supp, minConfidence=desired_conf)
model = fpGrowth.fit(reordered_plants)

output = model.freqItemsets
output = output.withColumn('itemsize', size(output.items))

output.sort("itemsize", "freq", ascending=False).select("items", "freq").show(desired_rows)
