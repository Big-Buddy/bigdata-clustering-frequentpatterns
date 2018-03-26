import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import monotonically_increasing_id as mii, size, abs
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

total_plants = reordered_plants.count()

fpGrowth = FPGrowth(itemsCol="items", minSupport=desired_supp, minConfidence=desired_conf)
model = fpGrowth.fit(reordered_plants)

associationRules = model.associationRules
freqItemsets = model.freqItemsets

output = associationRules.join(freqItemsets, associationRules.consequent == freqItemsets.items)

output = output.withColumn("interest", abs(output.confidence-output.freq/total_plants))
output = output.withColumn("antecedentsize", size(output.antecedent))

output.sort("antecedentsize", "interest", ascending=False).select("antecedent", "consequent", "confidence", "items", "freq", "interest").show(desired_rows)
