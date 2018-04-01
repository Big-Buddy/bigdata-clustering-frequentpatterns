import sys
from pyspark.sql import SparkSession

def dictionary_build(data):
	dict_buff = []
	for s in states:
		if s in data.items:
			dict_buff.append({'name' : s, data.plant : 1})
		else:
			dict_buff.append({'name' : s, data.plant : 0})
	return dict_buff

file_name = sys.argv[1]
pick_key = sys.argv[2]
pick_state = sys.argv[3]
output_file = sys.argv[4]

spark = SparkSession.builder.master("local").appName("lab1").getOrCreate()

lines = spark.read.text(file_name).rdd
parts = lines.map(lambda row: row.value.split(","))
plantRDD = parts.map(lambda p: Row(plant=p[0], items=p[1:]))
stateRDD = plantRDD.flatMap(lambda x: s for s in x.items)
global states
states = stateRDD.distinct().collect()
dictionaryRDD = plantRDD.flatMap(lambda x: dictionary_build)

dictionaryRDD = dictionaryRDD.filter(lambda x: x['name'] != pick_state)

output

for row in dictionaryRDD.collect():
	if (pick_key in row.keys()):
		output = row[pick_key]

with open(output_file, 'w') as f:
	f.write(output)
