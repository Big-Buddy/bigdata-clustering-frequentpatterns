import sys
from pyspark import SparkContext, SparkConf

def dictionary_build(data):
	dict_buff = []
	for s in states:
		if s[1] in data[1]:
			dict_buff.append({'name' : s, data[0] : 1})
		else:
			dict_buff.append({'name' : s, data[0] : 0})
	return dict_buff

file_name = sys.argv[1]
pick_key = sys.argv[2]
pick_state = sys.argv[3]
output_file = sys.argv[4]

conf = SparkConf().setAppName("lab1").setMaster("local")
sc = SparkContext(conf=conf)

lines = sc.textFile(file_name)
parts = lines.map(lambda row: row.split(","))
plantRDD = parts.map(lambda p: (p[0], p[1:]))
stateRDD = plantRDD.flatMap(lambda x: [('state', s) for s in x[1]])
global states
states = stateRDD.distinct().collect()
dictionaryRDD = plantRDD.flatMap(dictionary_build)

dictionaryRDD = dictionaryRDD.filter(lambda x: x['name'] != pick_state)

for row in dictionaryRDD.collect():
	if (pick_key in row.keys()):
		output = row[pick_key]

with open(output_file, 'w') as f:
	f.write(str(output))
