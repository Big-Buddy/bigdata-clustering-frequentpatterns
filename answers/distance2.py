import sys
from pyspark import SparkContext, SparkConf

def dictionary_build(data):
	dict_buff = []
	for s in states:
		if s[1] in data[1]:
			dict_buff.append({'name' : s[1], data[0] : 1})
		else:
			dict_buff.append({'name' : s[1], data[0] : 0})
	return dict_buff

def distance_combine(data):
	dict_keys = list(data.keys())
	if (dict_keys[0] == 'name'):
		#tuple0 = data[dict_keys[0]]
		tuple0 = dict_keys[1]
		tuple11 = data[dict_keys[1]]
	else:
		#tuple0 = data[dict_keys[1]]
		tuple0 = dict_keys[0]
		tuple11 = data[dict_keys[0]]
	return (tuple0, tuple11)

file_name = sys.argv[1]
state1 = sys.argv[2]
state2 = sys.argv[3]

conf = SparkConf().setAppName("lab3").setMaster("local")
sc = SparkContext(conf=conf)

lines = sc.textFile(file_name)
parts = lines.map(lambda row: row.split(","))
plantRDD = parts.map(lambda p: (p[0], p[1:]))
stateRDD = plantRDD.flatMap(lambda x: [('state', s) for s in x[1]])
global states
states = stateRDD.distinct().collect()
dictionaryRDD = plantRDD.flatMap(dictionary_build)

dictionaryRDD = dictionaryRDD.filter(lambda x: (x['name'] == state1 or x['name'] == state2))
distanceRDD = dictionaryRDD.map(distance_combine)
distanceRDD = distanceRDD.reduceByKey(lambda a,b: (a-b)**2)
distanceRDD = distanceRDD.map(lambda x: ('plantDist', x[1]))
print(distanceRDD.reduceByKey(lambda a,b: a+b).collect()[0][1])