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

def dictionary_combine(data):
	dict_keys = list(data.keys())
	if (dict_keys[0] == 'name'):
		tuple0 = data[dict_keys[0]]
		tuple1 = data[dict_keys[1]]
	else:
		tuple0 = data[dict_keys[1]]
		tuple1 = data[dict_keys[0]]
	return (tuple0, tuple1)

file_name = sys.argv[1]
state1 = sys.argv[2]
state2 = sys.argv[3]

conf = SparkConf().setAppName("lab1").setMaster("local")
sc = SparkContext(conf=conf)

lines = sc.textFile(file_name)
parts = lines.map(lambda row: row.split(","))
plantRDD = parts.map(lambda p: (p[0], p[1:]))
stateRDD = plantRDD.flatMap(lambda x: [('state', s) for s in x[1]])
global states
states = stateRDD.distinct().collect()
dictionaryRDD = plantRDD.flatMap(dictionary_build)

dictionaryRDD = dictionaryRDD.filter(lambda x: !=(x['name'] == state1 or x['name'] == state2))
distanceRDD = dictionaryRDD.map(dictionary_combine)
distanceRDD = distanceRDD.reduceByKey(lambda a,b: a+b)

print(distanceRDD.collect())
#state_points = distanceRDD.collect()
#point_x = state_points[0][1]
#point_y = state_points[1][1]

#distance = (point_x-point_y)**2