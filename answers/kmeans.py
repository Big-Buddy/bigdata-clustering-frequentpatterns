import sys
import random
from pyspark import SparkContext, SparkConf
import time

def dictionary_build(data):
	dict_buff = []
	for s in all_states:
		if s in data[1]:
			dict_buff.append({'name' : s, data[0] : 1})
		else:
			dict_buff.append({'name' : s, data[0] : 0})
	return dict_buff

def distance_combine(data):
	dict_keys = list(data.keys())
	if (dict_keys[0] == 'name'):
		el00 = data[dict_keys[0]]
		el01 = dict_keys[1]
		el1 = data[dict_keys[1]]
	else:
		el00 = data[dict_keys[1]]
		el01 = dict_keys[0]
		el1 = data[dict_keys[0]]
	return (el00, [el1])

def closest_centroid(data, centroids):
	distance_buffer = []
	for x in centroids:
		distance_buffer.append((x, euclid_sqr(x, data[1])))
	closest = min(distance_buffer, key = lambda x: x[1])
	return closest[0]

def euclid_sqr(a,b):
	distance = 0
	for p in range(len(a)):
		distance += (a[p]-b[p])**2
	return distance

def add_pairs(a,b):
	point_buffer = []
	for x in range(0,len(a)):
		point_buffer.append(a[x]+b[x])
	return point_buffer

def update_centroids(classRDD, counts):
	classes = classRDD.collect()
	

###SETUP

START_TIME=time.time()

file_name = sys.argv[1]
num_clusters = int(sys.argv[2])
random_seed = int(sys.argv[3])
output_file = sys.argv[4]
random.seed(random_seed)

global all_states
all_states = ["ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl",
           "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
           "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm",
           "ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd",
           "tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al",
           "bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk",
           "yt", "dengl", "fraspm"]

init_states = random.sample(all_states, num_clusters)

###PREPPING DATA

conf = SparkConf().setAppName("lab3").setMaster("local")
sc = SparkContext(conf=conf)

lines = sc.textFile(file_name)
parts = lines.map(lambda row: row.split(","))
plantRDD = parts.map(lambda p: (p[0], p[1:]))
dictionaryRDD = plantRDD.flatMap(dictionary_build)
distanceRDD = dictionaryRDD.map(distance_combine).reduceByKey(lambda a,b: a+b)

centroids = distanceRDD.filter(lambda x: x[0] not in init_states).map(lambda x: x[1]).collect()

old_centroids = []
classes = []
max_it = 100

###KMEANS ITERATIONS BEGIN HERE
for i in range(0, max_it):
	#find closest centroid to each point
	classRDD = distanceRDD.map(lambda x: (closest_centroid(x, centroids), x))
	#update centroids
	old_centroids = centroids
	class_sumRDD = classRDD.reduceByKey(lambda a,b: addpairs(a,b)).map(lambda x: x[1])
	centroids = []
	for points in class_sumRDD.collect():
		count = len(points)
		mean_buffer = []
		for x in points:
			mean_buffer.append(x/count)
		centroids.append(mean_buffer)
	#check for convergence, if convergence, reduce each class to its states and break
	if (sorted(old_centroids) == sorted(centroids)):
		classes = classRDD.reduceByKey(lambda a,b: str(a[1][0]) + ' ' + b[1][0])
		break

###WRITE CLASSES TO FILE
#with open(output_file, 'w') as f:
#	counter = 0
#	for c in classes:
#		f.write("* Class " + str(counter) + "\n")
#		for s in c:
#			f.write(s + " ")
#		f.write("\n")
#		counter += 1

print(time.time() - START_TIME)

