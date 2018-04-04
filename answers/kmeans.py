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

def euclid_sqr(a,b):
	distance = 0
	for p in range(len(a)):
		distance += (a[p]-b[p])**2
	return distance

def update_centroids(classes, distances):
	mean_buffer = []
	###LOOP OVER k CLASSES
	for c in classes:
		class_size = len(c)
		class_buffer = []
		###LOOP OVER EVERY STATE IN THE CLASS
		for x in c:
			###LOOP OVER ALL THE POINTS IN THAT STATE AND ADD TO THE BUFFER
			for y in distances[x]:
				if (len(class_buffer) == len(distances[x])):
					class_buffer[distances[x].index(y)] += y
				else:
					class_buffer.append(y)
		###GET AVERAGE OF EACH POINT
		for a in class_buffer:
			a /= class_size
		mean_buffer.append(class_buffer)
	return mean_buffer

def compare_centroids(old, new):
	flag = True
	for x in old:
		if(x != new[old.index(x)]):
			flag = False
	return flag

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

conf = SparkConf().setAppName("lab3").setMaster("local")
sc = SparkContext(conf=conf)

start_time= time.time()

lines = sc.textFile(file_name)
parts = lines.map(lambda row: row.split(","))
plantRDD = parts.map(lambda p: (p[0], p[1:]))
dictionaryRDD = plantRDD.flatMap(dictionary_build)
distanceRDD = dictionaryRDD.map(distance_combine)
distances = distanceRDD.reduceByKey(lambda a,b: a+b).collectAsMap()

classes = []

for i in init_states:
	classes.append([i])

###FIRST ITER ACCORDING TO RANDOMLY CHOSEN CENTROIDS
for s in all_states:
	dist_buffer = []
	if(s not in init_states):
		for i in init_states:
			dist_buffer.append(euclid_sqr(distances[s], distances[i]))
		class_ptr = dist_buffer.index(min(dist_buffer))
		classes[class_ptr].append(s)
old_centroids = []
new_centroids = update_centroids(classes, distances)
for c in classes:
	c = []
###ITER UNTIL CONVERGENCE
while(not compare_centroids(old_centroids, new_centroids)):
	for s in all_states:
		dist_buffer = []
		for i in new_centroids:
			dist_buffer.append(euclid_sqr(distances[s], i))
		class_ptr = dist_buffer.index(min(dist_buffer))
		classes[class_ptr].append(s)
	old_centroids = new_centroids
	new_centroids = update_centroids(classes, distances)
	for c in classes:
		c = []

###WRITE CLASSES TO FILE
for c in classes:
	c.sort()
classes.sort()

with open(output_file, 'w') as f:
	counter = 0
	for c in classes:
		f.write("* Class " + str(counter) + "\n")
		for s in c:
			f.write(s + " ")
		f.write("\n")
		counter += 1

