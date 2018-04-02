from all_states import all_states as all_s
import sys
import random

file_name = sys.argv[1]
num_states = int(sys.argv[2])
seed = int(sys.argv[3])

random.seed(seed)
init_states = random.sample(all_s, num_states)

for x in init_states:
	print(x + '\n')