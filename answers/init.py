from all_states import all_states as all_s
import sys
import random

num_states = int(sys.argv[1])
seed = int(sys.argv[2])

random.seed(seed)
init_states = random.sample(all_s, num_states)

for x in init_states:
	print(x)