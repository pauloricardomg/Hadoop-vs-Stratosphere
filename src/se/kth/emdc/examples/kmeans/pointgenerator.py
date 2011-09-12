import sys
import random

MIN_RANGE=0
MAX_RANGE=1000000

def main():
	if len(sys.argv[1:]) != 2:
		print "Usage: " + sys.argv[0] + " <random seed> <number of points to generate>"
		sys.exit(1)
	
	seed = int(sys.argv[1:][0])
	num_points = int(sys.argv[1:][1])

	#print "Generating %d points. Random seed: %d" % (num_points, seed)

	random.seed(seed)

	for i in xrange(num_points):
		print "%d\t%d" % (random.randint(MIN_RANGE,MAX_RANGE), random.randint(MIN_RANGE,MAX_RANGE))


if __name__ == "__main__":
    main()
