import sys
import random

RANGE_SIZE = 100
POINTS_PER_CLUSTER = 100
MAX_RANGE = 100000

# Generates N clusters of 30 points
def main():
	if len(sys.argv[1:]) != 2:
		print "Usage: " + sys.argv[0] + " <random seed> <number of clusters to generate>"
		sys.exit(1)
	
	seed = int(sys.argv[1:][0])
	num_clusters = int(sys.argv[1:][1])

	#print "Generating %d points. Random seed: %d" % (num_points, seed)

	random.seed(seed)

	c = 0

	for i in xrange(num_clusters):
		cluster = random.randint(0, MAX_RANGE)
		#print "-- Cluster %d - Start: %d:" % (i, cluster)
		for j in xrange(POINTS_PER_CLUSTER):
			print "%d\t%d\t%d" % (c, random.randint(cluster,cluster+RANGE_SIZE), random.randint(cluster,cluster+RANGE_SIZE))
			c = c+1


if __name__ == "__main__":
    main()
