import sys
import random

RANGE_SIZE = 500
MAX_RANGE = 10000000000

def main():
	if len(sys.argv[1:]) != 3:
		print "Usage: " + sys.argv[0] + " <random seed> <number of points> <number of clusters>"
		sys.exit(1)
	
	seed = int(sys.argv[1:][0])
	num_points = int(sys.argv[1:][1])
	num_clusters = int(sys.argv[1:][2])

	points_per_cluster = num_points/num_clusters

	#print "Generating %d points. Random seed: %d" % (num_points, seed)

	random.seed(seed)

	for i in xrange(num_clusters):
		p1 = random.randint(0, MAX_RANGE)
		p2 = random.randint(0, MAX_RANGE)
		#print "-- Cluster %d - Start: %d:" % (i, cluster)
		for j in xrange(points_per_cluster):
			print "%d\t%d" % (random.randint(p1,p1+RANGE_SIZE), random.randint(p2,p2+RANGE_SIZE))


if __name__ == "__main__":
    main()
