/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package se.kth.emdc.examples.kmeans.stratosphere;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.OutputContract;
import eu.stratosphere.pact.common.contract.OutputContract.UniqueKey;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;

/**
 * The K-Means cluster algorithm is well-known (see
 * http://en.wikipedia.org/wiki/K-means_clustering). KMeansIteration is a PACT
 * program that computes a single iteration of the k-means algorithm. The job
 * has two inputs, a set of data points and a set of cluster centers. A Cross
 * PACT is used to compute all distances from all centers to all points. A
 * following Reduce PACT assigns each data point to the cluster center that is
 * next to it. Finally, a second Reduce PACT compute the new locations of all
 * cluster centers.
 * 
 * File based on eu.stratosphere.pact.example.datamining.KMeansIteration
 * 
 * @author Fabian Hueske (original author)
 */
public class KmeansPACT implements PlanAssembler, PlanAssemblerDescription {

	public static class Distance implements Value {

		PactPoint clusterCenter;
		PactDouble distance;

		/**
		 * Initializes a blank Distance object. Required for deserialization.
		 */
		public Distance() {
		}

		/**
		 * Initialized the distance from a data point to a cluster center.
		 * 
		 * @param clusterCenter
		 *        The coordinate vector of the data point.
		 * @param clusterId
		 *        The id of the cluster center.
		 * @param distance
		 *        The distance from the data point to the cluster center.
		 */
		public Distance(PactPoint clusterCenter, PactDouble distance) {
			this.clusterCenter = clusterCenter;
			this.distance = distance;
		}

		/**
		 * Returns the coordinate vector of the data point.
		 * 
		 * @return The coordinate vector of the data point.
		 */
		public PactPoint getClusterCenter() {
			return clusterCenter;
		}

		/**
		 * Returns the distance from the data point to the cluster center.
		 * 
		 * @return The distance from the data point to the cluster center.
		 */
		public PactDouble getDistance() {
			return distance;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void read(DataInput in) throws IOException {
			clusterCenter = new PactPoint();
			clusterCenter.read(in);
			distance = new PactDouble();
			distance.read(in);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			clusterCenter.write(out);
			distance.write(out);
		}

	}

	/**
	 * Cross PACT computes the distance of all data points to all cluster
	 * centers. The SameKeyFirst OutputContract is annotated because PACTs
	 * output key is the key of the first input (data points).
	 * 
	 * @author Fabian Hueske
	 */
	@OutputContract.SameKeyFirst
	public static class ComputeDistance extends
			CrossStub<PactPoint, PactNull, PactPoint, PactNull, PactPoint, Distance> {

		/**
		 * Computes the distance of one data point to one cluster center and
		 * emits a key-value-pair where the id of the data point is the key and
		 * a Distance object is the value.
		 */
		@Override
		public void cross(PactPoint dataPoint, PactNull nullValue, PactPoint center, PactNull nullVal2, Collector<PactPoint, Distance> out) {

			// compute Euclidian distance and create Distance object
			Distance distance = new Distance(center, new PactDouble(dataPoint.euclidianDistanceTo(center)));

			// emit key-value-pair with distance information
			out.collect(dataPoint, distance);
		}
	}

	/**
	 * Reduce PACT determines the closes cluster center for a data point. This
	 * is a minimum aggregation. Hence, a Combiner can be easily implemented.
	 * 
	 * @author Fabian Hueske
	 */
	@Combinable
	public static class FindNearestCenter extends ReduceStub<PactPoint, Distance, PactInteger, CoordinatesSum> {

		/**
		 * Computes a minimum aggregation on the distance of a data point to
		 * cluster centers. Emits a key-value-pair where the key is the id of
		 * the closest cluster center and the value is the coordinate vector of
		 * the data point. The CoordVectorCountSum data type is used to enable
		 * the use of a Combiner for the second Reduce PACT.
		 */
		@Override
		public void reduce(PactPoint dataPoint, Iterator<Distance> distancesList, Collector<PactInteger, CoordinatesSum> out) {

			// initialize nearest cluster with the first distance
			Distance nearestCluster = null;
			if (distancesList.hasNext()) {
				nearestCluster = distancesList.next();
			} else {
				return;
			}

			// check all cluster centers
			while (distancesList.hasNext()) {
				Distance distance = distancesList.next();

				// compare distances
				if (distance.getDistance().getValue() < nearestCluster.getDistance().getValue()) {
					// if distance is smaller than smallest till now, update
					// nearest cluster
					nearestCluster = distance;
				}
			}

			// emit a key-value-pair where the cluster center id is the key and
			// the coordinate vector of the data point is the value. The
			// CoordVectorCountSum data type is used to enable the use of a
			// Combiner for the second Reduce PACT.
			out.collect(new PactInteger(nearestCluster.getClusterCenter().hashCode()), new CoordinatesSum(dataPoint.getCoords()));
		}

		/**
		 * Computes a minimum aggregation on the distance of a data point to
		 * cluster centers.
		 */
		@Override
		public void combine(PactPoint dataPoint, Iterator<Distance> distancesList, Collector<PactPoint, Distance> out) {

			// initialize nearest cluster with the first distance
			Distance nearestCluster = null;
			if (distancesList.hasNext()) {
				nearestCluster = distancesList.next();
			} else {
				return;
			}

			// check all cluster centers
			while (distancesList.hasNext()) {
				Distance distance = distancesList.next();

				// compare distance
				if (distance.getDistance().getValue() < nearestCluster.getDistance().getValue()) {
					// if distance is smaller than smallest till now, update
					// nearest cluster
					nearestCluster = distance;
				}
			}

			// emit nearest cluster
			out.collect(dataPoint, nearestCluster);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataPointInput = (args.length > 1 ? args[1] : "");
		String clusterInput = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for data point input
		FileDataSourceContract<PactPoint, PactNull> dataPoints = new FileDataSourceContract<PactPoint, PactNull> (
				PactPoint.LineInFormat.class, dataPointInput, "Data Points");
		dataPoints.setParameter(PactPoint.LineInFormat.RECORD_DELIMITER, "\n");
		dataPoints.setDegreeOfParallelism(noSubTasks);
		//dataPoints.setOutputContract(UniqueKey.class);

		// create DataSourceContract for cluster center input
		FileDataSourceContract<PactPoint, PactNull> clusterPoints = new FileDataSourceContract<PactPoint, PactNull> (
				PactPoint.LineInFormat.class, clusterInput, "Centers");
		clusterPoints.setParameter(PactPoint.LineInFormat.RECORD_DELIMITER, "\n");
		clusterPoints.setDegreeOfParallelism(1);
		//clusterPoints.setOutputContract(UniqueKey.class);

		// create CrossContract for distance computation
		CrossContract<PactPoint, PactNull, PactPoint, PactNull, PactPoint, Distance> computeDistance = new CrossContract<PactPoint, PactNull, PactPoint, PactNull, PactPoint, Distance>(
				ComputeDistance.class, "Compute Distances");
		computeDistance.setDegreeOfParallelism(noSubTasks);
		//?computeDistance.getCompilerHints().setAvgBytesPerRecord(48);

		// create ReduceContract for finding the nearest cluster centers
		ReduceContract<PactPoint, Distance, PactInteger, CoordinatesSum> findNearestClusterCenters = new ReduceContract<PactPoint, Distance, PactInteger, CoordinatesSum>(
				FindNearestCenter.class, "Find Nearest Centers");
		findNearestClusterCenters.setDegreeOfParallelism(noSubTasks);
		//findNearestClusterCenters.getCompilerHints().setAvgBytesPerRecord(48);

		// create ReduceContract for computing new cluster positions
		ReduceContract<PactInteger, CoordinatesSum, PactNull, PactPoint> recomputeClusterCenter = new ReduceContract<PactInteger, CoordinatesSum, PactNull, PactPoint>(
				KmeansMR.RecomputeClusterCenter.class, "Recompute Center Positions");
		recomputeClusterCenter.setDegreeOfParallelism(noSubTasks);
		//recomputeClusterCenter.getCompilerHints().setAvgBytesPerRecord(36);

		// create DataSinkContract for writing the new cluster positions
		FileDataSinkContract<PactNull, PactPoint> newClusterPoints = new FileDataSinkContract<PactNull, PactPoint>(
				PactPoint.LineOutFormat.class, output, "New Centers");
		newClusterPoints.setDegreeOfParallelism(noSubTasks);

		// assemble the PACT plan
		newClusterPoints.setInput(recomputeClusterCenter);
		recomputeClusterCenter.setInput(findNearestClusterCenters);
		findNearestClusterCenters.setInput(computeDistance);
		computeDistance.setFirstInput(dataPoints);
		computeDistance.setSecondInput(clusterPoints);

		// return the PACT plan
		return new Plan(newClusterPoints, "KMeans Iteration");

	}

	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [dataPoints] [clusterCenters] [output]";
	}

}
