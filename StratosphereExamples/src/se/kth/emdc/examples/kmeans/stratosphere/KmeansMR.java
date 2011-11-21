package se.kth.emdc.examples.kmeans.stratosphere;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import se.kth.emdc.examples.kmeans.BasePoint;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;

public class KmeansMR implements PlanAssembler, PlanAssemblerDescription{
	public static String CENTERS_FILENAME_CONF = "CENTERS_FILENAME";

	public static class NearestCenterMapper extends MapStub<PactNull, CoordinatesSum, PactInteger, CoordinatesSum> {
		
		private List<BasePoint> centers = null;
		
		@Override
		public void configure(Configuration parameters) {
			String centersFilePath = parameters.getString(CENTERS_FILENAME_CONF, null);

			try {
				centers = BasePoint.getPoints(centersFilePath);
			} catch (Exception e) {
				System.err.println("Could not read centers file. Empty centers list.");
				e.printStackTrace();
				centers = new LinkedList<BasePoint>();
			}
		}

		@Override
		public void map(PactNull key, CoordinatesSum onePoint, Collector<PactInteger, CoordinatesSum> out) {
			double minDist = Double.MAX_VALUE;
			Integer closestCenterId = -1;

			for (int i = 0; i < centers.size(); i++) {
				BasePoint center = centers.get(i);
				double dist = onePoint.euclidianDistanceTo(center);
				if(dist < minDist){
					minDist = dist;
					closestCenterId = i;
				}
			}

			out.collect(new PactInteger(closestCenterId), onePoint);
		}
	}


	@Combinable
	public static class RecomputeClusterCenter extends ReduceStub<PactInteger, CoordinatesSum, PactNull, PactPoint> {

		@Override
		public void combine(PactInteger key, Iterator<CoordinatesSum> points, Collector<PactInteger, CoordinatesSum> out) {

			Long[] coordSums = null;
			long length=0;
			
			while(points.hasNext()){
				CoordinatesSum onePoint = points.next();
				Long[] pointCoords = onePoint.getCoords();
				if(coordSums == null){
					coordSums = new Long[pointCoords.length];
					for (int i=0; i<pointCoords.length; i++) {
						coordSums[i] = pointCoords[i];
					}
				} else {
					for (int i = 0; i < coordSums.length; i++) {
						coordSums[i] += pointCoords[i];
					}					
				}
				length += onePoint.getCount();
			}
			
			out.collect(key, new CoordinatesSum(coordSums, length));
		}

		@Override
		public void reduce(PactInteger key, Iterator<CoordinatesSum> points, Collector<PactNull, PactPoint> out) {
			Long[] coordSums = null;
			long length=0;
			
			while(points.hasNext()){
				CoordinatesSum onePoint = points.next();
				Long[] pointCoords = onePoint.getCoords();
				if(coordSums == null){
					coordSums = new Long[pointCoords.length];
					for (int i=0; i<pointCoords.length; i++) {
						coordSums[i] = pointCoords[i];
					}
				} else {
					for (int i = 0; i < coordSums.length; i++) {
						coordSums[i] += pointCoords[i];
					}					
				}
				length += onePoint.getCount();
			}
			
			Long[] centCoords = new Long[coordSums.length];
			for (int i = 0; i < centCoords.length; i++) {
				centCoords[i] = coordSums[i]/length;
			}
			
			PactPoint globalCentroid = new PactPoint(centCoords);
			
			out.collect(new PactNull(), globalCentroid);
		}

	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String centersFileName = (args.length > 2 ? args[2] : "");
		String output    = (args.length > 3 ? args[3] : "");

		FileDataSourceContract<PactNull, CoordinatesSum> data = new FileDataSourceContract<PactNull, CoordinatesSum>(
				CoordinatesSum.LineInFormat.class, dataInput, "Input Lines");
		data.setDegreeOfParallelism(noSubTasks);

		MapContract<PactNull, CoordinatesSum, PactInteger, CoordinatesSum> mapper = new MapContract<PactNull, CoordinatesSum, PactInteger, CoordinatesSum>(
				NearestCenterMapper.class, "Find nearest center for each point");
		mapper.setDegreeOfParallelism(noSubTasks);
		mapper.setParameter(CENTERS_FILENAME_CONF, centersFileName);

		ReduceContract<PactInteger, CoordinatesSum, PactNull, PactPoint> reducer = new ReduceContract<PactInteger, CoordinatesSum, PactNull, PactPoint>(
				RecomputeClusterCenter.class, "Compute the new centers");
		reducer.setDegreeOfParallelism(noSubTasks);

		FileDataSinkContract<PactNull, PactPoint> out = new FileDataSinkContract<PactNull, PactPoint>(
				PactPoint.LineOutFormat.class, output, "Centers");

		out.setDegreeOfParallelism(noSubTasks);

		out.setInput(reducer);
		reducer.setInput(mapper);
		mapper.setInput(data);

		return new Plan(out, "Counterpart of Kmeans to MapReduce ");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [input] [localLocalFile] [output]";
	}
}
