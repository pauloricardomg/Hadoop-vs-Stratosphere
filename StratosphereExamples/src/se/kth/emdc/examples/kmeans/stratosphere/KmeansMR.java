package se.kth.emdc.examples.kmeans.stratosphere;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import se.kth.emdc.examples.kmeans.BasePoint;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;

public class KmeansMR implements PlanAssembler, PlanAssemblerDescription{
	public static String CENTERS_FILENAME_CONF = "CENTERS_FILENAME";

	public static List<BasePoint> getCenters(String centersFileName) throws Exception{
		
		BufferedReader pointReader = new BufferedReader(new FileReader(centersFileName));

		LinkedList<BasePoint> centersList = new LinkedList<BasePoint>();

		String line;
		while((line = pointReader.readLine()) != null){
			centersList.add(new BasePoint(line.split("\\s+")));
		}		
		return centersList;
	}

	/**
	 * Converts a input string (a line) into a KeyValuePair with the string
	 * being the key and the value being a zero Integer.
	 */
	public static class LineInFormat extends TextInputFormat<PactNull, PactString> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean readLine(KeyValuePair<PactNull, PactString> pair, byte[] line) {
			pair.setKey(new PactNull());
			pair.setValue(new PactString(new String(line)));
			return true;
		}

	}

	/**
	 * Writes a (String,Integer)-KeyValuePair to a string. The output format is:
	 * "&lt;key&gt;&nbsp;&lt;value&gt;\nl"
	 */
	public static class KmeansOutFormat extends TextOutputFormat<PactString, PactString> {

		/**
		 * {@inheritDoc}
		 */

		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactString> pair) {
			// TODO Auto-generated method stub
			String key = pair.getKey().toString();
			String value = pair.getValue().toString();
			String line = key + " " + value + "\n";
			return line.getBytes();
		}

	}

	/**
	 * Converts a (String,Integer)-KeyValuePair into multiple KeyValuePairs. The
	 * key string is tokenized by spaces. For each token a new
	 * (String,Integer)-KeyValuePair is emitted where the Token is the key and
	 * an Integer(1) is the value.
	 */
	public static class NearestCenterMapper extends MapStub<PactNull, PactString, PactString, PactString> {
		
		String centersFilePath = "";
		private List<BasePoint> centers = null;
		
		@Override
		public void configure(Configuration parameters) {
			this.centersFilePath = parameters.getString(CENTERS_FILENAME_CONF, null);
		}
		
		/**
		 * {@inheritDoc}
		 */
		public void map(PactNull key, PactString value, Collector<PactString, PactString> out) {
			if(centers == null){
				try {
					centers = getCenters(centersFilePath);
				} catch (Exception e) {
					System.err.println("Could not read centers file. Empty centers list.");
					e.printStackTrace();
					centers = new LinkedList<BasePoint>();
				}
			}


			BasePoint point = null;
			try {
				point = new BasePoint(value.toString().split("\\s+"));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


			double minDist = Double.MAX_VALUE;
			BasePoint closestCenter = null;

			for (BasePoint center : centers) {
				double dist = point.euclidianDistanceTo(center);
				if(dist < minDist){
					minDist = dist;
					closestCenter = center;
				}
			}
			
			out.collect(new PactString(closestCenter.toString()), new PactString(point.toString()));

		}
	}

	/**
	 * Counts the number of values for a given key. Hence, the number of
	 * occurences of a given token (word) is computed and emitted. The key is
	 * not modified, hence a SameKey OutputContract is attached to this class.
	 */
	@SameKey
	@Combinable
	public static class KmeansReducer extends ReduceStub<PactString, PactString, PactString, PactString> {


		/**
		 * {@inheritDoc}
		 */
		@Override
		public void reduce(PactString key, Iterator<PactString> values, Collector<PactString, PactString> out) {
			long x=0, y=0;
			long length=0;

			while (values.hasNext()) {

				BasePoint p = null;
				try {
					p = new BasePoint(values.next().toString().split("\\s+"));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				x += p.getCoords()[0];
				y += p.getCoords()[1];
				++length;
			}
			
			Long[] centroidCoords = new Long[2];
			centroidCoords[0] = x/length;
			centroidCoords[1] = y/length;
			
			BasePoint localCentroid = new BasePoint(centroidCoords);

			out.collect(key, new PactString(localCentroid.toString()));
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void combine(PactString key, Iterator<PactString> values, Collector<PactString, PactString> out) {

			this.reduce(key, values, out);
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


		FileDataSourceContract<PactNull, PactString> data = new FileDataSourceContract<PactNull, PactString>(
				LineInFormat.class, dataInput, "Input Lines");
		data.setDegreeOfParallelism(noSubTasks);

		MapContract<PactNull, PactString, PactString, PactString> mapper = new MapContract<PactNull, PactString, PactString, PactString>(
				NearestCenterMapper.class, "Find nearest center for each point");
		mapper.setDegreeOfParallelism(noSubTasks);
		mapper.setParameter(CENTERS_FILENAME_CONF, centersFileName);

		ReduceContract<PactString, PactString, PactString, PactString> reducer = new ReduceContract<PactString, PactString, PactString, PactString>(
				KmeansReducer.class, "Compute the new centers");
		reducer.setDegreeOfParallelism(noSubTasks);

		FileDataSinkContract<PactString, PactString> out = new FileDataSinkContract<PactString, PactString>(
				KmeansOutFormat.class, output, "Centers");

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
