package se.kth.emdc.examples.kmeans;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
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
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.PactNull;

public class KmeansMapReduceCounterpart implements PlanAssembler, PlanAssemblerDescription{
	public static String CENTERS_FILENAME = null;
	public static List<Point> centers = null;
	public static FileWriter fw = null; 
	
	public static List<Point> getCenters() throws Exception{
		
		BufferedReader pointReader = new BufferedReader(new FileReader(CENTERS_FILENAME));
		
		LinkedList<Point> centersList = new LinkedList<Point>();
		
		String line;
		while((line = pointReader.readLine()) != null){
			centersList.add(new Point(line.split(" +")));
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

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void map(PactNull key, PactString value, Collector<PactString, PactString> out) {
//
//			try {
//				KmeansMapReduceCounterpart.fw.write(value.toString()+" is there anything before?\n");
//				KmeansMapReduceCounterpart.fw.flush();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			
//			if(centers == null){
//				try {
//					centers = getCenters();
//				} catch (Exception e) {
//					System.err.println("Could not read centers file. Empty centers list.");
//					e.printStackTrace();
//					centers = new LinkedList<Point>();
//				}
//			}
//			

			Point point = new Point(value.toString().split(" +"));
			
			
//			int minDist = Integer.MAX_VALUE;
//			Point closestCenter = null;
//			
//			for (Point center : centers) {
//				int dist = point.distanceTo(center);
//				if(dist < minDist){
//					minDist = dist;
//					closestCenter = center;
//				}
//			}
//			
//			out.collect(new PactString(closestCenter.toString()), new PactString(point.toString()));
			out.collect(new PactString(point.toString()), new PactString(point.toString()));

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
			
//				int x=0, y=0;
//				int length=0;
//				Point localCentroid = new Point(0,0);
//				while (values.hasNext()) {
//
					Point p = new Point(values.next().toString().split(" +"));
//					x += p.getX();
//					y += p.getY();
//					++length;
//				}
//				localCentroid.setX(x/length);
//				localCentroid.setY(y/length);
				
//				out.collect(key, new PactString(localCentroid.toString()));
				out.collect(new PactString(p.toString()), new PactString(p.toString()));
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
		KmeansMapReduceCounterpart.CENTERS_FILENAME = (args.length > 2 ? args[2] : "");
		String output    = (args.length > 3 ? args[3] : "");
		

		FileDataSourceContract<PactNull, PactString> data = new FileDataSourceContract<PactNull, PactString>(
				LineInFormat.class, dataInput, "Input Lines");
		data.setDegreeOfParallelism(noSubTasks);

		MapContract<PactNull, PactString, PactString, PactString> mapper = new MapContract<PactNull, PactString, PactString, PactString>(
				NearestCenterMapper.class, "Find nearest center for each point");
		mapper.setDegreeOfParallelism(noSubTasks);

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
		return "Parameters: [noSubStasks] [input] [output]";
	}
	
}
