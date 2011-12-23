package se.kth.emdc.mapreduce.cross;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import se.kth.emdc.pact.cross.CrossPACT;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactNull;

public class CrossMR implements PlanAssembler, PlanAssemblerDescription{
	public static String VALUES_FILENAME = "VALUES_FILENAME";

	public static class CrossKeysValuesMapper extends MapStub<PactLong, PactNull, PactLong, PactLong> {
		
		private List<Long> values = null;
		
		@Override
		public void configure(Configuration parameters) {
			String valuesFilePath = parameters.getString(VALUES_FILENAME, null);

			try {
				values = readValues(valuesFilePath);
			} catch (Exception e) {
				System.err.println("Could not read centers file. Empty centers list.");
				e.printStackTrace();
				values = new LinkedList<Long>();
			}
		}

		private static List<Long> readValues(String valuesFilePath) throws Exception {
			BufferedReader longReader = new BufferedReader(new FileReader(valuesFilePath));

			LinkedList<Long> valuesList = new LinkedList<Long>();

			String line;
			String[] split;
			while((line = longReader.readLine()) != null){
				split = line.split("\\s+");
				valuesList.add(new Long(split[0]));
			}
			
			return valuesList;
		}

		@Override
		public void map(PactLong key, PactNull value, Collector<PactLong, PactLong> out) {

			for (Long crossVal : values) {
				out.collect(new PactLong(key.getValue()), new PactLong(crossVal.longValue()));
			}
		}
	}

	public static class IdentityReducer extends ReduceStub<PactLong, PactLong, PactLong, PactLong> {

		@Override
		public void reduce(PactLong key, Iterator<PactLong> points, Collector<PactLong, PactLong> out) {
			//empty to avoid output overhead

//			while(points.hasNext()){
//				PactLong val = points.next();
//				out.collect(new PactLong(key.getValue()), new PactLong(val.getValue()));
//			}			
		}

	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		String keysInput = (args.length > 0 ? args[0] : "");
		String valuesLocalFileName = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");
		int mapSubTasks   = 0;
		int reduceSubTasks   = 0;
		int inSubTasks   = 0;
		int outSubTasks   = 0;

		FileDataSourceContract<PactLong, PactNull> data = new FileDataSourceContract<PactLong, PactNull>(
				CrossPACT.LongKeyInput.class, keysInput, "Long keys");

		MapContract<PactLong, PactNull, PactLong, PactLong> mapper = new MapContract<PactLong, PactNull, PactLong, PactLong>(
				CrossKeysValuesMapper.class, "Cross keys and values mapper");
		mapper.setParameter(VALUES_FILENAME, valuesLocalFileName);

		ReduceContract<PactLong, PactLong, PactLong, PactLong> reducer = new ReduceContract<PactLong, PactLong, PactLong, PactLong>(
				IdentityReducer.class, "Identity reducer");

		FileDataSinkContract<PactLong, PactLong> out = new FileDataSinkContract<PactLong, PactLong>(
				CrossPACT.LongKeyValueOutput.class, output, "Cartesian Product");
		
		if(args.length > 3)
		{
			inSubTasks   = Integer.parseInt(args[3]);
			mapSubTasks   = Integer.parseInt(args[4]);
			reduceSubTasks   = Integer.parseInt(args[5]);
			outSubTasks   = Integer.parseInt(args[6]);
			data.setDegreeOfParallelism(inSubTasks);
			mapper.setDegreeOfParallelism(mapSubTasks);
			reducer.setDegreeOfParallelism(reduceSubTasks);
			out.setDegreeOfParallelism(outSubTasks);
		}

		out.setInput(reducer);
		reducer.setInput(mapper);
		mapper.setInput(data);

		return new Plan(out, "CROSS MapReduce version");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [mapInput] [localFile] [output] [InSubTasks] [MapSubTasks] [ReduceSubTasks] [OutSubTasks]";
	}
}
