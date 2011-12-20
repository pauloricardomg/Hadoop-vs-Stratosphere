package se.kth.emdc.examples.olap.mr;

import java.util.Iterator;
import java.util.LinkedList;



import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
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


public class OLAPQueryMRJob2 implements PlanAssembler, PlanAssemblerDescription {
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
	public static class OLAPOutFormat extends TextOutputFormat<PactNull, PactString> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public byte[] writeLine(KeyValuePair<PactNull, PactString> pair) {
			String value = pair.getValue().toString();
			String line = value + "\n";
			return line.getBytes();
		}

	}
	public static class OLAPJoinMapper extends MapStub<PactNull, PactString, PactString, PactString>{
		/*
		 * format of tuples from step1 : Rank | URL | Average Duration |\n
		 * example a tuple from step1: 86|url_1|50
		 * format of tuples in visits: IP|URL|DATE|1|2|3|4|5|6|\n
		 * example a tuple in visits:  133.33.250.203|url_910|2005-4-16|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|
		 */
		Integer step1_KeyPosition = 1;
		Integer visits_KeyPosition = 1;
		String Delimiter = "\\|";

		String date="2010";

		@Override
		public void map(PactNull key, PactString value, Collector<PactString, PactString> out){
			String[] items = value.toString().split(Delimiter);
			if(items.length == 3) // it's a step1-tuple
			{
				out.collect(new PactString(items[step1_KeyPosition]), value);
			}
			else if(items.length == 9) // it's a visits-tuple
			{
				if(items[2].substring(0, 4).equals(date)) // check whether the date is specified date
				{
					out.collect(new PactString(items[visits_KeyPosition]), value);
				}
			}
		};

	}

	public static class OLAPJoinReducer extends ReduceStub<PactString, PactString, PactNull, PactString> {
		String Delimiter = "\\|";

		@Override
		public void reduce(PactString key, Iterator<PactString> tuples,
				Collector<PactNull, PactString> out){
			boolean step1_tuple_found = false;
			boolean visits_tuple_found = false;
			LinkedList<PactString> outputTuples = new LinkedList<PactString>();
			while(tuples.hasNext())
			{
				PactString tuple=tuples.next();
				if(tuple.toString().split(Delimiter).length == 3)
				{
					step1_tuple_found = true;
					outputTuples.add(tuple);
				}
				if(tuple.toString().split(Delimiter).length == 9)
					visits_tuple_found = true;
			}

			if(step1_tuple_found && (!visits_tuple_found))
			{
				for(PactString tuple: outputTuples)
				{
					out.collect(PactNull.getInstance(), tuple);
				}
			}
		}
	}

	@Override
	public String getDescription() {
		return "Stratosphere-MR OLAP Job2";
	}



	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		if (args.length != 4) {
			System.err.println("Usage: " + OLAPQueryMRJob2.class.getName() + 
			"<step1_visits_file> <outputFolderPath> <num_mappers> <num_reducers>");

			System.err.println("<step1_visits_files>: the merged file from visits_file and step1_file");
			System.err.println("<outputFolderPath>: output folder in HDFS, must be empty before running the job");
			System.err.println("<num_mappers>: number of mappers");
			System.err.println("<num_reducers>: number of reducers");	
			System.exit(2);
		}
		String dataInput=args[0];
		String dataOutput=args[1];
		                   
		int MapSubTasks   = Integer.parseInt(args[2]);
		int ReduceSubTasks   = Integer.parseInt(args[3]);


		FileDataSourceContract<PactNull, PactString> data = new FileDataSourceContract<PactNull, PactString>(
				LineInFormat.class, dataInput, "Splitting Input To Mappers");
		

		MapContract<PactNull, PactString, PactString, PactString> mapper = new MapContract<PactNull, PactString, PactString, PactString>(
				OLAPJoinMapper.class, "Mapper Filtering Visits");
		

		ReduceContract<PactString, PactString, PactNull, PactString> reducer = new ReduceContract<PactString, PactString, PactNull, PactString>(
				OLAPJoinReducer.class, "Reducer Anti-joining");
		

		FileDataSinkContract<PactNull, PactString> out = new FileDataSinkContract<PactNull, PactString>(
				OLAPOutFormat.class, dataOutput, "Output To Disk");
		
		data.setDegreeOfParallelism(MapSubTasks);
		mapper.setDegreeOfParallelism(MapSubTasks);
		reducer.setDegreeOfParallelism(ReduceSubTasks);
		out.setDegreeOfParallelism(ReduceSubTasks);
		out.setInput(reducer);
		reducer.setInput(mapper);
		mapper.setInput(data);

		return new Plan(out, "OLAP Stratosphere-MR-2");
	}
}

