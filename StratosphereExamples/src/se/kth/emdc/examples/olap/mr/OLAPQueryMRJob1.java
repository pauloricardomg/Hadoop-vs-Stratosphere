package se.kth.emdc.examples.olap.mr;

import java.util.Iterator;

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


public class OLAPQueryMRJob1 implements PlanAssembler, PlanAssemblerDescription {

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
	public static class OLAPJoinMapper extends MapStub<PactNull, PactString, PactString, PactString>
	{
		/*
		 * format of tuples in docs : URL | Content|\n
		 * example a tuple in docs: url_1|words words words|
		 * format of tuples in ranks: Rank | URL | Average Duration |\n
		 * example a tuple in ranks:  86|url_1|50
		 */
		Integer docs_KeyPosition = 0;
		Integer ranks_KeyPosition = 1;
		String Delimiter = "\\|";

		String[] keywords={ " editors ", " oscillations ", " convection " };
		Integer rank=50;

		@Override
		public void map(PactNull key, PactString value,
				Collector<PactString, PactString> out) {
			String[] items = value.toString().split(Delimiter);
			if(items.length == 2) // it's a docs-tuple
			{
				boolean ContainAllKeywords=true;
				for(String keyword: keywords)
				{
					if(!items[1].contains(keyword)) // check whether content contains keyword
					{
						ContainAllKeywords=false;
					}
				}
				if(ContainAllKeywords)
				{
					out.collect(new PactString(items[docs_KeyPosition]), value);
				}
			}
			else if(items.length == 3) // it's a ranks-tuple
			{
				if(Integer.parseInt(items[0]) > rank) // check whether the rank is bigger enough
				{
					out.collect(new PactString(items[ranks_KeyPosition]), value);
				}
			}

		};

	}

	public static class OLAPJoinReducer extends ReduceStub<PactString, PactString, PactNull, PactString> {

			String Delimiter = "\\|";
			
			@Override
			public void reduce(PactString key, Iterator<PactString> tuples,
					Collector<PactNull, PactString> out) {
				boolean rank_tuple_found = false;
				boolean doc_tuple_found = false;

				PactString rank_tuple = new PactString();
				while(tuples.hasNext())
				{
					PactString tuple=tuples.next();
					if(tuple.toString().split(Delimiter).length == 3)
					{
						rank_tuple_found = true;
						rank_tuple.setValue(tuple.getValue());
					}
					else if(tuple.toString().split(Delimiter).length == 2)
					{
						doc_tuple_found = true;
					}
				}

				if(rank_tuple_found && doc_tuple_found)
				{					
					//emit the rank tuple
					out.collect(PactNull.getInstance(), rank_tuple);
				}
				
			}
		}

		@Override
		public String getDescription() {
			return "Stratosphere-MR OLAP Job1";
		}



		@Override
		public Plan getPlan(String... args) throws IllegalArgumentException {
			if (args.length != 4) {
				System.err.println("Usage: " + OLAPQueryMRJob1.class.getName() + 
								   "<docs_ranks_file> <outputFolderPath> <num_mappers> <num_reducers>");
				
				System.err.println("<docs_ranks_file>: the path to the \"docs_ranks\" file in HDFS");
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
					OLAPJoinMapper.class, "Mapper Filtering Docs and Ranks");
			

			ReduceContract<PactString, PactString, PactNull, PactString> reducer = new ReduceContract<PactString, PactString, PactNull, PactString>(
					OLAPJoinReducer.class, "Reducer Joining");
			

			FileDataSinkContract<PactNull, PactString> out = new FileDataSinkContract<PactNull, PactString>(
					OLAPOutFormat.class, dataOutput, "Output To Disk");
			
			data.setDegreeOfParallelism(MapSubTasks);
			mapper.setDegreeOfParallelism(MapSubTasks);
			reducer.setDegreeOfParallelism(ReduceSubTasks);
			out.setDegreeOfParallelism(ReduceSubTasks);
			out.setInput(reducer);
			reducer.setInput(mapper);
			mapper.setInput(data);

			return new Plan(out, "OLAP Stratosphere-MR-1");
		}
	}
