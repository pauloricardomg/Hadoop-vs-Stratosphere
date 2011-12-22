package se.kth.emdc.mapreduce.match;

import java.util.Iterator;

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
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.DocsRanksDataInFormat;
import eu.stratosphere.pact.example.relational.util.StringTupleDataOutFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;

public class MatchMR implements PlanAssembler, PlanAssemblerDescription {

	public static class IdentityMapper extends MapStub<PactString, Tuple, PactString, Tuple>
	{
		@Override
		public void map(PactString key, Tuple value, Collector<PactString, Tuple> out) {
				out.collect(key, value);
		};

	}
	
	public static class IdentityReducer extends ReduceStub<PactString, Tuple, PactString, Tuple> {
		
		@Override
		public void reduce(PactString key, Iterator<Tuple> tuples, Collector<PactString, Tuple> out) {
			while(tuples.hasNext())
			{
				out.collect(key, tuples.next());
			}			
		}
	}

	public static class JoinReducer extends ReduceStub<PactString, Tuple, PactString, Tuple> {
			
			@Override
			public void reduce(PactString key, Iterator<Tuple> tuples,
					Collector<PactString, Tuple> out) {
				boolean rank_tuple_found = false;
				boolean doc_tuple_found = false;

				Tuple rank_tuple = null;
				while(tuples.hasNext())
				{
					Tuple tuple=tuples.next();
					if(tuple.getNumberOfColumns() == 3)
					{
						rank_tuple_found = true;
						rank_tuple = tuple;
					}
					else if(tuple.getNumberOfColumns() == 0)
					{
						doc_tuple_found = true;
					}
				}

				if(rank_tuple_found && doc_tuple_found)
				{					
					//emit the rank tuple
					out.collect(key, rank_tuple);
				}
				
			}
		}

		@Override
		public String getDescription() {
			return "Emulated Match Operator";
		}



		@Override
		public Plan getPlan(String... args) throws IllegalArgumentException {
			// parse job parameters
			String inputFolder   = (args.length > 0 ? args[0] : "");
			String outputFolder  = (args.length > 1 ? args[1] : "");
			int inputDegreeParallelism = (args.length > 2 ? Integer.parseInt(args[2]) : 0);
			int mapperDegreeParallelism = (args.length > 3 ? Integer.parseInt(args[3]) : 0);
			int reducerDegreeParallelism = (args.length > 4 ? Integer.parseInt(args[4]) : 0);
			int outputDegreeParallelism = (args.length > 5 ? Integer.parseInt(args[5]) : 0);


			FileDataSourceContract<PactString, Tuple> data = new FileDataSourceContract<PactString, Tuple>(
					DocsRanksDataInFormat.class, inputFolder, "Splitting Input To Mappers");
			

			MapContract<PactString, Tuple, PactString, Tuple> mapper = new MapContract<PactString, Tuple, PactString, Tuple>(
					IdentityMapper.class, "Identity Mapper Reading Docs and Ranks");
			

			ReduceContract<PactString, Tuple, PactString, Tuple> reducer = new ReduceContract<PactString, Tuple, PactString, Tuple>(
					JoinReducer.class, "Reducer Joining on the URL Key");
			

			FileDataSinkContract<PactString, Tuple> out = new FileDataSinkContract<PactString, Tuple>(
					StringTupleDataOutFormat.class, outputFolder, "Output To Disk");
			
			data.setDegreeOfParallelism(inputDegreeParallelism);
			mapper.setDegreeOfParallelism(mapperDegreeParallelism);
			reducer.setDegreeOfParallelism(reducerDegreeParallelism);
			out.setDegreeOfParallelism(outputDegreeParallelism);
			out.setInput(reducer);
			reducer.setInput(mapper);
			mapper.setInput(data);

			return new Plan(out, "Emulating the Match operator with MapReduce");
		}
	}
