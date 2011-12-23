package se.kth.emdc.mapreduce.cogroup;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.DocsRanksDataInFormat;
import eu.stratosphere.pact.example.relational.util.StringTupleDataOutFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;

public class CoGroupR implements PlanAssembler, PlanAssemblerDescription {

	public static class IdentityReducer extends ReduceStub<PactString, Tuple, PactString, Tuple> {
		
		@Override
		public void reduce(PactString key, Iterator<Tuple> tuples, Collector<PactString, Tuple> out) {
			while(tuples.hasNext())
			{
				//out.collect(key, tuples.next());
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
			int reducerDegreeParallelism = (args.length > 3 ? Integer.parseInt(args[3]) : 0);
			int outputDegreeParallelism = (args.length > 4 ? Integer.parseInt(args[4]) : 0);


			FileDataSourceContract<PactString, Tuple> data = new FileDataSourceContract<PactString, Tuple>(
					DocsRanksDataInFormat.class, inputFolder, "Splitting Input To Mappers");
			

			ReduceContract<PactString, Tuple, PactString, Tuple> reducer = new ReduceContract<PactString, Tuple, PactString, Tuple>(
					IdentityReducer.class, "Identity Reducer Outputing CoGrouped Key/Value Pairs");
			

			FileDataSinkContract<PactString, Tuple> out = new FileDataSinkContract<PactString, Tuple>(
					StringTupleDataOutFormat.class, outputFolder, "Output To Disk");
			
			data.setDegreeOfParallelism(inputDegreeParallelism);
			reducer.setDegreeOfParallelism(reducerDegreeParallelism);
			out.setDegreeOfParallelism(outputDegreeParallelism);
			out.setInput(reducer);
			reducer.setInput(data);

			return new Plan(out, "Emulating the Match operator with MapReduce");
		}
	}
