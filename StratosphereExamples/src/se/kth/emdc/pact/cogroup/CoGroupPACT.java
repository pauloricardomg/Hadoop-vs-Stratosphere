package se.kth.emdc.pact.cogroup;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.DocsRanksDataInFormat;
import eu.stratosphere.pact.example.relational.util.StringTupleDataOutFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;


public class CoGroupPACT implements PlanAssembler, PlanAssemblerDescription {


	public static class CoGroupDocRanks extends CoGroupStub<PactString, Tuple, Tuple, PactString, Tuple> {

		@Override
		public void coGroup(PactString url, Iterator<Tuple> ranks,
				Iterator<Tuple> docs, Collector<PactString, Tuple> out) {
			while(ranks.hasNext())
			{
				out.collect(url, ranks.next());
			}
			while(docs.hasNext())
			{
				out.collect(url, docs.next());
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		String docsInput   = (args.length > 0 ? args[0] : "");
		String ranksInput  = (args.length > 1 ? args[1] : "");
		String output      = (args.length > 2 ? args[2] : "");
		int docsDegreeParallelism = (args.length > 3 ? Integer.parseInt(args[3]) : 0);
		int ranksDegreeParallelism = (args.length > 4 ? Integer.parseInt(args[4]) : 0);
		int coGroupDegreeParallelism = (args.length > 5 ? Integer.parseInt(args[5]) : 0);
		int outDegreeParallelism = (args.length > 6 ? Integer.parseInt(args[6]) : 0);

		// Create DataSourceContract for documents relation
		FileDataSourceContract<PactString, Tuple> docs = new FileDataSourceContract<PactString, Tuple>(
				DocsRanksDataInFormat.class, docsInput, "Documents");
		docs.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		if(docsDegreeParallelism != 0)
			docs.setDegreeOfParallelism(docsDegreeParallelism);
		else
			docs.setDegreeOfParallelism(1);

		// Create DataSourceContract for ranks relation
		FileDataSourceContract<PactString, Tuple> ranks = new FileDataSourceContract<PactString, Tuple>(
				DocsRanksDataInFormat.class, ranksInput, "Ranks");
		ranks.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		if(ranksDegreeParallelism != 0)
			ranks.setDegreeOfParallelism(ranksDegreeParallelism);
		else
			ranks.setDegreeOfParallelism(1);


		// Create MatchContract to join the filtered documents and ranks
		// relation
		CoGroupContract<PactString, Tuple, Tuple, PactString, Tuple> coGroupDocsRanks = new CoGroupContract<PactString, Tuple, Tuple, PactString, Tuple>(
				CoGroupDocRanks.class, "Join DocRanks");
		if(coGroupDegreeParallelism != 0)
			coGroupDocsRanks.setDegreeOfParallelism(coGroupDegreeParallelism);
		else
			coGroupDocsRanks.setDegreeOfParallelism(1);
		

		// Create DataSinkContract for writing the result of the OLAP query
		FileDataSinkContract<PactString, Tuple> result = new FileDataSinkContract<PactString, Tuple>(
				StringTupleDataOutFormat.class, output, "Result");
		if(outDegreeParallelism != 0)
			result.setDegreeOfParallelism(outDegreeParallelism);
		else
			result.setDegreeOfParallelism(1);

		// Assemble plan
		coGroupDocsRanks.setFirstInput(ranks);
		coGroupDocsRanks.setSecondInput(docs);
		result.setInput(coGroupDocsRanks);

		// Return the PACT plan
		return new Plan(result, "Match in PACT");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [docPath] [rankPath] [outPath] [docParallelismDegree] [rankParallelismDegree] [coGroupParallelismDegree] [outParallelismDegree]";
	}

}
