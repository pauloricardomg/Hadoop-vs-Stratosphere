package se.kth.emdc.pact.match;

import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.DocsRanksDataInFormat;
import eu.stratosphere.pact.example.relational.util.StringTupleDataOutFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;


public class MatchPACT implements PlanAssembler, PlanAssemblerDescription {

	public static class JoinDocRanks extends MatchStub<PactString, Tuple, Tuple, PactString, Tuple> {

		/**
		 * Collects all entries from the documents and ranks relation where the
		 * key (URL) is identical. The output consists of the old key (URL) and
		 * the attributes of the ranks relation.
		 */
		public void match(PactString url, Tuple ranks, Tuple docs, Collector<PactString, Tuple> out) {
			
			// emit the key and the rank value
			// out.collect(url, ranks);
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
		int matchDegreeParallelism = (args.length > 5 ? Integer.parseInt(args[5]) : 0);
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
		MatchContract<PactString, Tuple, Tuple, PactString, Tuple> joinDocsRanks = new MatchContract<PactString, Tuple, Tuple, PactString, Tuple>(
				JoinDocRanks.class, "Join DocRanks");
		if(matchDegreeParallelism != 0)
			joinDocsRanks.setDegreeOfParallelism(matchDegreeParallelism);
		else
			joinDocsRanks.setDegreeOfParallelism(1);
		

		// Create DataSinkContract for writing the result of the OLAP query
		FileDataSinkContract<PactString, Tuple> result = new FileDataSinkContract<PactString, Tuple>(
				StringTupleDataOutFormat.class, output, "Result");
		if(outDegreeParallelism != 0)
			result.setDegreeOfParallelism(outDegreeParallelism);
		else
			result.setDegreeOfParallelism(1);

		// Assemble plan
		joinDocsRanks.setFirstInput(ranks);
		joinDocsRanks.setSecondInput(docs);
		result.setInput(joinDocsRanks);

		// Return the PACT plan
		return new Plan(result, "Match in PACT");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [docPath] [rankPath] [outPath] [docParallelismDegree] [rankParallelismDegree] [matchParallelismDegree] [outParallelismDegree]";
	}

}
