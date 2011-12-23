/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package se.kth.emdc.pact.cross;

import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactNull;

public class CrossPACT implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * Builds a cartesian product over two Long datasets
	 *
	 */
	public static class LongLongCross extends
			CrossStub<PactLong, PactNull, PactLong, PactNull, PactLong, PactLong> {

		@Override
		public void cross(PactLong key, PactNull nullValue, PactLong value, PactNull nullVal2, Collector<PactLong, PactLong> out) {
			//out.collect(key, value);
			//empty to avoid output overhead
		}
	}

	/**
	 * Reads a file that contain a Long key
	 */
	public static class LongKeyInput extends TextInputFormat<PactLong, PactNull> {

		@Override
		public boolean readLine(KeyValuePair<PactLong, PactNull> pair, byte[] line) {
			String lineStr = new String(line);
			String[] split = lineStr.toString().split("\\s+");
			
			PactLong key = new PactLong(new Long(split[0]));
			
			pair.setKey(key);
			pair.setValue(new PactNull());
			return true;
		}
	}
	
	/**
	 * Reads a file that contain a Long value
	 */
	public static class LongKeyValueOutput extends TextOutputFormat<PactLong, PactLong> {

		@Override
		public byte[] writeLine(KeyValuePair<PactLong, PactLong> pair) {
			String result = pair.getKey().toString() + ", " + pair.getValue().toString() + "\n"; 
			return result.getBytes();
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		String keyInput = args[0];
		String valueInput = args[1];
		String output = args[2];

		// create DataSourceContract for data point input
		FileDataSourceContract<PactLong, PactNull> longKeys = new FileDataSourceContract<PactLong, PactNull> (
				LongKeyInput.class, keyInput, "Long keys");

		// create DataSourceContract for cluster center input
		FileDataSourceContract<PactLong, PactNull> longVals = new FileDataSourceContract<PactLong, PactNull> (
				LongKeyInput.class, valueInput, "Long values");

		// create CrossContract
		CrossContract<PactLong, PactNull, PactLong, PactNull, PactLong, PactLong> cross = new CrossContract<PactLong, PactNull, PactLong, PactNull, PactLong, PactLong>(
				LongLongCross.class, "Cross Keys and Values");
		
		// create DataSinkContract for writing the new cluster positions
		FileDataSinkContract<PactLong, PactLong> cartesianProduct = new FileDataSinkContract<PactLong, PactLong>(
				LongKeyValueOutput.class, output, "Cartesian Product");
		
		if(args.length > 3)
		{
			int keysSubTasks   = Integer.parseInt(args[3]);
			int valSubTasks   = Integer.parseInt(args[4]);
			int crossTasks   = Integer.parseInt(args[5]);
			int outSubTasks   = Integer.parseInt(args[6]);
			String streamedLoop = (args.length > 7 ? args[7] : "");
			
			longKeys.setDegreeOfParallelism(keysSubTasks);
			longVals.setDegreeOfParallelism(valSubTasks);
			cross.setDegreeOfParallelism(crossTasks);
			cartesianProduct.setDegreeOfParallelism(outSubTasks);
			
			if(streamedLoop.equals("true")){
				cross.getParameters().setString("LOCAL_STRATEGY","LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST");		
			}
		}
		
		// assemble the PACT plan
		cartesianProduct.setInput(cross);
		cross.setFirstInput(longKeys);
		cross.setSecondInput(longVals);

		// return the PACT plan
		return new Plan(cartesianProduct, "CROSS PACT version");

	}

	@Override
	public String getDescription() {
		return "Parameters: [keyInput] [valueInput] [output] [keysSubTasks] [valSubTasks] [crossTasks] [outSubTasks] {streamedLoop?}";
	}

}
