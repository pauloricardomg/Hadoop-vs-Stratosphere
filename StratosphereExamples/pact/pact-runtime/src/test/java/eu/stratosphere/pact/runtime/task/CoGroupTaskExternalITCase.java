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

package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

public class CoGroupTaskExternalITCase extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(CoGroupTaskExternalITCase.class);
	
	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testExternalSortCoGroupTask() {

		int keyCnt1 = 16384;
		int valCnt1 = 4*2;
		
		int keyCnt2 = 65536*2;
		int valCnt2 = 1;
		
		super.initEnvironment(6*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		CoGroupTask testTask = new CoGroupTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockCoGroupStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + Math.max(keyCnt1, keyCnt2) - Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
				
	}
	
	public static class MockCoGroupStub extends CoGroupStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void coGroup(PactInteger key, Iterator<PactInteger> values1, Iterator<PactInteger> values2,
				Collector<PactInteger, PactInteger> out) {

			int val1Cnt = 0;
			
			while(values1.hasNext()) {
				val1Cnt++;
				values1.next();
			}
			
			while(values2.hasNext()) {
				PactInteger val2 =  values2.next();
				if(val1Cnt == 0) {
					out.collect(key,val2);
				} else {
					for(int i=0; i<val1Cnt; i++) {
						out.collect(key,val2);
					}
				}
			}
		}
	
	}
		
}
