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

package eu.stratosphere.pact.common.stub;

import java.util.Iterator;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * The ReduceStub must be extended to provide a reducer implementation which is
 * called by a Reduce PACT. By definition, the Reduce PACT calls the reduce
 * implementation once for each distinct key and all values that come with that key.
 * For details on the Reduce PACT read the documentation of the PACT programming model.
 * <p>
 * The ReduceStub extension must be parameterized with the types of its input and output keys and values.
 * <p>
 * For a reduce implementation, the <code>reduce()</code> method must be implemented.
 * 
 * @author Fabian Hueske
 */
public abstract class ReduceStub<IK extends Key, IV extends Value, OK extends Key, OV extends Value> extends
		SingleInputStub<IK, IV, OK, OV> {

	/**
	 * The central function to be implemented for a reducer. The function receives per call one
	 * key and all the values that belong to that key. Each key is guaranteed to be processed by exactly
	 * one function call across all involved instances across all computing nodes.
	 * 
	 * @param key
	 *        The input key.
	 * @param values
	 *        All values that belong to the given input key.
	 * @param out
	 *        The collector to hand results to.
	 */
	public abstract void reduce(IK key, Iterator<IV> values, Collector<OK, OV> out);

	/**
	 * No default implementation provided.
	 * This method must be overridden by reduce stubs that want to make use of the combining feature.
	 * In addition, the ReduceStub extending class must be annotated as Combinable.
	 * Note that this function must be implemented, if the reducer is annotated as combinable.
	 * <p>
	 * The use of the combiner is typically a pre-reduction of the data. It works similar as the reducer, only that is
	 * is not guaranteed to see all values with the same key in one call to the combine function. Since it is called
	 * prior to the <code>reduce()</code> method, input and output types of the combine method are the input types of
	 * the <code>reduce()</code> method.
	 * 
	 * @see eu.stratosphere.pact.common.contract.ReduceContract.Combinable
	 * @param key
	 *        The key to combine values for.
	 * @param values
	 *        The values to be combined. Unlike in the reduce method, these are not necessarily all values
	 *        belonging to the given key.
	 * @param out
	 *        The collector to write the result to.
	 */
	public void combine(IK key, Iterator<IV> values, Collector<IK, IV> out) {
		// to be implemented, if the reducer should use a combiner. Note that the combining method
		// is only used, if the stub class is further annotated with the annotation
		// @ReduceContract.Combinable
	}

}
