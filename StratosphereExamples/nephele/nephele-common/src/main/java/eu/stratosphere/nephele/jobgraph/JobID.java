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

package eu.stratosphere.nephele.jobgraph;

import eu.stratosphere.nephele.io.AbstractID;

/**
 * A <code>JobID</code> is a statistically unique identification number that unambiguously
 * identifies a job configuration.
 * 
 * @author warneke
 */
public final class JobID extends AbstractID {
	/**
	 * Constructs a new random ID from a uniform distribution.
	 */
	public JobID() {
		super();
	}

	/**
	 * Constructs a new ID with a specific bytes value.
	 * 
	 * @param bytes
	 *        the ID in byte representation
	 */
	public JobID(final byte[] bytes) {
		super(bytes);
	}
}
