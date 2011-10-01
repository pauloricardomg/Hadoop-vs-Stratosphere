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

package eu.stratosphere.nephele.io;

/**
 * Represents a bipartite distribution pattern, i.e. given two distinct sets of vertices A and B, each vertex of set A
 * is wired to every vertex of set B and vice-versa.
 * 
 * @author warneke
 */
public class BipartiteDistributionPattern implements DistributionPattern {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean createWire(final int nodeLowerStage, final int nodeUpperStage, final int sizeSetLowerStage,
			final int sizeSetUpperStage) {

		return true;
	}

}
