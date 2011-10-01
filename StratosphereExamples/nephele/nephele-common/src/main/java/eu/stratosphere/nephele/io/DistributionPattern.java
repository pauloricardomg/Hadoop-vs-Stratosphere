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
 * A distribution pattern determines which subtasks of a producing Nephele task a wired to which
 * subtasks of a consuming subtask. Custom distribution pattern can be provided by implementing
 * this interface.
 * 
 * @author warneke
 */
public interface DistributionPattern {

	/**
	 * Checks if two subtasks of different tasks should be wired.
	 * 
	 * @param nodeLowerStage
	 *        the index of the producing task's subtask
	 * @param nodeUpperStage
	 *        the index of the consuming task's subtask
	 * @param sizeSetLowerStage
	 *        the number of subtasks of the producing task
	 * @param sizeSetUpperStage
	 *        the number of subtasks of the consuming task
	 * @return <code>true</code> if a wire between the two considered subtasks should be created, <code>false</code>
	 *         otherwise
	 */
	boolean createWire(int nodeLowerStage, int nodeUpperStage, int sizeSetLowerStage, int sizeSetUpperStage);
}
