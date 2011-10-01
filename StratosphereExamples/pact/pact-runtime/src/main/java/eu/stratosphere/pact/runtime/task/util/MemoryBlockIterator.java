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

package eu.stratosphere.pact.runtime.task.util;

public interface MemoryBlockIterator<E> extends ResettableIterator<E> {

	/**
	 * Move the iterator to the next memory block. The next memory block starts at the first element that was not
	 * in the block before. A special case is when no record was in the block before, which happens when this
	 * function is invoked two times directly in a sequence, without calling hasNext() or next in between. Then
	 * the block moves one element.
	 * 
	 * @return true if a new memory block was loaded, false if there were no further records
	 */
	public boolean nextBlock();
}
