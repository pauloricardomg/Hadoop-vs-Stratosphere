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

package eu.stratosphere.pact.runtime.sort;

import java.util.Iterator;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;

/**
 * The SortMerger interface representing the public interface to all specific Sort-Merge implementations.
 * 
 * @author Erik Nijkamp
 * @param <K>
 *        Key class
 * @param <V>
 *        value class
 */
public interface SortMerger<K extends Key, V extends Value> extends CloseableInputProvider<KeyValuePair<K, V>>
{
	Iterator<KeyValuePair<K, V>> getIterator() throws InterruptedException;
}
