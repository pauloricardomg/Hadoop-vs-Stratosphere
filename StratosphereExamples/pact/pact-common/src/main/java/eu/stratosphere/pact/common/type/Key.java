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

package eu.stratosphere.pact.common.type;

/**
 * This interface has to be implemented by all data types that act as key. Keys are used to establish
 * relationships between values. A key must always be {@link java.lang.Comparable} to other keys of
 * the same type.
 * <p>
 * This interface extends {@link eu.stratosphere.pact.common.type.Value} and requires to implement
 * the serialization of its value.
 * 
 * @see eu.stratosphere.pact.common.type.Value
 * @see eu.stratosphere.nephele.io.IOReadableWritable
 * @see java.lang.Comparable
 */
public interface Key extends Value, Comparable<Key> {
}
