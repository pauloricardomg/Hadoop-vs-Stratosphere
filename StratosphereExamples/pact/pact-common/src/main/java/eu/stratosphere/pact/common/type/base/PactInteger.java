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

package eu.stratosphere.pact.common.type.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Key;

/**
 * Integer base type for PACT programs that implements the Key interface.
 * PactInteger encapsulates a Java primitive int.
 * 
 * @see eu.stratosphere.pact.common.type.Key
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class PactInteger implements Key {

	private int value;

	/**
	 * Initializes the encapsulated int with 0.
	 */
	public PactInteger() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated int with the provided value.
	 * 
	 * @param value Inital value of the encapsulated int.
	 */
	public PactInteger(final int value) {
		this.value = value;
	}
	
	/**
	 * Returns the value of the encapsulated int.
	 * 
	 * @return the value of the encapsulated int.
	 */
	public int getValue() {
		return this.value;
	}

	/**
	 * Sets the encapsulated int to the specified value.
	 * 
	 * @param value
	 *        the new value of the encapsulated int.
	 */
	public void setValue(final int value) {
		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.value = in.readInt();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(this.value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof PactInteger))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to N_Integer!");

		final int other = ((PactInteger) o).value;

		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 37;
		int result = 1;
		result = prime * result + this.value;
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final PactInteger other = (PactInteger) obj;
		if (this.value != other.value)
			return false;
		return true;
	}

}
