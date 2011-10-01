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

package eu.stratosphere.nephele.services.iomanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

public class MemoryIOWrapper implements IOReadableWritable
{
	private final MemorySegment memory;

	private int offset;

	private int length;

	public MemoryIOWrapper(MemorySegment memory) {
		this.memory = memory;
	}

	public void setIOBlock(int offset, int length) {
		this.offset = offset;
		this.length = length;
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.memory.put(in, this.offset, this.length);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.memory.get(out, this.offset, this.length);
	}
}
