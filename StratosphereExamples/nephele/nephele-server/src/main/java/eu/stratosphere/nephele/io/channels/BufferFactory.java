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

package eu.stratosphere.nephele.io.channels;

import java.nio.ByteBuffer;
import java.util.Deque;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.InternalBuffer;

public abstract class BufferFactory {

	public static Buffer createFromCheckpoint(int bufferSize, ChannelID sourceChannelID, long offset,
			FileBufferManager fileBufferManager) {

		final InternalBuffer internalBuffer = new CheckpointFileBuffer(bufferSize, sourceChannelID, offset,
			fileBufferManager);
		return new Buffer(internalBuffer);
	}

	public static Buffer createFromFile(int bufferSize, ChannelID sourceChannelID, FileBufferManager fileBufferManager) {

		final InternalBuffer internalBuffer = new FileBuffer(bufferSize, sourceChannelID, fileBufferManager);
		return new Buffer(internalBuffer);
	}

	public static Buffer createFromMemory(int bufferSize, ByteBuffer byteBuffer,
			Deque<ByteBuffer> queueForRecycledBuffers) {

		final InternalBuffer internalBuffer = new MemoryBuffer(bufferSize, byteBuffer, queueForRecycledBuffers);
		return new Buffer(internalBuffer);
	}
}
