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

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.Deserializer;
import eu.stratosphere.nephele.services.iomanager.MemoryIOWrapper;
import eu.stratosphere.nephele.services.iomanager.RawComparator;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.iomanager.Serializer;
import eu.stratosphere.nephele.services.iomanager.Writer;
import eu.stratosphere.nephele.services.memorymanager.MemoryBacked;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.UnboundMemoryBackedException;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * This class is legacy code and here only because some older code still references it.
 * 
 * @author Erik Nijkamp
 * @author Stephan Ewen
 * 
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public final class BufferSortable<K extends Key, V extends Value> extends MemoryBacked implements IndexedSortable
{
	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(BufferSortable.class);

	/**
	 * The percentage of the byte array size that may be allocated for tracking the
	 * record boundaries (accounting space).
	 */
	private final float kvindicesperc;

	/**
	 * Position in the underlying memory.
	 */
	protected int position;

	// ------------------------------------------------------------------------
	//                     Serialization / Deserialization
	// ------------------------------------------------------------------------

	private final MemoryIOWrapper memoryWrapper;

	private final RawComparator comparator;

	private final SerializationFactory<K> keySerialization;

	private final SerializationFactory<V> valSerialization;

	private final Serializer<K> keySerializer;

	private final Serializer<V> valSerializer;

	private final Deserializer<K> keyDeserializer;

	private final Deserializer<V> valDeserializer;

	// ------------------------------------------------------------------------
	//                      Key/Value accounting
	// ------------------------------------------------------------------------

	private static final int KEYSTART = 0; // key offset in acct

	private static final int VALSTART = 1; // value offset in acct

	private static final int ACCTSIZE = 2; // total #fields in acct

	private static final int RECSIZE = (ACCTSIZE + 1) * 4; // acct bytes per record 
	
	private int[] kvoffsets; // indices into kvindices

	private int[] kvindices; // offsets into the byte[] segment

	private int kvindex; // current index within kvoffsets

	private int kvlast; // last key position in kvindices

	// -------------------------------------------------------------------------
	//                     Constructors / Destructors
	// -------------------------------------------------------------------------

	public BufferSortable(MemorySegment memory, RawComparator comparator, SerializationFactory<K> keySerialization,
			SerializationFactory<V> valSerialization, float kvindicesperc) {
		super();

		// buffers and accounting
		this.kvindicesperc = kvindicesperc;

		// serialization
		this.comparator = comparator;
		this.keySerialization = keySerialization;
		this.valSerialization = valSerialization;
		this.keySerializer = keySerialization.getSerializer();
		this.valSerializer = valSerialization.getSerializer();
		this.keyDeserializer = keySerialization.getDeserializer();
		this.valDeserializer = valSerialization.getDeserializer();

		// bind memory segment
		bind(memory);
		this.memoryWrapper = new MemoryIOWrapper(memory);
	}

	// -------------------------------------------------------------------------
	//                         Memory Segment
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.nephele.services.memorymanager.MemoryBacked#bind(eu.stratosphere.nephele.services.memorymanager.
	 * MemorySegment)
	 */
	@Override
	public boolean bind(MemorySegment memory) {
		if (super.bind(memory)) {
			// accounting
			int segmentSize = memory.size();

			int recordCapacity = (int) (segmentSize * kvindicesperc);
			recordCapacity -= recordCapacity % RECSIZE;
			recordCapacity /= RECSIZE;

			this.kvoffsets = new int[recordCapacity];
			this.kvindices = new int[recordCapacity * ACCTSIZE];

			// reset counters
			reset();
			return true;
		} else {
			return false;
		}
	}

	public void reset() {
		try {
			// memory segment
			this.memory.outputView.reset();

			// buffer
			this.position = 0;

			// serialization
			this.keySerializer.open(this.memory.outputView);
			this.valSerializer.open(this.memory.outputView);

			// accounting
			this.kvindex = 0;
			this.kvlast = 0;
		} catch (IOException iex) {
			throw new RuntimeException(iex);
		}
	}

	// -------------------------------------------------------------------------
	//                                 Buffering
	// -------------------------------------------------------------------------

	protected int getRemainingBytes() {
		return this.memory.size() - this.memory.outputView.getPosition();
	}

	protected boolean isEmpty() {
		return this.memory.outputView.getPosition() == 0;
	}

	public int getPosition() {
		return this.position;
	}

	// -------------------------------------------------------------------------
	//                             Retrieving and Writing
	// -------------------------------------------------------------------------

	/**
	 * Gets the key at the specified position.
	 * 
	 * @param position
	 *        The position of the value.
	 * @return The key.
	 * @throws IOException
	 *         Thrown, if the deserialization causes an exception.
	 * @throws ArrayIndexOutOfBoundsException
	 *         If the position is negative or if it is larger or equal to
	 *         the number of key/value pairs in the buffer.
	 */
	public K getKey(int position) throws IOException {
		this.keyDeserializer.open(this.memory.inputView);

		int index = this.kvoffsets[position];

		int keyStart = this.kvindices[index + KEYSTART];

		K key = this.keySerialization.newInstance();

		this.memory.inputView.setPosition(keyStart);
		this.keyDeserializer.deserialize(key);

		return key;
	}

	/**
	 * Gets the value at the specified position.
	 * 
	 * @param position
	 *        The position of the value.
	 * @return The value.
	 * @throws IOException
	 *         Thrown, if the deserialization causes an exception.
	 * @throws ArrayIndexOutOfBoundsException
	 *         If the position is negative or if it is larger or equal to
	 *         the number of key/value pairs in the buffer.
	 */
	public V getValue(int position) throws IOException {
		this.valDeserializer.open(this.memory.inputView);

		int index = this.kvoffsets[position];
		int valStart = this.kvindices[index + VALSTART];

		V val = this.valSerialization.newInstance();

		this.memory.inputView.setPosition(valStart);
		this.valDeserializer.deserialize(val);

		return val;
	}

	/**
	 * Writes the provided key/value pair into the underlying memory segment.
	 * 
	 * @param pair
	 *        The key/value pair to be written.
	 * @throws IOException
	 * @throws UnboundMemoryBackedException
	 */
	public boolean write(KeyValuePair<K, V> pair) {
		try {
			// increment index
			final int kvnext = (kvindex + 1);

			// check accounting space
			if (kvnext == kvoffsets.length - 1) {
				LOG.debug(getClass().getSimpleName() + " ran out of accounting space.");
				return false;
			}

			// serialize key bytes into buffer
			final int keystart = memory.outputView.getPosition();
			this.keySerializer.serialize(pair.getKey());

			// serialize value bytes into buffer
			final int valstart = memory.outputView.getPosition();
			this.valSerializer.serialize(pair.getValue());

			// update accounting info
			final int index = this.kvindex * ACCTSIZE;
			this.kvoffsets[kvindex] = index;
			this.kvindices[index + KEYSTART] = keystart;
			this.kvindices[index + VALSTART] = valstart;
			this.kvindex = kvnext;
			this.kvlast = keystart;

			// update current write position
			this.position = this.memory.outputView.getPosition();
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	/**
	 * Writes this buffer completely to the given writer.
	 * 
	 * @param writer
	 *        The writer to write the segment to.
	 * @throws IOException
	 *         Thrown, if the writer caused an I/O exception.
	 */
	public void writeToChannel(Writer writer) throws IOException {
		if (!isBound()) {
			new UnboundMemoryBackedException();
		}

		final MemoryIOWrapper memoryWrapper = new MemoryIOWrapper(memory);

		// write according to index
		for (int i = 0; i < kvindex; i++) {
			// index into kvindices
			int index = kvoffsets[i];

			// start and end within memory segment
			int kvstart = kvindices[index + KEYSTART];
			int kvend = kvindices[index + ACCTSIZE];
			// -> kvend = kvstart of next pair

			// for the last written pair kvindices[index + ACCTSIZE] does not exist
			if (kvstart == kvlast) {
				kvend = position;
			}

			// length of serialized pair
			final int kvlength = kvend - kvstart;

			// set offset within memory segment
			memoryWrapper.setIOBlock(kvstart, kvlength);

			// copy serialized pair to writer
			writer.write(memoryWrapper);
		}
	}

	/**
	 * Writes a series of key/value pairs in this buffer to the given writer.
	 * 
	 * @param writer
	 *        The writer to write the pairs to.
	 * @param start
	 *        The position (logical number) of the first pair that is written.
	 * @param num
	 *        The number of pairs to be written.
	 * @throws IOException
	 *         Thrown, if the writer caused an I/O exception.
	 */
	public void writeToChannel(Writer writer, int start, int num) throws IOException {
		// write according to index
		for (int i = start; i < start + num; i++) {
			// index into kvindices
			int index = kvoffsets[i];

			// start and end within memory segment
			int kvstart = kvindices[index + KEYSTART];
			int kvend = kvindices[index + ACCTSIZE];

			// for the last written pair kvindices[index + ACCTSIZE] does not exist
			if (kvstart == kvlast) {
				kvend = position;
			}

			// length of serialized pair
			final int kvlength = kvend - kvstart;

			// set offset within memory segment
			memoryWrapper.setIOBlock(kvstart, kvlength);

			// copy serialized pair to writer
			writer.write(memoryWrapper);
		}
	}

	// -------------------------------------------------------------------------
	// Indexed Sorting
	// -------------------------------------------------------------------------

	@Override
	public int compare(int i, int j) {
		final byte[] backingArray = memory.getBackingArray();
		
		// index
		final int ii = kvoffsets[i];
		final int ij = kvoffsets[j];

		// keys
		final int indexi = memory.translateOffset(kvindices[ii + KEYSTART]);
		final int indexj = memory.translateOffset(kvindices[ij + KEYSTART]);
		
		return comparator.compare(backingArray, backingArray, indexi, indexj);
	}

	@Override
	public void swap(int i, int j) {
		int tmp = kvoffsets[i];
		kvoffsets[i] = kvoffsets[j];
		kvoffsets[j] = tmp;
	}

	@Override
	public int size() {
		return kvindex;
	}

	public Iterator<KeyValuePair<K, V>> getIterator() {

		Iterator<KeyValuePair<K, V>> it = new Iterator<KeyValuePair<K, V>>() {

			int pairIdx = 0;

			@Override
			public boolean hasNext() {
				if (pairIdx < kvindex) {
					return true;
				} else {
					return false;
				}
			}

			@Override
			public KeyValuePair<K, V> next() {
				try {
					keyDeserializer.open(memory.inputView);
					valDeserializer.open(memory.inputView);

					int index = kvoffsets[pairIdx];
					int keyStart = kvindices[index + KEYSTART];
					int valStart = kvindices[index + VALSTART];

					K key = keySerialization.newInstance();
					V val = valSerialization.newInstance();

					memory.inputView.setPosition(keyStart);
					keyDeserializer.deserialize(key);

					memory.inputView.setPosition(valStart);
					valDeserializer.deserialize(val);

					pairIdx++;

					return new KeyValuePair<K, V>(key, val);

				} catch (IOException ioe) {
					throw new RuntimeException(ioe);
				}
			}

			@Override
			public void remove() {

			}
		};

		return it;
	}
}
