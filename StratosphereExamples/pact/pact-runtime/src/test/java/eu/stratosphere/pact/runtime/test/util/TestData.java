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

package eu.stratosphere.pact.runtime.test.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

import eu.stratosphere.pact.common.type.KeyValuePair;

/**
 * Test data utilities classes.
 * 
 * @author Alexander Alexandrov
 * @author Erik Nijkamp
 */
public final class TestData {
	/**
	 * Private constructor (container class should not be instantiated)
	 */
	private TestData() {
	}

	/**
	 * Key comparator.
	 */
	public static class KeyComparator implements Comparator<Key> {
		@Override
		public int compare(Key k1, Key k2) {
			return k1.compareTo(k2);
		}
	};

	/**
	 * Key implementation.
	 */
	public static class Key implements eu.stratosphere.pact.common.type.Key {
		private int key;

		public Key() {
		}

		public Key(int k) {
			key = k;
		}

		public int getKey() {
			return key;
		}

		@Override
		public void read(DataInput in) throws IOException {
			key = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(key);
		}

		@Override
		public int compareTo(eu.stratosphere.pact.common.type.Key o) {
			Key other = (Key) o;
			return this.key - other.key;
		}

		@Override
		public boolean equals(Object obj) {
			return this.key == ((Key) obj).key;
		}

		@Override
		public int hashCode() {
			return key;
		}

		@Override
		public String toString() {
			return String.valueOf(key);
		}
	}

	/**
	 * Value implementation.
	 */
	public static class Value implements eu.stratosphere.pact.common.type.Value {
		private String value;

		public Value() {
		}

		public Value(String v) {
			value = v;
		}

		public String getValue() {
			return value;
		}

		@Override
		public void read(DataInput in) throws IOException {
			value = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(value);
		}

		@Override
		public boolean equals(Object obj) {
			return this.value.equals(((Value) obj).value);
		}
		
		@Override
		public int hashCode() {
			return this.value.hashCode();
		}

		@Override
		public String toString() {
			return value;
		}
	}

	/**
	 * Pair generator.
	 */
	public static class Generator {
		public enum KeyMode {
			SORTED, RANDOM
		};

		public enum ValueMode {
			FIX_LENGTH, RANDOM_LENGTH
		};

		public enum CreationMode {
			MUTABLE, IMMUTABLE
		}

		private static char[] alpha = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'a', 'b', 'c',
			'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm' };

		private final long seed;

		private final int keyMax;

		private final int valueLength;

		private final KeyMode keyMode;

		private final ValueMode valueMode;

		private final CreationMode creationMode;

		private Random random;

		private int counter;

		private KeyValuePair<Key, Value> pair;

		public Generator(long seed, int keyMax, int valueLength) {
			this(seed, keyMax, valueLength, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
		}

		public Generator(long seed, int keyMax, int valueLength, KeyMode keyMode, ValueMode valueMode) {
			this(seed, keyMax, valueLength, keyMode, valueMode, CreationMode.IMMUTABLE);
		}

		public Generator(long seed, int keyMax, int valueLength, KeyMode keyMode, ValueMode valueMode,
				CreationMode creationMode) {
			this.seed = seed;
			this.keyMax = keyMax;
			this.valueLength = valueLength;
			this.keyMode = keyMode;
			this.valueMode = valueMode;
			this.creationMode = creationMode;

			this.random = new Random(seed);
			this.counter = 0;
		}

		public KeyValuePair<Key, Value> next() {
			if (creationMode == CreationMode.IMMUTABLE) {
				Key key = new Key(keyMode == KeyMode.SORTED ? ++counter : Math.abs(random.nextInt() % keyMax) + 1);
				Value value = new Value(randomString());

				return new KeyValuePair<Key, Value>(key, value);
			} else {
				if (pair == null) {
					Key key = new Key(keyMode == KeyMode.SORTED ? ++counter : Math.abs(random.nextInt() % keyMax) + 1);
					Value value = new Value(randomString());

					pair = new KeyValuePair<Key, Value>(key, value);
				} else {
					pair.getKey().key = keyMode == KeyMode.SORTED ? ++counter : Math.abs(random.nextInt() % keyMax) + 1;
					pair.getValue().value = randomString();
				}
				return pair;
			}
		}

		public int sizeOf(KeyValuePair<Key, Value> pair) {
			// key
			int valueLength = Integer.SIZE / 8;

			// value
			String text = pair.getValue().value;
			int strlen = text.length();
			int utflen = 0;
			int c;
			for (int i = 0; i < strlen; i++) {
				c = text.charAt(i);
				if ((c >= 0x0001) && (c <= 0x007F)) {
					utflen++;
				} else if (c > 0x07FF) {
					utflen += 3;
				} else {
					utflen += 2;
				}
			}
			valueLength += 2 + utflen;

			return valueLength;
		}

		public void reset() {
			this.random = new Random(seed);
			this.counter = 0;
		}

		private String randomString() {
			int length;

			if (valueMode == ValueMode.FIX_LENGTH) {
				length = valueLength;
			} else {
				length = valueLength - random.nextInt(valueLength / 3);
			}

			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < length; i++) {
				sb.append(alpha[random.nextInt(alpha.length)]);
			}
			return sb.toString();
		}

	}

	/**
	 * Record reader mock.
	 */
	public static class RecordReaderMock implements eu.stratosphere.nephele.io.Reader<KeyValuePair<Key, Value>> {
		private final Generator generator;

		private final int numberOfRecords;

		private int counter;

		public RecordReaderMock(Generator generator, int numberOfRecords) {
			this.generator = generator;
			this.generator.reset();
			this.numberOfRecords = numberOfRecords;
			this.counter = 0;
		}

		public boolean hasNext() {
			return counter < numberOfRecords;
		}

		public KeyValuePair<Key, Value> next() {
			counter++;
			return generator.next();
		}
	}

	/**
	 * Record reader mock.
	 */
	public static class RecordReaderIterMock implements eu.stratosphere.nephele.io.Reader<KeyValuePair<Key, Value>> {
		private final Iterator<KeyValuePair<Key, Value>> iterator;

		public RecordReaderIterMock(Iterator<KeyValuePair<Key, Value>> iterator) {
			this.iterator = iterator;
		}

		public boolean hasNext() {
			return iterator.hasNext();
		}

		public KeyValuePair<Key, Value> next() {
			return iterator.next();
		}
	}
	
	/**
	 * Record reader mock.
	 */
	public static class GeneratorIterator implements Iterator<KeyValuePair<Key, Value>> {
		private final Generator generator;

		private final int numberOfRecords;

		private int counter;

		public GeneratorIterator(Generator generator, int numberOfRecords) {
			this.generator = generator;
			this.generator.reset();
			this.numberOfRecords = numberOfRecords;
			this.counter = 0;
		}

		@Override
		public boolean hasNext() {
			return counter < numberOfRecords;
		}

		@Override
		public KeyValuePair<Key, Value> next() {
			counter++;
			return generator.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		public void reset() {
			this.counter = 0;
		}
	}
	
	public static class ConstantValueIterator implements Iterator<KeyValuePair<Key, Value>>
	{
		private final String valueValue;
		
		private final int keyValue;
		
		private final int numPairs;
		
		private int pos;
		
		
		public ConstantValueIterator(int keyValue, String valueValue, int numPairs) {
			this.keyValue = keyValue;
			this.valueValue = valueValue;
			this.numPairs = numPairs;
		}

		@Override
		public boolean hasNext() {
			return pos < this.numPairs;
		}
		
		@Override
		public KeyValuePair<Key, Value> next() {
			KeyValuePair<Key, Value> pair = new KeyValuePair<Key, Value>(new Key(this.keyValue), new Value(this.valueValue + ' ' + pos));
			pos++;
			return pair;
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		public void reset() {
			this.pos = 0;
		}
	}
}
