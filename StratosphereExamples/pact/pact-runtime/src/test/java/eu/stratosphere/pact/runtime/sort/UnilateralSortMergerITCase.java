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

import java.util.Comparator;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.CreationMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;

/**
 * @author Erik Nijkamp
 */
public class UnilateralSortMergerITCase {
	private static final Log LOG = LogFactory.getLog(UnilateralSortMergerITCase.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 118;

	private static final int NUM_PAIRS = 50000;

	public static final int MEMORY_SIZE = 1024 * 1024 * 256;
	
	private final AbstractTask parentTask = new DummyInvokable();

	private IOManager ioManager;

	private MemoryManager memoryManager;

	@BeforeClass
	public static void beforeClass() {
		
	}

	@AfterClass
	public static void afterClass() {
	}

	@Before
	public void beforeTest() {
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
		ioManager = new IOManager();
	}

	@After
	public void afterTest() {
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			Assert.fail("I/O Manager was not properly shut down.");
		}
		
		if (memoryManager != null) {
			Assert.assertTrue("Memory leak: not all segments have been returned to the memory manager.", 
				memoryManager.verifyEmpty());
			memoryManager.shutdown();
			memoryManager = null;
		}
	}

	@Test
	public void testSort() throws Exception {
		// serialization
		final SerializationFactory<TestData.Key> keySerialization = new WritableSerializationFactory<TestData.Key>(
			TestData.Key.class);
		final SerializationFactory<TestData.Value> valSerialization = new WritableSerializationFactory<TestData.Value>(
			TestData.Value.class);

		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		// reader
		MockRecordReader<KeyValuePair<TestData.Key, TestData.Value>> reader = new MockRecordReader<KeyValuePair<TestData.Key, TestData.Value>>();

		// merge iterator
		LOG.debug("Initializing sortmerger...");
		SortMerger<TestData.Key, TestData.Value> merger = new UnilateralSortMerger<TestData.Key, TestData.Value>(
			memoryManager, ioManager, 16 * 1024 * 1024, 1024 * 1024 * 4, 1, 2, keySerialization, valSerialization,
			keyComparator, reader, parentTask, 0.7f);

		// emit data
		LOG.debug("Emitting data...");
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.FIX_LENGTH);
		for (int i = 0; i < NUM_PAIRS; i++) {
			reader.emit(generator.next());
		}
		reader.close();

		// check order
		Iterator<KeyValuePair<TestData.Key, TestData.Value>> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsEmitted = 0;
		KeyValuePair<TestData.Key, TestData.Value> pair1 = null;
		while (iterator.hasNext()) {
			pairsEmitted++;
			KeyValuePair<TestData.Key, TestData.Value> pair2 = iterator.next();
			if (pair1 != null && pair2 != null) {
				Assert.assertTrue(keyComparator.compare(pair1.getKey(), pair2.getKey()) <= 0);
			}
			pair1 = pair2;
		}
		Assert.assertTrue(NUM_PAIRS == pairsEmitted);
		
		merger.close();
	}

	@Test
	public void testSortInMemory() throws Exception {
		// serialization
		final SerializationFactory<TestData.Key> keySerialization = new WritableSerializationFactory<TestData.Key>(
			TestData.Key.class);
		final SerializationFactory<TestData.Value> valSerialization = new WritableSerializationFactory<TestData.Value>(
			TestData.Value.class);

		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		// reader
		MockRecordReader<KeyValuePair<TestData.Key, TestData.Value>> reader = new MockRecordReader<KeyValuePair<TestData.Key, TestData.Value>>();
		
		// merge iterator
		LOG.debug("initializing sortmerger");
		SortMerger<TestData.Key, TestData.Value> merger = new UnilateralSortMerger<TestData.Key, TestData.Value>(
			memoryManager, ioManager, 40 * 1024 * 1024, 1024 * 1024 * 1, 10, 2, keySerialization, valSerialization,
			keyComparator, reader, parentTask, 0.7f);

		// emit data
		LOG.debug("emitting data");
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.FIX_LENGTH);
		for (int i = 0; i < NUM_PAIRS; i++) {
			reader.emit(generator.next());
		}
		reader.close();

		// check order
		Iterator<KeyValuePair<TestData.Key, TestData.Value>> iterator = merger.getIterator();
		
		LOG.debug("checking results");
		int pairsEmitted = 0;
		KeyValuePair<TestData.Key, TestData.Value> pair1 = null;
		while (iterator.hasNext()) {
			pairsEmitted++;
			KeyValuePair<TestData.Key, TestData.Value> pair2 = iterator.next();
			if (pair1 != null && pair2 != null) {
				if (keyComparator.compare(pair1.getKey(), pair2.getKey()) > 0) {
					Assert.fail();
				}
			}
			pair1 = pair2;
		}
		merger.close();
		
		Assert.assertTrue(NUM_PAIRS == pairsEmitted);
	}
	
	@Test
	public void testSortTenBuffers() throws Exception {
		// serialization
		final SerializationFactory<TestData.Key> keySerialization = new WritableSerializationFactory<TestData.Key>(
			TestData.Key.class);
		final SerializationFactory<TestData.Value> valSerialization = new WritableSerializationFactory<TestData.Value>(
			TestData.Value.class);

		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		// reader
		MockRecordReader<KeyValuePair<TestData.Key, TestData.Value>> reader = new MockRecordReader<KeyValuePair<TestData.Key, TestData.Value>>();

		// merge iterator
		LOG.debug("initializing sortmerger");
		SortMerger<TestData.Key, TestData.Value> merger = new UnilateralSortMerger<TestData.Key, TestData.Value>(
			memoryManager, ioManager, 1024 * 1024 * 42, 1024 * 1024 * 2, 10, 2, keySerialization, valSerialization,
			keyComparator, reader, parentTask, 0.7f);

		// emit data
		LOG.debug("emitting data");
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.FIX_LENGTH);
		for (int i = 0; i < NUM_PAIRS * 10; i++) {
			reader.emit(generator.next());
		}
		reader.close();

		// check order
		Iterator<KeyValuePair<TestData.Key, TestData.Value>> iterator = merger.getIterator();
		
		LOG.debug("checking results");
		int pairsEmitted = 0;
		KeyValuePair<TestData.Key, TestData.Value> pair1 = null;
		while (iterator.hasNext()) {
			pairsEmitted++;
			KeyValuePair<TestData.Key, TestData.Value> pair2 = iterator.next();
			if (pair1 != null && pair2 != null) {
				Assert.assertTrue(keyComparator.compare(pair1.getKey(), pair2.getKey()) <= 0);
			}
			pair1 = pair2;
		}
		Assert.assertTrue(NUM_PAIRS * 10 == pairsEmitted);
		merger.close();
	}

	@Test
	public void testSortHugeAmountOfPairs() throws Exception {
		// amount of pairs
		final int PAIRS = 10000000;

		// serialization
		final SerializationFactory<TestData.Key> keySerialization = new WritableSerializationFactory<TestData.Key>(
			TestData.Key.class);
		final SerializationFactory<TestData.Value> valSerialization = new WritableSerializationFactory<TestData.Value>(
			TestData.Value.class);

		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		// reader
		MockRecordReader<KeyValuePair<TestData.Key, TestData.Value>> reader = new MockRecordReader<KeyValuePair<TestData.Key, TestData.Value>>();

		// merge iterator
		LOG.debug("initializing sortmerger");
		SortMerger<TestData.Key, TestData.Value> merger = new UnilateralSortMerger<TestData.Key, TestData.Value>(
			memoryManager, ioManager, 1024 * 1024 * 64, 16, keySerialization, valSerialization,
			keyComparator, reader, parentTask, 0.7f);

		// emit data
		long start = System.currentTimeMillis();
		LOG.debug("emitting data");
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.RANDOM_LENGTH, CreationMode.MUTABLE);
		long bytesWritten = 0;
		for (int i = 1; i <= PAIRS; i++) {
			if (i % (PAIRS / 20) == 0 || i == PAIRS) {
				long mb = bytesWritten / 1024 / 1024;
				LOG.debug("emitted " + (int) (100.0 * i / PAIRS) + "% (" + i + " pairs, " + mb + " mb)");
			}
			KeyValuePair<TestData.Key, TestData.Value> pair = generator.next();
			bytesWritten += generator.sizeOf(pair);
			reader.emit(pair);
		}
		reader.close();

		// check order
		Iterator<KeyValuePair<TestData.Key, TestData.Value>> iterator = merger.getIterator();
		
		LOG.debug("checking results");
		int pairsEmitted = 0;
		KeyValuePair<TestData.Key, TestData.Value> pair1 = null;
		while (iterator.hasNext()) {
			// check
			pairsEmitted++;
			KeyValuePair<TestData.Key, TestData.Value> pair2 = iterator.next();
			if (pair1 != null && pair2 != null) {
				Assert.assertTrue(keyComparator.compare(pair1.getKey(), pair2.getKey()) <= 0);
			}
			pair2 = pair1;

			// log
			if (pairsEmitted % (PAIRS / 20) == 0 || pairsEmitted == PAIRS - 1) {
				LOG.debug("checked " + (int) (100.0 * pairsEmitted / PAIRS) + "% (" + pairsEmitted + " pairs)");
			}
		}
		Assert.assertTrue(PAIRS == pairsEmitted);
		merger.close();

		// throughput
		long end = System.currentTimeMillis();
		long diff = end - start;
		long secs = diff / 1000;
		long mb = bytesWritten / 1024 / 1024;
		LOG.debug("sorting a workload of " + PAIRS + " pairs (" + mb + "mb)  took " + secs + " seconds -> " + (1.0 * mb)
			/ secs + "mb/s");
	}
}
