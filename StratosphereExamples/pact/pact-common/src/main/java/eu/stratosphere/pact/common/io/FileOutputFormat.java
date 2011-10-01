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

package eu.stratosphere.pact.common.io;


import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;


/**
 * 
 */
public abstract class FileOutputFormat<K extends Key, V extends Value> extends OutputFormat<K, V>
{
	public static final String FILE_PARAMETER_KEY = "pact.output.file";
	
	/**
	 * The path of the file to be written.
	 */
	protected Path outputFilePath;
	
	/**
	 * The stream to which the data is written;
	 */
	protected FSDataOutputStream stream;

	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters)
	{
		String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
		if (filePath == null) {
			throw new IllegalArgumentException("Configuration file FileOutputFormat does not contain the file path.");
		}
		
		try {
			this.outputFilePath = new Path(filePath);
		}
		catch (RuntimeException rex) {
			throw new RuntimeException("Could not create a valid URI from the given file path name: " + rex.getMessage()); 
		}
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#open()
	 */
	@Override
	public void open(int taskNumber) throws IOException
	{
		// obtain FSDataOutputStream asynchronously, since HDFS client can not handle InterruptedExceptions
		OutputPathOpenThread opot = new OutputPathOpenThread(this.outputFilePath, taskNumber, 10000);
		opot.start();
		
		try {
			// get FSDataOutputStream
			this.stream = opot.getFSDataOutputStream();
		}
		catch (Exception e) {
			throw new RuntimeException("Stream to output file could not be opened: " + e.getMessage(), e);
		}
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#close()
	 */
	@Override
	public void close() throws IOException
	{
		if (this.stream != null) {
			this.stream.close();
		}
	}
	
	// ============================================================================================
	
	/**
	 * Obtains a DataOutputStream in an thread that is not interrupted.
	 * The HDFS client is very sensitive to InterruptedExceptions.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 */
	private static class OutputPathOpenThread extends Thread {

		private final Object lock = new Object();
		
		private final Path path;
		
		private final long timeoutMillies;

		private final int taskIndex;

		private volatile FSDataOutputStream fdos;

		private volatile Exception exception;
		
		private volatile boolean canceled = false;
		

		public OutputPathOpenThread(Path path, int taskIndex, long timeoutMillies) {
			this.path = path;
			this.timeoutMillies = timeoutMillies;
			this.taskIndex = taskIndex;
		}

		@Override
		public void run() {
			
			try {
				final FileSystem fs = path.getFileSystem();
				Path p = this.path;
				
				if (fs.exists(this.path) && fs.getFileStatus(this.path).isDir()) {
					// write output in directory
					p = this.path.suffix("/" + this.taskIndex);
				}
				
				final FSDataOutputStream stream = fs.create(p, true);

				// create output file
				synchronized (this.lock) {
					this.lock.notifyAll();
					
					if (!this.canceled) {
						this.fdos = stream;
					}
					else {
						this.fdos = null;
						stream.close();
					}
				}
			}
			catch (Exception t) {
				synchronized (this.lock) {
					this.canceled = true;
					this.exception = t;
				}
			}
		}

		public FSDataOutputStream getFSDataOutputStream()
		throws Exception
		{
			long start = System.currentTimeMillis();
			long remaining = this.timeoutMillies;
			
			if (this.exception != null) {
				throw this.exception;
			}
			if (this.fdos != null) {
				return this.fdos;
			}
			
			synchronized (this.lock) {
				do {
					try {
						this.lock.wait(remaining);
					}
					catch (InterruptedException iex) {
						this.canceled = true;
						if (this.fdos != null) {
							try  {
								this.fdos.close();
							} catch (Throwable t) {}
						}
						throw new Exception("Output Path Opener was interrupted.");
					}
				}
				while (this.exception == null && this.fdos == null &&
						(remaining = this.timeoutMillies + start - System.currentTimeMillis()) > 0);
			
				if (this.exception != null) {
					if (this.fdos != null) {
						try  {
							this.fdos.close();
						} catch (Throwable t) {}
					}
					throw this.exception;
				}
				
				if (this.fdos != null) {
					return this.fdos;
				}
			}
			
			// try to forcefully shut this thread down
			throw new Exception("Output Path Opener timed out.");
		}
	}
}