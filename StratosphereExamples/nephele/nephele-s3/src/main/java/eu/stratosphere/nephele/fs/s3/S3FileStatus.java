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

package eu.stratosphere.nephele.fs.s3;

import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.Path;

public final class S3FileStatus implements FileStatus {

	private final Path path;

	private final long length;

	private final boolean isDir;

	private final long modificationTime;
	
	private final long accessTime;
	
	S3FileStatus(final Path path, final long length, final boolean isDir, final long modificationTime,
			final long accessTime) {
		this.path = path;
		this.length = length;
		this.isDir = isDir;
		this.modificationTime = modificationTime;
		this.accessTime = accessTime;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getLen() {

		return this.length;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getBlockSize() {

		return this.length;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public short getReplication() {

		return 1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getModificationTime() {
		
		return this.modificationTime;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getAccessTime() {
		
		return this.accessTime;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isDir() {

		return this.isDir;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Path getPath() {

		return this.path;
	}

}
