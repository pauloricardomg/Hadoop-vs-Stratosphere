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

package eu.stratosphere.nephele.jobmanager;

/**
 * This class takes care of cleaning up when the job manager is closed.
 * 
 * @author warneke
 */
public class JobManagerCleanUp extends Thread {

	/**
	 * The job manager to clean up for.
	 */
	private final JobManager jobManager;

	/**
	 * Constructs a new clean up object when the job manager is closed.
	 * 
	 * @param jobManager
	 *        the job manager to clean up for
	 */
	public JobManagerCleanUp(JobManager jobManager) {
		this.jobManager = jobManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		// Shut down the job manager properly
		this.jobManager.shutdown();
	}

}
