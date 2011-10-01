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

package eu.stratosphere.nephele.jobmanager.scheduler.queue;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;

/**
 * This is a wrapper class for the {@link QueueScheduler} to receive
 * notifications about state changes of vertices belonging
 * to scheduled jobs.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class QueueExecutionListener implements ExecutionListener {

	/**
	 * The instance of the {@link QueueScheduler}.
	 */
	private final QueueScheduler queueScheduler;

	/**
	 * The {@link ExecutionVertex} this wrapper object belongs to.
	 */
	private final ExecutionVertex executionVertex;

	/**
	 * Constructs a new wrapper object for the given {@link ExecutionVertex}.
	 * 
	 * @param localScheduler
	 *        the instance of the {@link QueueScheduler}
	 * @param executionVertex
	 *        the {@link ExecutionVertex} the received notification refer to
	 */
	public QueueExecutionListener(final QueueScheduler localScheduler, final ExecutionVertex executionVertex) {
		this.queueScheduler = localScheduler;
		this.executionVertex = executionVertex;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void executionStateChanged(final Environment ee, final ExecutionState newExecutionState,
			final String optionalMessage) {

		final ExecutionGraph eg = this.executionVertex.getExecutionGraph();

		if (newExecutionState == ExecutionState.FINISHED || newExecutionState == ExecutionState.CANCELED
			|| newExecutionState == ExecutionState.FAILED) {
			// Check if instance can be released
			this.queueScheduler.checkAndReleaseAllocatedResource(eg, this.executionVertex.getAllocatedResource());
		}

		// In case of an error, check if vertex can be rescheduled
		if (newExecutionState == ExecutionState.FAILED) {
			if (this.executionVertex.hasRetriesLeft()) {
				// Reschedule vertex
				this.executionVertex.setExecutionState(ExecutionState.SCHEDULED);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(final Environment ee, final Thread userThread) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final Environment ee, final Thread userThread) {
		// Nothing to do here
	}
}
