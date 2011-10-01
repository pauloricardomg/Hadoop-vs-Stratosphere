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

package eu.stratosphere.nephele.taskmanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.EnumUtils;

/**
 * This class can be used to updates about a task's execution state from the
 * task manager to the job manager.
 * 
 * @author warneke
 */
public class TaskExecutionState implements IOReadableWritable {

	private JobID jobID = null;

	private ExecutionVertexID executionVertexID = null;

	private ExecutionState executionState = null;

	private String description = null;

	/**
	 * Creates a new task execution result.
	 * 
	 * @param id
	 *        the ID of the task whose result is to be reported
	 * @param executionState
	 *        the execution state with which the task finished
	 * @param description
	 *        an optional description
	 */
	public TaskExecutionState(JobID jobID, ExecutionVertexID id, ExecutionState executionState, String description) {
		this.jobID = jobID;
		this.executionVertexID = id;
		this.executionState = executionState;
		this.description = description;
	}

	/**
	 * Creates an empty task execution result.
	 */
	public TaskExecutionState() {
	}

	/**
	 * Returns the description of this task execution result.
	 * 
	 * @return the description of this task execution result or <code>null</code> if there is no description available
	 */
	public String getDescription() {
		return this.description;
	}

	/**
	 * Returns the ID of the task this result belongs to
	 * 
	 * @return the ID of the task this result belongs to
	 */
	public ExecutionVertexID getID() {
		return this.executionVertexID;
	}

	/**
	 * Returns the execution state with which the task finished.
	 * 
	 * @return the execution state with which the task finished
	 */
	public ExecutionState getExecutionState() {
		return this.executionState;
	}

	public JobID getJobID() {
		return this.jobID;
	}

	@Override
	public void read(DataInput in) throws IOException {

		boolean isNotNull = in.readBoolean();

		if (isNotNull) {
			this.jobID = new JobID();
			this.jobID.read(in);
		} else {
			this.jobID = null;
		}

		isNotNull = in.readBoolean();

		// Read the execution vertex ID
		if (isNotNull) {
			this.executionVertexID = new ExecutionVertexID();
			this.executionVertexID.read(in);
		} else {
			this.executionVertexID = null;
		}

		// Read execution state
		this.executionState = EnumUtils.readEnum(in, ExecutionState.class);

		// Read description
		this.description = StringRecord.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		if (this.jobID == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.jobID.write(out);
		}

		// Write the execution vertex ID
		if (this.executionVertexID == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.executionVertexID.write(out);
		}

		// Write execution state
		EnumUtils.writeEnum(out, this.executionState);

		// Write description
		StringRecord.writeString(out, this.description);

	}

}
