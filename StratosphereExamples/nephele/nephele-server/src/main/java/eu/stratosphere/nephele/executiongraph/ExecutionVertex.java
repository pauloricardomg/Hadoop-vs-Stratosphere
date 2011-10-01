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

package eu.stratosphere.nephele.executiongraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.AllocationID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.types.Record;

/**
 * An execution vertex represents an instance of a task in a Nephele job. An execution vertex
 * is initially created from a job vertex and always belongs to exactly one group vertex.
 * It is possible to duplicate execution vertices in order to distribute a task to several different
 * task managers and process the task in parallel.
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class ExecutionVertex {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ExecutionVertex.class);

	/**
	 * The class of the task to be executed by this vertex.
	 */
	private final Class<? extends AbstractInvokable> invokableClass;

	/**
	 * The ID of the vertex.
	 */
	private final ExecutionVertexID vertexID;

	/**
	 * The environment created to execute the vertex's task.
	 */
	private Environment environment = null;

	/**
	 * The group vertex this vertex belongs to.
	 */
	private final ExecutionGroupVertex groupVertex;

	/**
	 * The execution graph is vertex belongs to.
	 */
	private final ExecutionGraph executionGraph;

	/**
	 * The allocated resources assigned to this vertex.
	 */
	private AllocatedResource allocatedResource = null;

	/**
	 * The allocation ID identifying the allocated resources used by this vertex
	 * within the instance.
	 */
	private AllocationID allocationID = null;

	/**
	 * A list of {@link VertexAssignmentListener} objects to be notified about changes in the instance assignment.
	 */
	private List<VertexAssignmentListener> vertexAssignmentListeners = new ArrayList<VertexAssignmentListener>();

	/**
	 * Create a new execution vertex and instantiates its environment.
	 * 
	 * @param jobID
	 *        the ID of the job this execution vertex is created from
	 * @param invokableClass
	 *        the task that is assigned to this execution vertex
	 * @param executionGraph
	 *        the execution graph the new vertex belongs to
	 * @param groupVertex
	 *        the group vertex the new vertex belongs to
	 * @throws Exception
	 *         any exception that might be thrown by the user code during instantiation and registration of input and
	 *         output channels
	 */
	public ExecutionVertex(JobID jobID, Class<? extends AbstractInvokable> invokableClass,
			ExecutionGraph executionGraph, ExecutionGroupVertex groupVertex) throws Exception {
		this(new ExecutionVertexID(), invokableClass, executionGraph, groupVertex);

		this.groupVertex.addInitialSubtask(this);

		if (invokableClass == null) {
			LOG.error("Vertex " + groupVertex.getName() + " does not specify a task");
		}

		this.environment = new Environment(jobID, groupVertex.getName(), invokableClass, groupVertex.getConfiguration());

		// Register the vertex itself as a listener for state changes
		this.environment.registerExecutionListener(this.executionGraph);
		this.environment.instantiateInvokable();
	}

	/**
	 * Private constructor used to duplicate execution vertices.
	 * 
	 * @param vertexID
	 *        the ID of the new execution vertex.
	 * @param invokableClass
	 *        the task that is assigned to this execution vertex
	 * @param executionGraph
	 *        the execution graph the new vertex belongs to
	 * @param groupVertex
	 *        the group vertex the new vertex belongs to
	 */
	private ExecutionVertex(ExecutionVertexID vertexID, Class<? extends AbstractInvokable> invokableClass,
			ExecutionGraph executionGraph, ExecutionGroupVertex groupVertex) {
		this.vertexID = vertexID;
		this.invokableClass = invokableClass;
		this.executionGraph = executionGraph;
		this.groupVertex = groupVertex;

		// Duplication of environment is done in method duplicateVertex
	}

	/**
	 * Returns the environment of this execution vertex.
	 * 
	 * @return the environment of this execution vertex
	 */
	public synchronized Environment getEnvironment() {
		return this.environment;
	}

	/**
	 * Returns the group vertex this execution vertex belongs to.
	 * 
	 * @return the group vertex this execution vertex belongs to
	 */
	public ExecutionGroupVertex getGroupVertex() {
		return this.groupVertex;
	}

	/**
	 * Returns the name of the execution vertex.
	 * 
	 * @return the name of the execution vertex
	 */
	public String getName() {
		return this.groupVertex.getName();
	}

	/**
	 * Returns a duplicate of this execution vertex.
	 * 
	 * @param preserveVertexID
	 *        <code>true</code> to copy the vertex's ID to the duplicated vertex, <code>false</code> to create a new ID
	 * @return a duplicate of this execution vertex
	 * @throws Exception
	 *         any exception that might be thrown by the user code during instantiation and registration of input and
	 *         output channels
	 */
	public synchronized ExecutionVertex duplicateVertex(boolean preserveVertexID) throws Exception {

		ExecutionVertexID newVertexID;
		if (preserveVertexID) {
			newVertexID = this.vertexID;
		} else {
			newVertexID = new ExecutionVertexID();
		}

		final ExecutionVertex duplicatedVertex = new ExecutionVertex(newVertexID, this.invokableClass,
			this.executionGraph, this.groupVertex);

		synchronized (duplicatedVertex) {

			duplicatedVertex.environment = this.environment.duplicateEnvironment();

			// TODO set new profiling record with new vertex id
			duplicatedVertex.allocatedResource = this.allocatedResource;
		}

		return duplicatedVertex;
	}

	/**
	 * Returns a duplicate of this execution vertex. The duplicated vertex receives
	 * a new vertex ID.
	 * 
	 * @return a duplicate of this execution vertex.
	 * @throws Exception
	 *         any exception that might be thrown by the user code during instantiation and registration of input and
	 *         output channels
	 */
	public ExecutionVertex splitVertex() throws Exception {

		return duplicateVertex(false);
	}

	/**
	 * Returns this execution vertex's current execution status.
	 * 
	 * @return this execution vertex's current execution status
	 */
	public synchronized ExecutionState getExecutionState() {
		return this.environment.getExecutionState();
	}

	/**
	 * Sets this execution vertex's current execution state.
	 * 
	 * @param executionState
	 *        the new execution state
	 */
	public synchronized void setExecutionState(ExecutionState executionState) {
		this.environment.changeExecutionState(executionState, null);
	}

	/**
	 * Assigns the execution vertex with an {@link AllocatedResource}.
	 * 
	 * @param allocatedResource
	 *        the resources which are supposed to be allocated to this vertex
	 */
	public synchronized void setAllocatedResource(AllocatedResource allocatedResource) {
		this.allocatedResource = allocatedResource;

		// Notify all listener objects
		final Iterator<VertexAssignmentListener> it = this.vertexAssignmentListeners.iterator();
		while (it.hasNext()) {
			it.next().vertexAssignmentChanged(this.vertexID, this.allocatedResource);
		}
	}

	/**
	 * Returns the allocated resources assigned to this execution vertex.
	 * 
	 * @return the allocated resources assigned to this execution vertex
	 */
	public synchronized AllocatedResource getAllocatedResource() {
		return this.allocatedResource;
	}

	/**
	 * Returns the allocation ID which identifies the resources used
	 * by this vertex within the assigned instance.
	 * 
	 * @return the allocation ID which identifies the resources used
	 *         by this vertex within the assigned instance or <code>null</code> if the instance is still assigned to a
	 *         {@link eu.stratosphere.nephele.instance.DummyInstance}.
	 */
	public synchronized AllocationID getAllocationID() {
		return this.allocationID;
	}

	/**
	 * Returns the ID of this execution vertex.
	 * 
	 * @return the ID of this execution vertex
	 */
	public ExecutionVertexID getID() {
		return this.vertexID;
	}

	/**
	 * Returns the number of predecessors, i.e. the number of vertices
	 * which connect to this vertex.
	 * 
	 * @return the number of predecessors
	 */
	public synchronized int getNumberOfPredecessors() {

		int numberOfPredecessors = 0;

		for (int i = 0; i < this.environment.getNumberOfInputGates(); i++) {
			numberOfPredecessors += this.environment.getInputGate(i).getNumberOfInputChannels();
		}

		return numberOfPredecessors;
	}

	/**
	 * Returns the number of successors, i.e. the number of vertices
	 * this vertex is connected to.
	 * 
	 * @return the number of successors
	 */
	public synchronized int getNumberOfSuccessors() {

		int numberOfSuccessors = 0;

		for (int i = 0; i < this.environment.getNumberOfOutputGates(); i++) {
			numberOfSuccessors += this.environment.getOutputGate(i).getNumberOfOutputChannels();
		}

		return numberOfSuccessors;
	}

	public synchronized ExecutionVertex getPredecessor(int index) {

		for (int i = 0; i < this.environment.getNumberOfInputGates(); i++) {

			final InputGate<? extends Record> inputGate = this.environment.getInputGate(i);

			if (index >= 0 && index < inputGate.getNumberOfInputChannels()) {

				final AbstractInputChannel<? extends Record> inputChannel = inputGate.getInputChannel(index);
				return this.executionGraph.getVertexByChannelID(inputChannel.getConnectedChannelID());
			}
			index -= inputGate.getNumberOfInputChannels();
		}

		return null;
	}

	public synchronized ExecutionVertex getSuccessor(int index) {

		for (int i = 0; i < this.environment.getNumberOfOutputGates(); i++) {

			final OutputGate<? extends Record> outputGate = this.environment.getOutputGate(i);

			if (index >= 0 && index < outputGate.getNumberOfOutputChannels()) {

				final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(index);
				return this.executionGraph.getVertexByChannelID(outputChannel.getConnectedChannelID());
			}
			index -= outputGate.getNumberOfOutputChannels();
		}

		return null;

	}

	/**
	 * Checks if this vertex is an input vertex in its stage, i.e. has either no
	 * incoming connections or only incoming connections to group vertices in a lower stage.
	 * 
	 * @return <code>true</code> if this vertex is an input vertex, <code>false</code> otherwise
	 */
	public boolean isInputVertex() {
		// No need to synchronized this method
		return this.groupVertex.isInputVertex();
	}

	/**
	 * Checks if this vertex is an output vertex in its stage, i.e. has either no
	 * outgoing connections or only outgoing connections to group vertices in a higher stage.
	 * 
	 * @return <code>true</code> if this vertex is an output vertex, <code>false</code> otherwise
	 */
	public boolean isOutputVertex() {
		// No need to synchronized this method
		return this.groupVertex.isOutputVertex();
	}

	/**
	 * Deploys and starts the task represented by this vertex
	 * on the assigned instance.
	 * 
	 * @return the result of the task submission attempt
	 */
	public TaskSubmissionResult startTask() {

		AllocatedResource allocatedRes = null;
		Environment env = null;
		synchronized (this) {
			if (this.allocatedResource == null) {
				final TaskSubmissionResult result = new TaskSubmissionResult(getID(),
					AbstractTaskResult.ReturnCode.ERROR);
				result.setDescription("Assigned instance of vertex " + this.toString() + " is null!");
				return result;
			}

			allocatedRes = this.allocatedResource;
			env = this.environment;
		}

		try {
			return allocatedRes.getInstance().submitTask(this.vertexID, this.executionGraph.getJobConfiguration(), env);
		} catch (IOException e) {
			final TaskSubmissionResult result = new TaskSubmissionResult(getID(), AbstractTaskResult.ReturnCode.ERROR);
			result.setDescription(StringUtils.stringifyException(e));
			return result;
		}
	}

	/**
	 * Cancels and removes the task represented by this vertex
	 * from the instance it is currently running on. If the task
	 * is not currently running, its execution state is simply
	 * updated to <code>CANCELLED</code>.
	 * 
	 * @return the result of the task cancel attempt
	 */
	public TaskCancelResult cancelTask() {

		AllocatedResource allocatedRes = null;

		synchronized (this) {

			if (this.groupVertex.getStageNumber() != this.executionGraph.getIndexOfCurrentExecutionStage()) {
				// Set to canceled directly
				setExecutionState(ExecutionState.CANCELED);
				return new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.SUCCESS);
			}

			final ExecutionState es = this.environment.getExecutionState();
			if (es == ExecutionState.FINISHED || es == ExecutionState.FAILED) {
				// Ignore this call
				return new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.SUCCESS);
			}

			if (es != ExecutionState.RUNNING && es != ExecutionState.FINISHING) {
				// Set to canceled directly
				setExecutionState(ExecutionState.CANCELED);
				return new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.SUCCESS);
			}

			if (this.allocatedResource == null) {
				final TaskCancelResult result = new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.ERROR);
				result.setDescription("Assigned instance of vertex " + this.toString() + " is null!");
				return result;
			}

			allocatedRes = this.allocatedResource;
		}

		try {
			return allocatedRes.getInstance().cancelTask(this.vertexID);
		} catch (IOException e) {
			final TaskCancelResult result = new TaskCancelResult(getID(), AbstractTaskResult.ReturnCode.ERROR);
			result.setDescription(StringUtils.stringifyException(e));
			return result;
		}
	}

	/**
	 * Returns the {@link ExecutionGraph} this vertex belongs to.
	 * 
	 * @return the {@link ExecutionGraph} this vertex belongs to
	 */
	public ExecutionGraph getExecutionGraph() {
		// No need to synchronize this method
		return this.executionGraph;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		return getName() + " (" + (this.environment.getIndexInSubtaskGroup() + 1) + "/"
			+ (this.environment.getCurrentNumberOfSubtasks()) + ")";
	}

	/**
	 * Returns the task represented by this vertex has
	 * a retry attempt left in case of an execution
	 * failure.
	 * 
	 * @return <code>true</code> if the task has a retry attempt left, <code>false</code> otherwise
	 */
	public boolean hasRetriesLeft() {
		// TODO: Implement me
		return false;
	}

	/**
	 * Registers the {@link VertexAssignmentListener} object for this vertex. This object
	 * will be notified about reassignments of this vertex to another instance.
	 * 
	 * @param vertexAssignmentListener
	 *        the object to be notified about reassignments of this vertex to another instance
	 */
	public synchronized void registerVertexAssignmentListener(VertexAssignmentListener vertexAssignmentListener) {

		if (!this.vertexAssignmentListeners.contains(vertexAssignmentListener)) {
			this.vertexAssignmentListeners.add(vertexAssignmentListener);
		}
	}

	/**
	 * Unregisters the {@link VertexAssignmentListener} object for this vertex. This object
	 * will no longer be notified about reassignments of this vertex to another instance.
	 * 
	 * @param vertexAssignmentListener
	 *        the listener to be unregistered
	 */
	public void unregisterVertexAssignmentListener(VertexAssignmentListener vertexAssignmentListener) {

		this.vertexAssignmentListeners.remove(vertexAssignmentListener);
	}
}
