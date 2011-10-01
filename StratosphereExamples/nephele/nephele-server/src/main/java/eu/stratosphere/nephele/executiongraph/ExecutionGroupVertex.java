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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.ExecutionSignature;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * An ExecutionGroupVertex is created for every JobVertex of the initial job graph. It represents a number of execution
 * vertices
 * that originate from the same job vertex.
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class ExecutionGroupVertex {

	/**
	 * The name of the vertex.
	 */
	private final String name;

	/**
	 * The ID of the job vertex which is represented by this group vertex.
	 */
	private final JobVertexID jobVertexID;

	/**
	 * The list of execution vertices which are managed by this group vertex.
	 */
	private final ArrayList<ExecutionVertex> groupMembers = new ArrayList<ExecutionVertex>();

	/**
	 * Maximum number of execution vertices this group vertex can manage.
	 */
	private int maxMemberSize = 1;

	/**
	 * Minimum number of execution vertices this group vertex can manage.
	 */
	private int minMemberSize = 1;

	/**
	 * The user defined number of execution vertices, -1 if the user has not specified it.
	 */
	private final int userDefinedNumberOfMembers;

	/**
	 * The instance type to be used for execution vertices this group vertex manages.
	 */
	private InstanceType instanceType = null;

	/**
	 * Stores whether the instance type is user defined.
	 */
	private final boolean userDefinedInstanceType;

	/**
	 * Stores the number of subtasks per instance.
	 */
	private int numberOfSubtasksPerInstance = -1;

	/**
	 * Stores whether the number of subtasks per instance is user defined.
	 */
	private final boolean userDefinedNumberOfSubtasksPerInstance;

	/**
	 * The execution group vertex to share instances with.
	 */
	private ExecutionGroupVertex vertexToShareInstancesWith = null;

	/**
	 * Set of execution vertices sharing instances with this vertex.
	 */
	private final List<ExecutionGroupVertex> verticesSharingInstances = new ArrayList<ExecutionGroupVertex>();

	/**
	 * Stores whether the group vertex to share instances with is user defined.
	 */
	private final boolean userDefinedVertexToShareInstancesWith;

	/**
	 * The cryptographic signature of the vertex.
	 */
	private final ExecutionSignature executionSignature;

	/**
	 * List of outgoing edges.
	 */
	private final ArrayList<ExecutionGroupEdge> forwardLinks = new ArrayList<ExecutionGroupEdge>();

	/**
	 * List of incoming edges.
	 */
	private final ArrayList<ExecutionGroupEdge> backwardLinks = new ArrayList<ExecutionGroupEdge>();

	/**
	 * The execution graph this group vertex belongs to.
	 */
	private final ExecutionGraph executionGraph;

	/**
	 * List of input splits assigned to this group vertex.
	 */
	private InputSplit[] inputSplits = null;

	/**
	 * The execution stage this vertex belongs to.
	 */
	private ExecutionStage executionStage = null;

	/**
	 * The configuration object of the original job vertex.
	 */
	private final Configuration configuration;

	/**
	 * Constructs a new group vertex.
	 * 
	 * @param name
	 *        the name of the group vertex
	 * @param jobVertexID
	 *        the ID of the job vertex which is represented by this group vertex
	 * @param executionGraph
	 *        the execution graph is group vertex belongs to
	 * @param userDefinedNumberOfMembers
	 *        the user defined number of subtasks, -1 if the user did not specify the number
	 * @param instanceType
	 *        the instance type to be used for execution vertices this group vertex manages.
	 * @param userDefinedInstanceType
	 *        <code>true</code> if the instance type is user defined, <code>false</code> otherwise
	 * @param numberOfSubtasksPerInstance
	 *        the user defined number of subtasks per instance, -1 if the user did not specify the number
	 * @param userDefinedVertexToShareInstanceWith
	 *        <code>true</code> if the user specified another vertex to share instances with, <code>false</code>
	 *        otherwise
	 * @param configuration
	 *        the vertex's configuration object
	 * @param signature
	 *        the cryptographic signature of the vertex
	 */
	public ExecutionGroupVertex(String name, JobVertexID jobVertexID, ExecutionGraph executionGraph,
			int userDefinedNumberOfMembers, InstanceType instanceType, boolean userDefinedInstanceType,
			int numberOfSubtasksPerInstance, boolean userDefinedVertexToShareInstanceWith, Configuration configuration,
			ExecutionSignature signature) {
		this.name = name;
		this.jobVertexID = jobVertexID;
		this.executionGraph = executionGraph;
		this.userDefinedNumberOfMembers = userDefinedNumberOfMembers;
		this.instanceType = instanceType;
		this.userDefinedInstanceType = userDefinedInstanceType;
		if (numberOfSubtasksPerInstance != -1) {
			this.numberOfSubtasksPerInstance = numberOfSubtasksPerInstance;
			this.userDefinedNumberOfSubtasksPerInstance = true;
		} else {
			this.numberOfSubtasksPerInstance = 1;
			this.userDefinedNumberOfSubtasksPerInstance = false;
		}
		this.userDefinedVertexToShareInstancesWith = userDefinedVertexToShareInstanceWith;
		this.configuration = configuration;
		this.executionSignature = signature;
	}

	/**
	 * Returns the name of the group vertex, usually copied from the initial job vertex.
	 * 
	 * @return the name of the group vertex.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Sets the execution stage this group vertex is associated with.
	 * 
	 * @param executionStage
	 *        The new execution stage.
	 */
	public synchronized void setExecutionStage(ExecutionStage executionStage) {
		this.executionStage = executionStage;
	}

	/**
	 * Returns the execution stage this group vertex is associated with.
	 * 
	 * @return The execution stage this vertex is associated with.
	 */
	public synchronized ExecutionStage getExecutionStage() {

		return this.executionStage;
	}

	/**
	 * Adds the initial execution vertex to this group vertex.
	 * 
	 * @param ev
	 *        The new execution vertex to be added.
	 */
	public void addInitialSubtask(ExecutionVertex ev) {

		synchronized (this.groupMembers) {

			if (groupMembers.size() == 0) {
				groupMembers.add(ev);
			}
		}
	}

	/**
	 * Returns a specific execution vertex from the list of members.
	 * 
	 * @param pos
	 *        The position of the execution vertex to be returned.
	 * @return The execution vertex at position <code>pos</code> of the member list, <code>null</code> if there is no
	 *         such position.
	 */
	public ExecutionVertex getGroupMember(int pos) {

		synchronized (this.groupMembers) {

			if (pos < this.groupMembers.size()) {
				return this.groupMembers.get(pos);
			}
		}

		return null;
	}

	/**
	 * Sets the maximum number of members this group vertex can have.
	 * 
	 * @param maxSize
	 *        the maximum number of members this group vertex can have
	 */
	public synchronized void setMaxMemberSize(int maxSize) {
		maxMemberSize = maxSize;
	}

	/**
	 * Sets the minimum number of members this group vertex must have.
	 * 
	 * @param minSize
	 *        the minimum number of members this group vertex must have
	 */
	public synchronized void setMinMemberSize(int minSize) {
		minMemberSize = minSize;
	}

	/**
	 * Returns the current number of members this group vertex has.
	 * 
	 * @return the current number of members this group vertex has
	 */
	public int getCurrentNumberOfGroupMembers() {

		synchronized (this.groupMembers) {

			return groupMembers.size();
		}
	}

	/**
	 * Returns the maximum number of members this group vertex can have.
	 * 
	 * @return the maximum number of members this group vertex can have
	 */
	public synchronized int getMaximumNumberOfGroupMembers() {
		return maxMemberSize;
	}

	/**
	 * Returns the minimum number of members this group vertex must have.
	 * 
	 * @return the minimum number of members this group vertex must have
	 */
	public synchronized int getMinimumNumberOfGroupMember() {
		return minMemberSize;
	}

	/**
	 * Wires this group vertex to the specified group vertex and creates
	 * a back link.
	 * 
	 * @param groupVertex
	 *        the group vertex that should be the target of the wiring
	 * @param indexOfInputGate
	 *        the index of the consuming task's input gate
	 * @param indexOfOutputGate
	 *        the index of the producing tasks's output gate
	 * @param channelType
	 *        the channel type to be used for this edge
	 * @param userDefinedChannelType
	 *        <code>true</code> if the channel type is user defined, <code>false</code> otherwise
	 * @param compressionLevel
	 *        the compression level to be used for this edge
	 * @param userDefinedCompressionLevel
	 *        <code>true</code> if the compression level is user defined, <code>false</code> otherwise
	 */
	public void wireTo(ExecutionGroupVertex groupVertex, int indexOfInputGate, int indexOfOutputGate,
			ChannelType channelType, boolean userDefinedChannelType, CompressionLevel compressionLevel,
			boolean userDefinedCompressionLevel) throws GraphConversionException {

		synchronized (this.forwardLinks) {

			if (indexOfOutputGate < this.forwardLinks.size()) {
				final ExecutionGroupEdge previousEdge = this.forwardLinks.get(indexOfOutputGate);
				if (previousEdge != null) {
					throw new GraphConversionException("Output gate " + indexOfOutputGate + " of" + getName()
						+ " already has an outgoing edge");
				}
			}
		}

		final ExecutionGroupEdge edge = new ExecutionGroupEdge(this.executionGraph, this, indexOfOutputGate,
			groupVertex, indexOfInputGate, channelType, userDefinedChannelType, compressionLevel,
			userDefinedCompressionLevel);

		synchronized (this.forwardLinks) {
			this.forwardLinks.add(edge);
		}
		groupVertex.wireBackLink(edge);
	}

	/**
	 * Checks if this group vertex is wired to the given group vertex.
	 * 
	 * @param groupVertex
	 *        the group vertex to check for
	 * @return <code>true</code> if there is a wire from the current group vertex to the specified group vertex,
	 *         otherwise <code>false</code>
	 */
	public boolean isWiredTo(ExecutionGroupVertex groupVertex) {

		synchronized (this.forwardLinks) {
			for (int i = 0; i < this.forwardLinks.size(); i++) {
				if (this.forwardLinks.get(i).getTargetVertex() == groupVertex) {
					return true;
				}
			}
		}

		return false;
	}

	/**
	 * Creates a back link from the current group vertex to the specified group vertex.
	 * 
	 * @param groupVertex
	 *        the target of the back link
	 */
	private void wireBackLink(ExecutionGroupEdge edge) {

		synchronized (this.backwardLinks) {
			this.backwardLinks.add(edge);
		}
	}

	/**
	 * Returns the number of forward links the current group vertex has.
	 * 
	 * @return the number of forward links the current group vertex has
	 */
	public int getNumberOfForwardLinks() {

		synchronized (this.forwardLinks) {
			return this.forwardLinks.size();
		}
	}

	/**
	 * Returns the number of backward links the current group vertex has.
	 * 
	 * @return the number of backward links the current group vertex has
	 */
	public int getNumberOfBackwardLinks() {

		synchronized (this.backwardLinks) {
			return this.backwardLinks.size();
		}
	}

	/**
	 * Returns the number of the stage this group vertex belongs to.
	 * 
	 * @return the number of the stage this group vertex belongs to
	 */
	public int getStageNumber() {

		ExecutionStage executionStage = null;

		synchronized (this) {
			executionStage = this.executionStage;
		}

		// Make sure we do not hold a lock on the execution stage object when we call for the stage number
		return executionStage.getStageNumber();
	}

	/**
	 * Changes the number of members this group vertex currently has.
	 * 
	 * @param newNumberOfMembers
	 *        the new number of members
	 * @throws GraphConversionException
	 *         thrown if the number of members for this group vertex cannot be set to the desired value
	 */
	public void changeNumberOfGroupMembers(int newNumberOfMembers) throws GraphConversionException {

		// If the requested number of members does not change, do nothing
		if (newNumberOfMembers == this.getCurrentNumberOfGroupMembers()) {
			return;
		}

		// If the number of members is user defined, prevent overwriting
		if (this.userDefinedNumberOfMembers != -1) {
			if (this.userDefinedNumberOfMembers == getCurrentNumberOfGroupMembers()) { // Note that
				// this.userDefinedNumberOfMembers
				// is final and requires no
				// locking!
				throw new GraphConversionException("Cannot overwrite user defined number of group members");
			}
		}

		// Make sure the value of newNumber is valid
		if (this.getMinimumNumberOfGroupMember() < 1) {
			throw new GraphConversionException("The minimum number of members is below 1 for group vertex "
				+ this.getName());
		}

		if ((this.getMaximumNumberOfGroupMembers() != -1)
			&& (this.getMaximumNumberOfGroupMembers() < this.getMinimumNumberOfGroupMember())) {
			throw new GraphConversionException(
				"The maximum number of members is smaller than the minimum for group vertex " + this.getName());
		}

		if (newNumberOfMembers < this.getMinimumNumberOfGroupMember()) {
			throw new GraphConversionException("Number of members must be at least "
				+ this.getMinimumNumberOfGroupMember());
		}

		if ((this.getMaximumNumberOfGroupMembers() != -1)
			&& (newNumberOfMembers > this.getMaximumNumberOfGroupMembers())) {
			throw new GraphConversionException("Number of members cannot exceed "
				+ this.getMaximumNumberOfGroupMembers());
		}

		// Unwire this vertex before we adjust the number of members
		unwire();

		if (newNumberOfMembers < this.getCurrentNumberOfGroupMembers()) {

			while (this.getCurrentNumberOfGroupMembers() > newNumberOfMembers) {

				synchronized (this.groupMembers) {
					this.groupMembers.remove(this.groupMembers.size() - 1);
				}
			}

		} else {

			while (this.getCurrentNumberOfGroupMembers() < newNumberOfMembers) {

				synchronized (this.groupMembers) {
					try {
						final ExecutionVertex vertex = this.groupMembers.get(0).splitVertex();
						// vertex.setInstance(new DummyInstance(vertex.getInstance().getType()));
						this.groupMembers.add(vertex);
					} catch (Exception e) {
						throw new GraphConversionException(StringUtils.stringifyException(e));
					}
				}
			}
		}

		// After the number of members is adjusted we start rewiring
		wire();

		// Update the index and size information attached to the vertices
		synchronized (this.groupMembers) {
			for (int i = 0; i < this.groupMembers.size(); i++) {
				final ExecutionVertex vertex = this.groupMembers.get(i);
				vertex.getEnvironment().setIndexInSubtaskGroup(i);
				vertex.getEnvironment().setCurrentNumberOfSubtasks(this.groupMembers.size());
			}
		}

		// Repair instance assignment
		reassignInstances();
		// This operation cannot affect the stages
		// FH: Repairing Instance Assingments when not all group vertices have the right number of sub-tasks does not
		// work
		// this.executionGraph.repairInstanceAssignment();
	}

	protected void unwire() throws GraphConversionException {

		synchronized (this.forwardLinks) {
			// Remove all channels to consuming tasks
			Iterator<ExecutionGroupEdge> it = this.forwardLinks.iterator();

			while (it.hasNext()) {
				final ExecutionGroupEdge edge = it.next();
				this.executionGraph.unwire(edge.getSourceVertex(), edge.getIndexOfOutputGate(), edge.getTargetVertex(),
					edge.getIndexOfInputGate());
			}
		}

		synchronized (this.backwardLinks) {
			// Remove all channels from producing tasks
			Iterator<ExecutionGroupEdge> it = this.backwardLinks.iterator();
			while (it.hasNext()) {
				final ExecutionGroupEdge edge = it.next();
				this.executionGraph.unwire(edge.getSourceVertex(), edge.getIndexOfOutputGate(), edge.getTargetVertex(),
					edge.getIndexOfInputGate());
			}
		}
	}

	protected void wire() throws GraphConversionException {

		synchronized (this.forwardLinks) {
			Iterator<ExecutionGroupEdge> it = this.forwardLinks.iterator();
			while (it.hasNext()) {
				final ExecutionGroupEdge edge = it.next();
				this.executionGraph.wire(edge.getSourceVertex(), edge.getIndexOfOutputGate(), edge.getTargetVertex(),
					edge.getIndexOfInputGate(), edge.getChannelType(), edge.getCompressionLevel());
			}
		}

		synchronized (this.backwardLinks) {
			// Remove all channels from producing tasks
			Iterator<ExecutionGroupEdge> it = this.backwardLinks.iterator();
			while (it.hasNext()) {
				final ExecutionGroupEdge edge = it.next();
				this.executionGraph.wire(edge.getSourceVertex(), edge.getIndexOfOutputGate(), edge.getTargetVertex(),
					edge.getIndexOfInputGate(), edge.getChannelType(), edge.getCompressionLevel());
			}
		}
	}

	/**
	 * Sets the input splits that should be assigned to this group vertex.
	 * 
	 * @param inputSplits
	 *        the input splits that shall be assigned to this group vertex
	 */
	public synchronized void setInputSplits(InputSplit[] inputSplits) {
		this.inputSplits = inputSplits;
	}

	/**
	 * Returns the input splits assigned to this group vertex.
	 * 
	 * @return the input splits, possibly <code>null</code> if the group vertex does not represent an input vertex
	 */
	public synchronized InputSplit[] getInputSplits() {
		return this.inputSplits;
	}

	public ExecutionGroupEdge getForwardEdge(int index) {

		synchronized (this.forwardLinks) {
			if (index < this.forwardLinks.size()) {
				return this.forwardLinks.get(index);
			}
		}

		return null;
	}

	public ExecutionGroupEdge getBackwardEdge(int index) {

		synchronized (this.backwardLinks) {
			if (index < this.backwardLinks.size()) {
				return this.backwardLinks.get(index);
			}
		}

		return null;
	}

	public List<ExecutionGroupEdge> getForwardEdges(ExecutionGroupVertex groupVertex) {

		final List<ExecutionGroupEdge> edges = new ArrayList<ExecutionGroupEdge>();

		synchronized (this.forwardLinks) {

			final Iterator<ExecutionGroupEdge> it = this.forwardLinks.iterator();
			while (it.hasNext()) {

				final ExecutionGroupEdge edge = it.next();
				if (edge.getTargetVertex() == groupVertex) {
					edges.add(edge);
				}
			}
		}

		return edges;
	}

	public List<ExecutionGroupEdge> getBackwardEdges(ExecutionGroupVertex groupVertex) {

		List<ExecutionGroupEdge> edges = new ArrayList<ExecutionGroupEdge>();

		synchronized (this.backwardLinks) {

			final Iterator<ExecutionGroupEdge> it = this.backwardLinks.iterator();
			while (it.hasNext()) {

				final ExecutionGroupEdge edge = it.next();
				if (edge.getSourceVertex() == groupVertex) {
					edges.add(edge);
				}
			}
		}

		return edges;
	}

	public boolean isNumberOfMembersUserDefined() {
		// No need to synchronize this method
		return (this.userDefinedNumberOfMembers == -1) ? false : true;
	}

	public int getUserDefinedNumberOfMembers() {
		// No need to synchronize this method
		return this.userDefinedNumberOfMembers;
	}

	public boolean isInstanceTypeUserDefined() {
		// No need to synchronize this method
		return this.userDefinedInstanceType;
	}

	public synchronized void setInstanceType(InstanceType instanceType) throws GraphConversionException {

		if (this.userDefinedInstanceType) {
			throw new GraphConversionException("Cannot overwrite user defined instance type "
				+ instanceType.getIdentifier());
		}

		this.instanceType = instanceType;

		synchronized (this.groupMembers) {
			// Reset instance allocation of all members and let reassignInstances do the work
			for (int i = 0; i < this.groupMembers.size(); i++) {
				final ExecutionVertex vertex = this.groupMembers.get(i);
				vertex.setAllocatedResource(null);
			}
		}

		reassignInstances();
		// This operation cannot affect the stages
		this.executionGraph.repairInstanceAssignment();
	}

	public synchronized InstanceType getInstanceType() {
		return this.instanceType;
	}

	public boolean isNumberOfSubtasksPerInstanceUserDefined() {
		// No need to synchronize this method
		return this.userDefinedNumberOfSubtasksPerInstance;
	}

	public synchronized void setNumberOfSubtasksPerInstance(int numberOfSubtasksPerInstance)
			throws GraphConversionException {

		if (this.userDefinedNumberOfSubtasksPerInstance
			&& (numberOfSubtasksPerInstance != this.numberOfSubtasksPerInstance)) {
			throw new GraphConversionException("Cannot overwrite user defined number of subtasks per instance");
		}

		this.numberOfSubtasksPerInstance = numberOfSubtasksPerInstance;

		// The assignment of instances may have changed, so we need to reasign them
		reassignInstances();
		// This operation cannot affect the stages
		this.executionGraph.repairInstanceAssignment();
	}

	public synchronized int getNumberOfSubtasksPerInstance() {
		return this.numberOfSubtasksPerInstance;
	}

	public synchronized void shareInstancesWith(ExecutionGroupVertex groupVertex) throws GraphConversionException {

		if (userDefinedVertexToShareInstancesWith && this.vertexToShareInstancesWith != null) {
			throw new GraphConversionException("Cannot overwrite user defined vertex to share instances with");
		}

		if (groupVertex == null) {
			throw new IllegalArgumentException("shareInstancesWith: argument is null!");
		}

		if (this.vertexToShareInstancesWith != null) {
			this.vertexToShareInstancesWith.removeFromVerticesSharingInstances(this);
		}

		this.vertexToShareInstancesWith = groupVertex;
		this.vertexToShareInstancesWith.addToVerticesSharingInstances(this);

		reassignInstances();
		// This operation cannot affect the stages
		this.executionGraph.repairInstanceAssignment();
	}

	public boolean isVertexToShareInstanceWithUserDefined() {
		// No need to synchronize this method
		return this.userDefinedVertexToShareInstancesWith;
	}

	/**
	 * Returns the configuration object of the original job vertex.
	 * 
	 * @return the configuration object of the original job vertex
	 */
	public Configuration getConfiguration() {
		// No need to synchronize this method
		return this.configuration;
	}

	/**
	 * Returns the execution signature of this vertex.
	 * 
	 * @return the execution signature of this vertex
	 */
	public ExecutionSignature getExecutionSignature() {
		// No need to synchronize this method
		return this.executionSignature;
	}

	private void addToVerticesSharingInstances(ExecutionGroupVertex groupVertex) {

		synchronized (this.verticesSharingInstances) {
			if (!this.verticesSharingInstances.contains(groupVertex)) {
				this.verticesSharingInstances.add(groupVertex);
			}
		}
	}

	private void removeFromVerticesSharingInstances(ExecutionGroupVertex groupVertex) {

		synchronized (this.verticesSharingInstances) {
			this.verticesSharingInstances.remove(groupVertex);
		}
	}

	public synchronized void reassignInstances() {

		int numberOfRequiredInstances = 0;

		synchronized (this.groupMembers) {
			numberOfRequiredInstances = (this.groupMembers.size() / this.numberOfSubtasksPerInstance)
				+ (((this.groupMembers.size() % this.numberOfSubtasksPerInstance) != 0) ? 1 : 0);
		}
		final List<AllocatedResource> availableInstances = collectAvailabbleResources();

		// Check if the number of available instances is sufficiently large, if not generate new instances
		while (availableInstances.size() < numberOfRequiredInstances) {
			final AllocatedResource newAllocatedResource = new AllocatedResource(DummyInstance
				.createDummyInstance(this.instanceType), this.instanceType, null);
			availableInstances.add(newAllocatedResource);
		}

		// Now assign the group members to the available instances
		synchronized (this.groupMembers) {
			final Iterator<ExecutionVertex> it = this.groupMembers.iterator();
			int instanceIndex = 0, i = 0;
			// TODO: Changing the size of index steps may not be beneficial in all situations
			int sizeOfIndexStep = availableInstances.size() / numberOfRequiredInstances;

			while (it.hasNext()) {
				final ExecutionVertex vertex = it.next();
				// System.out.println("Assigning " + getName() + " " + i + " to instance " +
				// availableInstances.get(instanceIndex));
				vertex.setAllocatedResource(availableInstances.get(instanceIndex));

				if ((++i % this.numberOfSubtasksPerInstance) == 0) {
					instanceIndex += sizeOfIndexStep;
				}
			}
		}

		synchronized (this.verticesSharingInstances) {
			// Since the number of instances may have changed, other vertices sharing instances with us could be
			// affected by that change
			final Iterator<ExecutionGroupVertex> it2 = this.verticesSharingInstances.iterator();
			while (it2.hasNext()) {
				final ExecutionGroupVertex groupVertex = it2.next();
				groupVertex.reassignInstances();
			}
		}
	}

	private synchronized List<AllocatedResource> collectAvailabbleResources() {

		List<AllocatedResource> availableResources;

		if (this.vertexToShareInstancesWith != null) {
			availableResources = this.vertexToShareInstancesWith.collectAvailabbleResources();
		} else {
			availableResources = new ArrayList<AllocatedResource>();

			synchronized (this.groupMembers) {

				final Iterator<ExecutionVertex> it = this.groupMembers.iterator();
				while (it.hasNext()) {

					final ExecutionVertex vertex = it.next();
					final AllocatedResource allocatedResource = vertex.getAllocatedResource();
					if (allocatedResource != null) {
						if (!availableResources.contains(allocatedResource)) {
							availableResources.add(allocatedResource);
						}
					}
				}
			}
		}

		return availableResources;
	}

	/**
	 * Checks if this vertex is an input vertex in its stage, i.e. has either no
	 * incoming connections or only incoming connections to group vertices in a lower stage.
	 * 
	 * @return <code>true</code> if this vertex is an input vertex, <code>false</code> otherwise
	 */
	public boolean isInputVertex() {

		synchronized (this.backwardLinks) {

			if (this.backwardLinks.size() == 0) {
				return true;
			}

			final Iterator<ExecutionGroupEdge> it = this.backwardLinks.iterator();
			while (it.hasNext()) {
				if (it.next().getSourceVertex().getStageNumber() == this.getStageNumber()) {
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Checks if this vertex is an output vertex in its stage, i.e. has either no
	 * outgoing connections or only outgoing connections to group vertices in a higher stage.
	 * 
	 * @return <code>true</code> if this vertex is an output vertex, <code>false</code> otherwise
	 */
	public boolean isOutputVertex() {

		synchronized (this.forwardLinks) {

			if (this.forwardLinks.size() == 0) {
				return true;
			}

			final Iterator<ExecutionGroupEdge> it = this.forwardLinks.iterator();
			while (it.hasNext()) {
				if (it.next().getTargetVertex().getStageNumber() == this.getStageNumber()) {
					return false;
				}
			}
		}

		return true;
	}

	public synchronized ExecutionGroupVertex getVertexToShareInstancesWith() {
		return this.vertexToShareInstancesWith;
	}

	/**
	 * Returns the ID of the job vertex which is represented by
	 * this group vertex.
	 * 
	 * @return the ID of the job vertex which is represented by
	 *         this group vertex
	 */
	public JobVertexID getJobVertexID() {
		// No need to synchronize this method
		return this.jobVertexID;
	}
}