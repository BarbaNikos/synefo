package gr.katsip.deprecated.server2;

import gr.katsip.synefo.utils.JoinOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @deprecated
 */
public class ScaleFunction {

	public ConcurrentHashMap<String, ArrayList<String>> physicalTopology;

	private ConcurrentHashMap<String, ArrayList<String>> activeTopology;
	
	private ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation = null;
	
	private final ReadWriteLock activeTopologyLock = new ReentrantReadWriteLock();

	public ScaleFunction(ConcurrentHashMap<String, ArrayList<String>> physicalTopology, 
			ConcurrentHashMap<String, ArrayList<String>> activeTopology, 
			ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation) {
		this.physicalTopology = physicalTopology;
		this.activeTopology = activeTopology;
		this.taskToJoinRelation = taskToJoinRelation;
	}
	
	public ConcurrentHashMap<String, ArrayList<String>> getActiveTopology() {
		ConcurrentHashMap<String, ArrayList<String>> activeTopologyCopy = null;
		activeTopologyLock.readLock().lock();
		activeTopologyCopy = new ConcurrentHashMap<String, ArrayList<String>>(activeTopology);
		activeTopologyLock.readLock().unlock();
		return activeTopologyCopy;
	}

	/**
	 * Modifies activeTopology
	 * @param upstreamTask
	 * @param overloadedWorker
	 * @return
	 */
	public String produceScaleOutCommand(String upstreamTask, String overloadedWorker) {
		if(overloadedWorker.toLowerCase().contains("spout")) {
			return "";
		}else if(upstreamTask == null || upstreamTask.equals("")) {
			return "";
		}
		activeTopologyLock.writeLock().lock();
		if(activeTopology.containsKey(overloadedWorker) == false) {
			activeTopologyLock.writeLock().unlock();
			return "";
		}
		Integer identifier = Integer.parseInt(overloadedWorker.split("[:@]")[1]);
		ArrayList<String> availableNodes = null;
		if(taskToJoinRelation.containsKey(identifier)) {
			System.out.println("ScaleFunction.produceScaleOutCommand(): overloadedTask " + identifier + " is a join operator");
			availableNodes = ScaleFunction.getInActiveJoinNodes(physicalTopology, 
					activeTopology, upstreamTask, overloadedWorker, taskToJoinRelation);
		}else {
			System.out.println("ScaleFunction.produceScaleOutCommand(): overloadedTask " + identifier + " is NOT a join operator");
			availableNodes = ScaleFunction.getInActiveNodes(
					physicalTopology, activeTopology,
					upstreamTask, overloadedWorker);
		}
		if(availableNodes == null || availableNodes.size() == 0) {
			activeTopologyLock.writeLock().unlock();
			return "";
		}
		String selectedTask = randomChoice(availableNodes);
		System.out.println("ScaleFunction.produceScaleOutCommand(): selected task to scale-out: " + selectedTask);
		if(selectedTask != null && selectedTask.length() > 0) {
			addActiveNodeTopology(selectedTask);
			activeTopologyLock.writeLock().unlock();
			return "ADD~" + selectedTask;
		}else {
			activeTopologyLock.writeLock().unlock();
			return "";
		}
	}

	/**
	 * Modifies activeTopology
	 * @param upstreamTask
	 * @param underloadedWorker
	 * @return
	 */
	public String produceScaleInCommand(String upstreamTask, String underloadedWorker) {
		activeTopologyLock.writeLock().lock();
		if(activeTopology.containsKey(underloadedWorker) == false) {
			activeTopologyLock.writeLock().unlock();
			return "";
		}
		ArrayList<String> activeNodes = null;
		Integer identifier = Integer.parseInt(underloadedWorker.split("[:@]")[1]);
		if(taskToJoinRelation.containsKey(identifier)) {
			activeNodes = ScaleFunction.getActiveJoinNodes(physicalTopology, 
					activeTopology, upstreamTask, underloadedWorker, taskToJoinRelation);
		}else {
			activeNodes = ScaleFunction.getActiveNodes(
					physicalTopology, activeTopology,
					upstreamTask, underloadedWorker);
		}
		if(activeNodes != null && activeNodes.size() > 1) {
			removeActiveNodeGc(underloadedWorker);
			activeTopologyLock.writeLock().unlock();
			return "REMOVE~" + underloadedWorker;
		}else {
			activeTopologyLock.writeLock().unlock();
			return "";
		}
	}

	public void removeActiveNode(String node) {
		activeTopologyLock.writeLock().lock();
		removeActiveNodeGc(node);
		activeTopologyLock.writeLock().unlock();
	}

	private void removeActiveNodeGc(String removedNode) {
		Iterator<Entry<String, ArrayList<String>>> itr = activeTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			String upstreamNode = pair.getKey();
			ArrayList<String> downstreamNodes = pair.getValue();
			if(downstreamNodes.indexOf(removedNode) >= 0) {
				downstreamNodes.remove(downstreamNodes.indexOf(removedNode));
				activeTopology.put(upstreamNode, downstreamNodes);
			}
		}
		/**
		 * Remove entry of removedNode (if exists) from active topology
		 */
		if(activeTopology.containsKey(removedNode)) {
			activeTopology.remove(removedNode);
		}
	}
	
	public void addInactiveNode(String node) {
		activeTopologyLock.writeLock().lock();
		addActiveNodeTopology(node);
		activeTopologyLock.writeLock().unlock();
	}

	private void addActiveNodeTopology(String addedNode) {
		/**
		 * Add addedNode to the active topology downstream-lists of 
		 * all of addedNode's upstream nodes.
		 */
		Iterator<Entry<String, ArrayList<String>>> itr = activeTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			String upstreamNode = pair.getKey();
			ArrayList<String> activeDownStreamNodes = pair.getValue();
			ArrayList<String> physicalDownStreamNodes = physicalTopology.get(upstreamNode);
			if(physicalDownStreamNodes.indexOf(addedNode) >= 0 && activeDownStreamNodes.indexOf(addedNode) < 0) {
				activeDownStreamNodes.add(addedNode);
				activeTopology.put(upstreamNode, activeDownStreamNodes);
			}
		}
		/**
		 * Add an entry for addedNode with all of its active downstream nodes
		 */
		ArrayList<String> downStreamNodes = physicalTopology.get(addedNode);
		ArrayList<String> activeDownStreamNodes = new ArrayList<String>();
		for(String node : downStreamNodes) {
			if(activeTopology.containsKey(node)) {
				activeDownStreamNodes.add(node);
			}
		}
		activeTopology.put(addedNode, activeDownStreamNodes);
	}
	
	public void updateTaskAddress(String taskName, Integer identifier, String newAddress, String previousAddress) {
		ArrayList<String> parentNodes = null;
		activeTopologyLock.writeLock().lock();
		if(physicalTopology.containsKey(taskName + ":" + identifier + "@" + previousAddress)) {
			parentNodes = ScaleFunction.getInverseTopology(physicalTopology).get(taskName + ":" + identifier + "@" + previousAddress);
			if(parentNodes != null) {
				for(String parent : parentNodes) {
					ArrayList<String> childTasks = physicalTopology.get(parent);
					for(int i = 0; i < childTasks.size(); i++) {
						if(childTasks.get(i).equals(taskName + ":" + identifier + "@" + previousAddress)) {
							childTasks.set(i, taskName + ":" + identifier + "@" + newAddress);
							physicalTopology.put(parent, childTasks);
							break;
						}
					}
				}
			}
			ArrayList<String> childTasks = physicalTopology.get(taskName + ":" + identifier + "@" + previousAddress);
			physicalTopology.put(taskName + ":" + identifier + "@" + newAddress, childTasks);
			physicalTopology.remove(taskName + ":" + identifier + "@" + previousAddress);
		}
		if(activeTopology.containsKey(taskName + ":" + identifier + "@" + previousAddress)) {
			parentNodes = ScaleFunction.getInverseTopology(activeTopology).get(taskName + ":" + identifier + "@" + previousAddress);
			if(parentNodes != null) {
				for(String parent : parentNodes) {
					ArrayList<String> childTasks = activeTopology.get(parent);
					for(int i = 0; i < childTasks.size(); i++) {
						if(childTasks.get(i).equals(taskName + ":" + identifier + "@" + previousAddress)) {
							childTasks.set(i, taskName + ":" + identifier + "@" + newAddress);
							activeTopology.put(parent, childTasks);
							break;
						}
					}
				}
			}
			ArrayList<String> childTasks = activeTopology.get(taskName + ":" + identifier + "@" + previousAddress);
			activeTopology.put(taskName + ":" + identifier + "@" + newAddress, childTasks);
			activeTopology.remove(taskName + ":" + identifier + "@" + previousAddress);
		}
		activeTopologyLock.writeLock().unlock();
	}

	public static String getParentNode(ConcurrentHashMap<String, ArrayList<String>> physicalTopology, String task_name, String task_id) {
		Iterator<Entry<String, ArrayList<String>>> itr = physicalTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			ArrayList<String> tasks = pair.getValue();
			for(String t : tasks) {
				if(t.equals(task_name + ":" + task_id))
					return pair.getKey();
			}
		}
		return null;
	}

	public static ArrayList<String> getInActiveNodes(
			ConcurrentHashMap<String, ArrayList<String>> physicalTopology, 
			ConcurrentHashMap<String, ArrayList<String>> activeTopology, 
			String upstream_task, String overloadedWorker) {
		ArrayList<String> available_nodes = new ArrayList<String>();
		ArrayList<String> active_nodes = activeTopology.get(upstream_task);
		ArrayList<String> physical_nodes = physicalTopology.get(upstream_task);
		for(String task : physical_nodes) {
			if(active_nodes.lastIndexOf(task) < 0 && overloadedWorker.equals(task) == false) {
				available_nodes.add(task);
			}
		}
		return available_nodes;
	}
	
	public static ArrayList<String> getInActiveJoinNodes(ConcurrentHashMap<String, ArrayList<String>> physicalTopology, 
			ConcurrentHashMap<String, ArrayList<String>> activeTopology, 
			String upstream_task, String overloadedWorker, 
			ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation) {
		System.out.println("ScaleFunction.getInActiveJoinNodes(): called for " + overloadedWorker + ", with parent node: " + upstream_task);
		Integer overloadedIdentifier = Integer.parseInt(overloadedWorker.split("[:@]")[1]);
		ArrayList<String> available_nodes = new ArrayList<String>();
		ArrayList<String> physical_nodes = physicalTopology.get(upstream_task);
		physical_nodes.removeAll(activeTopology.get(upstream_task));
		for(String task : physical_nodes) {
			Integer identifier = Integer.parseInt(task.split("[:@]")[1]);
			if(taskToJoinRelation.get(identifier).getRelation().equals(taskToJoinRelation.get(overloadedIdentifier).getRelation())) {
				available_nodes.add(task);
			}
		}
		System.out.println("ScaleFunction.getInActiveJoinNodes(): resulted with list of available nodes: " + available_nodes.toString());
		return available_nodes;
	}

	public static ArrayList<String> getActiveNodes(
			ConcurrentHashMap<String, ArrayList<String>> physicalTopology, 
			ConcurrentHashMap<String, ArrayList<String>> activeTopology, 
			String upstreamTask, String underloadedNode) {
		if(activeTopology.containsKey(upstreamTask) == false)
			return null;
		ArrayList<String> activeNodes = new ArrayList<String>(activeTopology.get(upstreamTask));
		if(activeNodes == null || activeNodes.size() == 0)
			return null;
		for(String task : activeNodes) {
			if(task.equals(underloadedNode)) {
				return activeNodes;
			}
		}
		return null;
	}
	
	public static ArrayList<String> getActiveJoinNodes(ConcurrentHashMap<String, ArrayList<String>> physicalTopology, 
			ConcurrentHashMap<String, ArrayList<String>> activeTopology, 
			String upstreamTask, String underloadedNode, 
			ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation) {
		if(activeTopology.containsKey(upstreamTask) == false)
			return null;
		ArrayList<String> activeNodes = new ArrayList<String>(activeTopology.get(upstreamTask));
		ArrayList<String> sameRelationActiveNodes = new ArrayList<String>();
		Integer underloadedIdentifier = Integer.parseInt(underloadedNode.split("[:@]")[1]);
		for(String task : activeNodes) {
			Integer taskIdentifier = Integer.parseInt(task.split("[:@]")[1]);
			if(taskToJoinRelation.containsKey(taskIdentifier) && 
					taskToJoinRelation.get(taskIdentifier).getRelation().equals(taskToJoinRelation.get(underloadedIdentifier).getRelation())) {
				sameRelationActiveNodes.add(task);
			}
		}
		return sameRelationActiveNodes;
	}

	private static String randomChoice(ArrayList<String> available_nodes) {
		Random random = new Random();
		return available_nodes.get(random.nextInt(available_nodes.size()));
	}

	public static String produceActivateCommand(String addCommand) {
		return "ACTIVATE~" + addCommand.substring(addCommand.lastIndexOf("~") + 1, addCommand.length());
	}

	public static String produceDeactivateCommand(String removeCommand) {
		return "DEACTIVATE~" + removeCommand.substring(removeCommand.lastIndexOf("~") + 1, removeCommand.length());
	}

	public static ConcurrentHashMap<String, ArrayList<String>> getInverseTopology(ConcurrentHashMap<String, ArrayList<String>> topology) {
		ConcurrentHashMap<String, ArrayList<String>> inverseTopology = new ConcurrentHashMap<String, ArrayList<String>>();
		if(topology == null || topology.size() == 0)
			return null;
		Iterator<Entry<String, ArrayList<String>>> itr = topology.entrySet().iterator();
		while(itr.hasNext()) {
			Map.Entry<String, ArrayList<String>> entry = itr.next();
			String taskName = entry.getKey();
			ArrayList<String> downStreamNames = entry.getValue();
			if(downStreamNames != null && downStreamNames.size() > 0) {
				for(String downStreamTask : downStreamNames) {
					if(inverseTopology.containsKey(downStreamTask)) {
						ArrayList<String> parentList = inverseTopology.get(downStreamTask);
						if(parentList.indexOf(taskName) < 0) {
							parentList.add(taskName);
							inverseTopology.put(downStreamTask, parentList);
						}
					}else {
						ArrayList<String> parentList = new ArrayList<String>();
						parentList.add(taskName);
						inverseTopology.put(downStreamTask, parentList);
					}
				}
				if(inverseTopology.containsKey(taskName) == false)
					inverseTopology.put(taskName, new ArrayList<String>());
			}
		}
		return inverseTopology;
	}

	/**
	 * This function separates the topology operators into different layers (stages) of computation. In each layer, 
	 * the source operators (nodes with no upstream operators) and the drain operators (operators with no downstream operators) are 
	 * not included. For each operator, a signature-key is created by appending all parent operators of that node, followed by 
	 * all child operators. For instance, if operator X has operators {A, B} as parents, and operators {Z, Y} as children, the 
	 * signature-key is created as : "A,B,Z,Y". After a signature-key is created, the operator is stored in a HashMap with key 
	 * its own signature and its name is added in a list. If two or more operators have the same signature-key they are added 
	 * in the same bucket in the HashMap.
	 * @param physicalTopology The physical topology of operators in synefo
	 * @param inverseTopology The map that contains the parent operators (upstream) of each operator
	 * @return a HashMap with the the operators separated in different buckets, according to their parent-operators list and children-operator lists.
	 */
	public static ConcurrentHashMap<String, ArrayList<String>> produceTopologyLayers(
			ConcurrentHashMap<String, ArrayList<String>> physicalTopology, 
			ConcurrentHashMap<String, ArrayList<String>> inverseTopology) {
		ConcurrentHashMap<String, ArrayList<String>> operatorLayers = new ConcurrentHashMap<String, ArrayList<String>>();
		Iterator<Entry<String, ArrayList<String>>> itr = physicalTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			String taskName = pair.getKey();
			ArrayList<String> childOperators = pair.getValue();
			ArrayList<String> parentOperators = inverseTopology.get(taskName);
			if(childOperators != null && childOperators.size() > 0 && 
					parentOperators != null && parentOperators.size() > 0) {
				StringBuilder strBuild = new StringBuilder();
				for(String parent : parentOperators) {
					strBuild.append(parent + ",");
				}
				for(String child : childOperators) {
					strBuild.append(child + ",");
				}
				strBuild.setLength(strBuild.length() - 1);
				String key = strBuild.toString();
				if(operatorLayers.containsKey(key)) {
					ArrayList<String> layerOperators = operatorLayers.get(key);
					layerOperators.add(taskName);
					operatorLayers.put(key, layerOperators);
				}else {
					ArrayList<String> layerOperators = new ArrayList<String>();
					layerOperators.add(taskName);
					operatorLayers.put(key, layerOperators);
				}
			}
		}
		return operatorLayers;
	}

	/**
	 * Function to randomly select the initially active topology of operators. This function includes 
	 * all source operators (spouts) and all drain operators (bolts in the end of the topology) in the 
	 * initial topology. The rest of the operators are divided into different layers (stages of computation), 
	 * and from each layer, one operator is picked randomly to be active in the beginning.
	 * @param physicalTopology the physical topology of operators
	 * @param inverseTopology a representation of the physical topology in which each key (operator) points to the list with its parent nodes
	 * @return a HashMap with the initially active topology of operators
	 */
	public static HashMap<String, ArrayList<String>> getInitialActiveTopology(
			ConcurrentHashMap<String, ArrayList<String>> physicalTopology, 
			ConcurrentHashMap<String, ArrayList<String>> inverseTopology) {
		HashMap<String, ArrayList<String>> activeTopology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> activeTasks = new ArrayList<String>();
		ConcurrentHashMap<String, ArrayList<String>> layerTopology = ScaleFunction.produceTopologyLayers(
				physicalTopology, inverseTopology);
		/**
		 * Add all source operators first
		 */
		Iterator<Entry<String, ArrayList<String>>> itr = inverseTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			String taskName = pair.getKey();
			ArrayList<String> parentTasks = pair.getValue();
			if(parentTasks == null || parentTasks.size() == 0) {
				activeTasks.add(taskName);
			}
		}
		/**
		 * Add all drain operators second
		 */
		itr = physicalTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			String taskName = pair.getKey();
			ArrayList<String> childTasks = pair.getValue();
			if(childTasks == null || childTasks.size() == 0) {
				activeTasks.add(taskName);
			}
		}
		/**
		 * From each operator layer (stage of computation) add one node
		 */
		itr = layerTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			ArrayList<String> layerTasks = pair.getValue();
			if(layerTasks != null && layerTasks.size() > 0) {
				/**
				 * TODO: Try different policies for picking initially active nodes
				 * (possible research)
				 */
				activeTasks.add(layerTasks.get(0));
			}
		}
		/**
		 * Now create the activeTopology by adding each node 
		 * in the activeNodes list, along with its active downstream 
		 * operators (also in the activeNodes list)
		 */
		for(String activeTask : activeTasks) {
			ArrayList<String> children = physicalTopology.get(activeTask);
			ArrayList<String> activeChildren = new ArrayList<String>();
			for(String childTask : children) {
				if(activeTasks.indexOf(childTask) >= 0) {
					activeChildren.add(childTask);
				}
			}
			activeTopology.put(activeTask, activeChildren);
		}
		return activeTopology;
	}
}
