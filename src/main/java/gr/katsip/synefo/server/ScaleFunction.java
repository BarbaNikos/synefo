package gr.katsip.synefo.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

public class ScaleFunction {

	public HashMap<String, ArrayList<String>> physicalTopology;

	public HashMap<String, ArrayList<String>> activeTopology;
	
	public HashMap<String, ArrayList<String>> inverseTopology;

	public ScaleFunction(HashMap<String, ArrayList<String>> physicalTopology, 
			HashMap<String, ArrayList<String>> activeTopology,
			HashMap<String, ArrayList<String>> inverseTopology) {
		this.physicalTopology = physicalTopology;
		this.activeTopology = activeTopology;
		this.inverseTopology = inverseTopology;
	}

	public synchronized String produceScaleOutCommand(String upstreamTask, String overloadedWorker) {
		if(overloadedWorker.toLowerCase().contains("spout")) {
			return "";
		}else if(upstreamTask == null || upstreamTask.equals("")) {
			return "";
		}
		ArrayList<String> availableNodes = getInActiveNodes(upstreamTask, overloadedWorker);
		if(availableNodes == null || availableNodes.size() == 0)
			return "";
		String selectedTask = randomChoice(availableNodes);
		addActiveNodeTopology(selectedTask);
		return "ADD~" + selectedTask;
	}

	public synchronized String produceScaleInCommand(String underloadedWorker) {
		String upstreamTask = getParentNode(underloadedWorker.substring(0, underloadedWorker.lastIndexOf(':')),
				underloadedWorker.substring(underloadedWorker.lastIndexOf(':') + 1, underloadedWorker.lastIndexOf('@')));
		ArrayList<String> activeNodes = getActiveNodes(upstreamTask, underloadedWorker);
		if(activeNodes != null && activeNodes.size() > 1) {
			removeActiveNodeGc(underloadedWorker);
			return "REMOVE~" + underloadedWorker;
		}else {
			return "";
		}
	}


	public void removeActiveNodeGc(String removedNode) {
		/**
		 * Remove removedNode from all its upstream-nodes lists
		 */
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
		System.out.println("ScaleFunction.removeActiveNodeTopology: removedNode: " + 
				removedNode + ", physical topology: " + physicalTopology.get(removedNode));
		if(activeTopology.containsKey(removedNode)) {
			activeTopology.remove(removedNode);
		}
	}

	public void addActiveNodeTopology(String addedNode) {
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
		System.out.println("ScaleFunction.addActiveNodeTopology: addedNode: " + 
				addedNode + ", physical topology: " + physicalTopology.get(addedNode));
		ArrayList<String> downStreamNodes = physicalTopology.get(addedNode);
		ArrayList<String> activeDownStreamNodes = new ArrayList<String>();
		for(String node : downStreamNodes) {
			if(activeTopology.containsKey(node)) {
				activeDownStreamNodes.add(node);
			}
		}
		activeTopology.put(addedNode, activeDownStreamNodes);
	}

	public String getParentNode(String task_name, String task_id) {
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

	public ArrayList<String> getInActiveNodes(String upstream_task, String overloadedWorker) {
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

	public ArrayList<String> getActiveNodes(String upstream_task, String underloadedNode) {
		if(activeTopology.containsKey(upstream_task) == false)
			return null;
		ArrayList<String> activeNodes = new ArrayList<String>(activeTopology.get(upstream_task));
		if(activeNodes == null || activeNodes.size() == 0)
			return null;
		for(String task : activeNodes) {
			if(task.equals(underloadedNode)) {
				activeNodes.remove(activeNodes.lastIndexOf(task));
				return activeNodes;
			}
		}
		return null;
	}

	private String randomChoice(ArrayList<String> available_nodes) {
		Random random = new Random();
		return available_nodes.get(random.nextInt(available_nodes.size()));
	}
}
