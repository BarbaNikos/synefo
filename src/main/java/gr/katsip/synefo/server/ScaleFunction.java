package gr.katsip.synefo.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

public class ScaleFunction {

	public HashMap<String, ArrayList<String>> physical_topology;

	public HashMap<String, ArrayList<String>> active_topology;

	public ScaleFunction(HashMap<String, ArrayList<String>> physical_topology, 
			HashMap<String, ArrayList<String>> active_topology) {
		this.physical_topology = physical_topology;
		this.active_topology = active_topology;
	}

	public synchronized String produceScaleOutCommand(String upstream_task, String overloadedWorker) {
		if(overloadedWorker.toLowerCase().contains("spout"))
			return "";
		if(upstream_task == null || upstream_task.equals(""))
			return "";
		ArrayList<String> available_nodes = getInActiveNodes(upstream_task, overloadedWorker);
		if(available_nodes == null || available_nodes.size() == 0)
			return "";
		String selectedTask = randomChoice(available_nodes);
//		ArrayList<String> active_nodes = active_topology.get(upstream_task);
//		active_nodes.add(selectedTask);
//		active_topology.put(upstream_task, active_nodes);
		addActiveNodeTopology(selectedTask);
		return "ADD~" + selectedTask;
	}

	public synchronized String produceScaleInCommand(String underloadedWorker) {
		String upstream_task = getParentNode(underloadedWorker.substring(0, underloadedWorker.lastIndexOf(':')),
				underloadedWorker.substring(underloadedWorker.lastIndexOf(':') + 1, underloadedWorker.lastIndexOf('@')));
		ArrayList<String> active_nodes = getActiveNodes(upstream_task, underloadedWorker);
		if(active_nodes.size() > 1) {
			String selectedTask = randomChoice(active_nodes);
//			active_nodes.remove(active_nodes.lastIndexOf(selectedTask));
//			active_topology.put(upstream_task, active_nodes);
			removeActiveNodeGc(selectedTask);
			return "REMOVE~" + upstream_task;
		}else {
			return "";
		}
	}
	

	public void removeActiveNodeGc(String removedNode) {
		/**
		 * Remove removedNode from all its upstream-nodes lists
		 */
		Iterator<Entry<String, ArrayList<String>>> itr = active_topology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			String upstreamNode = pair.getKey();
			ArrayList<String> downstreamNodes = pair.getValue();
			if(downstreamNodes.indexOf(removedNode) >= 0) {
				downstreamNodes.remove(downstreamNodes.indexOf(removedNode));
				active_topology.put(upstreamNode, downstreamNodes);
			}
		}
		/**
		 * Remove entry of removedNode (if exists) from active topology
		 */
		if(active_topology.containsKey(removedNode)) {
			active_topology.remove(removedNode);
		}
	}
	
	public void addActiveNodeTopology(String addedNode) {
		/**
		 * Add addedNode to the active topology downstream-lists of 
		 * all of addedNode's upstream nodes.
		 */
		Iterator<Entry<String, ArrayList<String>>> itr = physical_topology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			String upstreamNode = pair.getKey();
			ArrayList<String> physicalDownStreamNodes = pair.getValue();
			if(physicalDownStreamNodes.indexOf(addedNode) >= 0) {
				ArrayList<String> activeDownStreamNodes = active_topology.get(upstreamNode);
				activeDownStreamNodes.add(addedNode);
				active_topology.put(upstreamNode, activeDownStreamNodes);
			}
		}
		/**
		 * Add an entry for addedNode with all of its active downstream nodes
		 */
		System.out.println("ScaleFunction.addActiveNodeTopology: addedNode: " + addedNode + ", physical topology: " + physical_topology.get(addedNode));
		ArrayList<String> downStreamNodes = physical_topology.get(addedNode);
		ArrayList<String> activeDownStreamNodes = new ArrayList<String>();
		for(String node : downStreamNodes) {
			if(active_topology.containsKey(node)) {
				activeDownStreamNodes.add(node);
			}
		}
		active_topology.put(addedNode, activeDownStreamNodes);
	}

	public String getParentNode(String task_name, String task_id) {
		Iterator<Entry<String, ArrayList<String>>> itr = physical_topology.entrySet().iterator();
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
		ArrayList<String> active_nodes = active_topology.get(upstream_task);
		ArrayList<String> physical_nodes = physical_topology.get(upstream_task);
		for(String task : physical_nodes) {
			if(active_nodes.lastIndexOf(task) < 0 && overloadedWorker.equals(task) == false) {
				available_nodes.add(task);
			}
		}
		return available_nodes;
	}

	public ArrayList<String> getActiveNodes(String upstream_task, String underloadedNode) {
		ArrayList<String> active_nodes = new ArrayList<String>(active_topology.get(upstream_task));
		for(String task : active_nodes) {
			if(task.equals(underloadedNode)) {
				active_nodes.remove(active_nodes.lastIndexOf(task));
				return active_nodes;
			}
		}
		return null;
	}

	private String randomChoice(ArrayList<String> available_nodes) {
		Random random = new Random();
		return available_nodes.get(random.nextInt(available_nodes.size()));
	}
}
