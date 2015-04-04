package gr.katsip.synefo.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicBoolean;

import gr.katsip.synefo.storm.api.Pair;

public class SynefoCoordinatorThread implements Runnable {

	private HashMap<String, ArrayList<String>> physicalTopology;

	private HashMap<String, ArrayList<String>> activeTopology;

	private HashMap<String, ArrayList<String>> inverseTopology;

	private HashMap<String, Integer> taskNameToIdMap;

	private Integer totalTaskNum = -1;

	private ZooMaster tamer;

	private HashMap<String, String> taskIPs;

	private HashMap<String, Pair<Number, Number>> resourceThresholds;

	private String zooHost;

	private Integer zooPort;

	private Thread userInterfaceThread;
	
	private AtomicBoolean operationFlag;

	public SynefoCoordinatorThread(String zooHost, Integer zooPort, HashMap<String, Pair<Number, Number>> resourceThresholds, 
			HashMap<String, ArrayList<String>> physicalTopology, HashMap<String, ArrayList<String>> runningTopology, 
			HashMap<String, Integer> taskNameToIdMap, 
			HashMap<String, String> taskIPs,
			AtomicBoolean operationFlag) {
		this.physicalTopology = physicalTopology;
		this.activeTopology = runningTopology;
		inverseTopology = new HashMap<String, ArrayList<String>>();
		this.taskNameToIdMap = taskNameToIdMap;
		this.taskIPs = taskIPs;
		this.resourceThresholds = resourceThresholds;
		this.zooHost = zooHost;
		this.zooPort = zooPort;
		this.operationFlag = operationFlag;
	}

	public void run() {
		System.out.println("+efo coordinator thread: initiates execution...");
		synchronized(physicalTopology) {
			while(physicalTopology.size() == 0) {
				try {
					physicalTopology.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			totalTaskNum = physicalTopology.size();
		}
		System.out.println("+efo coordinator thread: Received physical topology (size: " + totalTaskNum + ").");
		synchronized(taskNameToIdMap) {
			while(taskNameToIdMap.size() < totalTaskNum) {
				try {
					taskNameToIdMap.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		/**
		 * After every task has registered and 
		 * inserted its task id in the nameToIdMap,
		 * the physicalTopology is updated with the 
		 * task-ids
		 */
		/**
		 * Update ZooKeeper entries and Nodes
		 */
		tamer = new ZooMaster(zooHost, zooPort, physicalTopology, activeTopology, inverseTopology);

		tamer.start();
		tamer.setScaleOutThresholds((double) resourceThresholds.get("cpu").upperBound, 
				(double) resourceThresholds.get("memory").upperBound, 
				(int) resourceThresholds.get("latency").upperBound, 
				(int) resourceThresholds.get("throughput").upperBound);
		tamer.setScaleInThresholds((double) resourceThresholds.get("cpu").lowerBound, 
				(double) resourceThresholds.get("memory").lowerBound, 
				(int) resourceThresholds.get("latency").lowerBound, 
				(int) resourceThresholds.get("throughput").lowerBound);
		tamer.setScaleOutEventWatch();
		tamer.setScaleInEventWatch();

		System.out.println("+efo coordinator thread: received tast name allocation from Storm cluster. Updating internal structures...");
		HashMap<String, ArrayList<String>> updatedTopology = new HashMap<String, ArrayList<String>>();
		HashMap<String, ArrayList<String>> activeUpdatedTopology = new HashMap<String, ArrayList<String>>();
		synchronized(taskNameToIdMap) {
			Iterator<Entry<String, ArrayList<String>>> itr = physicalTopology.entrySet().iterator();
			while(itr.hasNext()) {
				Map.Entry<String, ArrayList<String>> pair = itr.next();
				String taskName = pair.getKey();
				ArrayList<String> downStreamNames = pair.getValue();
				String parentTask = taskName + ":" + Integer.toString(taskNameToIdMap.get(taskName)) + "@" + 
						taskIPs.get(taskName + ":" + Integer.toString(taskNameToIdMap.get(taskName)));
				if(downStreamNames != null && downStreamNames.size() > 0) {
					ArrayList<String> downStreamIds = new ArrayList<String>();
					for(String name : downStreamNames) {
						if(taskNameToIdMap.containsKey(name) == false) {
							assert taskNameToIdMap.containsKey(name) == true;
						}
						String childTask = name + ":" + Integer.toString(taskNameToIdMap.get(name)) + "@" + 
								taskIPs.get(name + ":" + Integer.toString(taskNameToIdMap.get(name)));
						downStreamIds.add(childTask);
						if(inverseTopology.containsKey(childTask)) {
							ArrayList<String> parentList = inverseTopology.get(childTask);
							if(parentList.indexOf(parentTask) < 0) {
								parentList.add(parentTask);
								inverseTopology.put(childTask, parentList);
							}
						}else {
							ArrayList<String> parentList = new ArrayList<String>();
							parentList.add(parentTask);
							inverseTopology.put(childTask, parentList);
						}
					}
					updatedTopology.put(parentTask, downStreamIds);
					if(inverseTopology.containsKey(parentTask) == false)
						inverseTopology.put(parentTask, new ArrayList<String>());
				}else {
					updatedTopology.put(parentTask, new ArrayList<String>());
				}
			}
			activeUpdatedTopology = getInitialActiveTopology(updatedTopology, inverseTopology);
			physicalTopology.clear();
			physicalTopology.putAll(updatedTopology);
			activeTopology.clear();
			activeTopology.putAll(activeUpdatedTopology);
			tamer.setPhysicalTopology();
			tamer.setActiveTopology();
			operationFlag.set(true);

			taskNameToIdMap.clear();
			taskNameToIdMap.notifyAll();
		}

		userInterfaceThread = new Thread(new SynEFOUserInterface(tamer));
		userInterfaceThread.start();
	}

	public int getTaskId(String taskName) {
		Iterator<Entry<String, ArrayList<String>>> itr = physicalTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			if(pair.getKey().startsWith(taskName)) {
				StringTokenizer strTok = new StringTokenizer(pair.getKey(), ":");
				String taskIdWithIp = strTok.nextToken();
				strTok = new StringTokenizer(taskIdWithIp, "@");
				return Integer.parseInt(strTok.nextToken());
			}
		}
		return -1;
	}

	public ArrayList<String> getDownstreamTasks(String taskName, int task_id, String task_ip) {
		if(physicalTopology.containsKey(taskName + ":" + task_id + "@" + task_ip))
			return physicalTopology.get(taskName + ":" + task_id + "@" + task_ip);
		else
			return null;
	}

	public HashMap<String, ArrayList<String>> getInverseTopology() {
		return inverseTopology;
	}

	public ArrayList<String> getUpstreamTasks(String taskName, int task_id, String task_ip) {
		if(inverseTopology.containsKey(taskName + ":" + task_id + "@" + task_ip))
			return inverseTopology.get(taskName + ":" + task_id + "@" + task_ip);
		else 
			return null;
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
	public HashMap<String, ArrayList<String>> getInitialActiveTopology(HashMap<String, ArrayList<String>> physicalTopology, 
			HashMap<String, ArrayList<String>> inverseTopology) {
		HashMap<String, ArrayList<String>> activeTopology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> activeTasks = new ArrayList<String>();
		HashMap<String, ArrayList<String>> layerTopology = produceTopologyLayers(physicalTopology, inverseTopology);
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
	public HashMap<String, ArrayList<String>> produceTopologyLayers(HashMap<String, ArrayList<String>> physicalTopology, 
			HashMap<String, ArrayList<String>> inverseTopology) {
		HashMap<String, ArrayList<String>> operatorLayers = new HashMap<String, ArrayList<String>>();
		Iterator<Entry<String, ArrayList<String>>> itr = physicalTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			String taskName = pair.getKey();
			ArrayList<String> childOperators = pair.getValue();
			ArrayList<String> parentOperators = inverseTopology.get(taskName);
			if(childOperators != null && childOperators.size() > 0 && parentOperators != null && parentOperators.size() > 0) {
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

}
