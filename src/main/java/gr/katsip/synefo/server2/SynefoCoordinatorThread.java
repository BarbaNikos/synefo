package gr.katsip.synefo.server2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import gr.katsip.cestorm.db.CEStormDatabaseManager;
import gr.katsip.synefo.storm.api.Pair;

public class SynefoCoordinatorThread implements Runnable {

	private ConcurrentHashMap<String, ArrayList<String>> physicalTopology;

	private ConcurrentHashMap<String, ArrayList<String>> activeTopology;

	private ConcurrentHashMap<String, Integer> taskIdentifierIndex;

	private ZooMaster tamer;

	private ConcurrentHashMap<String, String> taskAddressIndex;
	
	private ConcurrentHashMap<String, Integer> taskWorkerPortIndex;

	private HashMap<String, Pair<Number, Number>> resourceThresholds;

	private String zooHost;

	private Thread userInterfaceThread;

	private AtomicBoolean operationFlag;

	private boolean demoMode;

	private AtomicInteger queryId;

	private CEStormDatabaseManager ceDb = null;

	private AtomicInteger taskNumber = null;

	private ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation = null;
	
	private ConcurrentLinkedQueue<String> pendingAddressUpdates;

	public SynefoCoordinatorThread(String zooHost, 
			HashMap<String, Pair<Number, Number>> resourceThresholds, 
			ConcurrentHashMap<String, ArrayList<String>> physicalTopology, 
			ConcurrentHashMap<String, ArrayList<String>> runningTopology, 
			ConcurrentHashMap<String, Integer> taskNameToIdMap, 
			ConcurrentHashMap<String, Integer> taskWorkerPortIndex, 
			ConcurrentHashMap<String, String> taskIPs,
			AtomicBoolean operationFlag, 
			boolean demoMode, 
			AtomicInteger queryId, 
			CEStormDatabaseManager ceDb, 
			AtomicInteger taskNumber, 
			ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation, 
			ConcurrentLinkedQueue<String> pendingAddressUpdates) {
		this.physicalTopology = physicalTopology;
		this.activeTopology = runningTopology;
		this.taskIdentifierIndex = taskNameToIdMap;
		this.taskWorkerPortIndex = taskWorkerPortIndex;
		this.taskAddressIndex = taskIPs;
		this.resourceThresholds = resourceThresholds;
		this.zooHost = zooHost;
		this.operationFlag = operationFlag;
		this.demoMode = demoMode;
		this.queryId = queryId;
		this.ceDb = ceDb;
		this.taskNumber = taskNumber;
		this.taskToJoinRelation = taskToJoinRelation;
		this.pendingAddressUpdates = pendingAddressUpdates;
	}

	public void run() {
		System.out.println("+efo coordinator thread: initiates execution...");
		while(this.taskNumber.get() == -1) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("+efo coordinator thread: Received physical topology (size: " + 
				taskNumber.get() + ").");
		while(taskIdentifierIndex.size() < taskNumber.get())
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
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
		tamer = new ZooMaster(zooHost, physicalTopology, activeTopology, taskToJoinRelation);
		tamer.start();
		tamer.setScaleOutThresholds((double) resourceThresholds.get("cpu").upperBound, 
				(double) resourceThresholds.get("memory").upperBound, 
				(int) resourceThresholds.get("latency").upperBound, 
				(int) resourceThresholds.get("throughput").upperBound);
		tamer.setScaleInThresholds((double) resourceThresholds.get("cpu").lowerBound, 
				(double) resourceThresholds.get("memory").lowerBound, 
				(int) resourceThresholds.get("latency").lowerBound, 
				(int) resourceThresholds.get("throughput").lowerBound);

		System.out.println("initial phys-top: " + physicalTopology.toString());
		ConcurrentHashMap<String, ArrayList<String>> expandedPhysicalTopology = physicalTopologyTaskExpand(taskIdentifierIndex, physicalTopology);
		System.out.println("expanded-phys-top: " + expandedPhysicalTopology.toString());
		physicalTopology.clear();
		physicalTopology.putAll(expandedPhysicalTopology);
		/**
		 * At this point, physicalTopologyWithIds has the actual topology of operators and the task-ids.
		 */
		System.out.println("+efo coordinator thread: received task name allocation from Storm cluster. Updating internal structures...");
		ConcurrentHashMap<String, ArrayList<String>> updatedPhysicalTopology = SynefoCoordinatorThread.updatePhysicalTopology(
				taskAddressIndex, taskIdentifierIndex, physicalTopology);
		physicalTopology.clear();
		physicalTopology.putAll(updatedPhysicalTopology);
		System.out.println("updated-phys-top: " + physicalTopology.toString());
		activeTopology.clear();
		/**
		 * Set the following flag to true if you need the minimal 
		 * active topology to start.
		 */
		activeTopology.putAll(SynefoCoordinatorThread.getInitialActiveTopologyWithJoinOperators(
				physicalTopology, 
				ScaleFunction.getInverseTopology(physicalTopology), 
				taskToJoinRelation, false));
		
		tamer.setPhysicalTopology();
		tamer.setActiveTopology();
		
		operationFlag.set(true);
		taskIdentifierIndex.clear();
		tamer.setScaleOutEventWatch();
		tamer.setScaleInEventWatch();

		userInterfaceThread = new Thread(new SynEFOUserInterface(tamer, physicalTopology, demoMode, queryId, ceDb));
		userInterfaceThread.start();
		/**
		 * Fault-tolerance mechanism for when an executor dies 
		 * and respawns in a different machine
		 */
		while(operationFlag.get()) {
			while(this.pendingAddressUpdates.isEmpty()) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			/**
			 * Update address in ScaleFunction for active and physical topology
			 */
			String addressChange = pendingAddressUpdates.poll();
			String taskName = addressChange.split("[:@]")[0];
			Integer identifier = Integer.parseInt(addressChange.split("[:@]")[1]);
			String newAddress = addressChange.split("[:@]")[2];
			tamer.scaleFunction.updateTaskAddress(taskName, identifier, newAddress, taskAddressIndex.get(taskName + ":" + identifier));
			taskAddressIndex.remove(taskName + ":" + identifier);
			taskAddressIndex.put(taskName + ":" + identifier, newAddress);
		}
	}
	
	public static ConcurrentHashMap<String, ArrayList<String>> updatePhysicalTopology(ConcurrentHashMap<String, String> taskAddressIndex, 
			ConcurrentHashMap<String, Integer> taskIdentifierIndex, 
			ConcurrentHashMap<String, ArrayList<String>> physicalTopology) {
		ConcurrentHashMap<String, ArrayList<String>> updatedTopology = new ConcurrentHashMap<String, ArrayList<String>>();
		Iterator<Entry<String, ArrayList<String>>> itr = physicalTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Map.Entry<String, ArrayList<String>> pair = itr.next();
			String taskName = pair.getKey();
			ArrayList<String> downStreamNames = pair.getValue();
			String parentTask = taskName + ":" + Integer.toString(taskIdentifierIndex.get(taskName)) + "@" + 
					taskAddressIndex.get(taskName + ":" + Integer.toString(taskIdentifierIndex.get(taskName)));
			if(downStreamNames != null && downStreamNames.size() > 0) {
				ArrayList<String> downStreamIds = new ArrayList<String>();
				for(String name : downStreamNames) {
					if(taskIdentifierIndex.containsKey(name) == false) {
						assert taskIdentifierIndex.containsKey(name) == true;
					}
					String childTask = name + ":" + Integer.toString(taskIdentifierIndex.get(name)) + "@" + 
							taskAddressIndex.get(name + ":" + Integer.toString(taskIdentifierIndex.get(name)));
					downStreamIds.add(childTask);
				}
				updatedTopology.put(parentTask, downStreamIds);
			}else {
				updatedTopology.put(parentTask, new ArrayList<String>());
			}
		}
		return updatedTopology;
	}

	public static ConcurrentHashMap<String, ArrayList<String>> physicalTopologyTaskExpand(ConcurrentHashMap<String, Integer> taskNameToIdMap, 
			ConcurrentHashMap<String, ArrayList<String>> physicalTopology) {
		ConcurrentHashMap<String, ArrayList<String>> physicalTopologyWithIds = new ConcurrentHashMap<String, ArrayList<String>>();
		Iterator<Entry<String, Integer>> taskNameIterator = taskNameToIdMap.entrySet().iterator();
		while(taskNameIterator.hasNext()) {
			Entry<String, Integer> pair = taskNameIterator.next();
			String taskName = pair.getKey();
			String taskNameWithoutId = taskName.split("_")[0];
			ArrayList<String> downstreamTaskList = physicalTopology.get(taskNameWithoutId);
			ArrayList<String> downstreamTaskWithIdList = new ArrayList<String>();
			for(String downstreamTask : downstreamTaskList) {
				Iterator<Entry<String, Integer>> downstreamTaskIterator = taskNameToIdMap.entrySet().iterator();
				while(downstreamTaskIterator.hasNext()) {
					Entry<String, Integer> downstreamPair = downstreamTaskIterator.next();
					if(downstreamPair.getKey().split("_")[0].equals(downstreamTask)) {
						downstreamTaskWithIdList.add(downstreamPair.getKey());
					}
				}
			}
			physicalTopologyWithIds.put(taskName, downstreamTaskWithIdList);
		}
		return physicalTopologyWithIds;
	}

	public static ConcurrentHashMap<String, ArrayList<String>> getInitialActiveTopologyWithJoinOperators(
			ConcurrentHashMap<String, ArrayList<String>> physicalTopology, 
			ConcurrentHashMap<String, ArrayList<String>> inverseTopology, 
			ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation, boolean minimalFlag) {
		ConcurrentHashMap<String, ArrayList<String>> activeTopology = new ConcurrentHashMap<String, ArrayList<String>>();
		ArrayList<String> activeTasks = new ArrayList<String>();
		ConcurrentHashMap<String, ArrayList<String>> layerTopology = ScaleFunction.produceTopologyLayers(
				physicalTopology, inverseTopology);
		/**
		 * If minimal flag is set to false, then all existing nodes 
		 * will be set as active
		 */
		if(minimalFlag == false) {
			activeTopology.putAll(physicalTopology);
			return activeTopology;
		}
		/**
		 * Add all source operators first
		 */
		Iterator<Entry<String, ArrayList<String>>> itr = inverseTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			String taskName = pair.getKey();
			ArrayList<String> parentTasks = pair.getValue();
			if(parentTasks == null) {
				activeTasks.add(taskName);
			}else if(parentTasks != null && parentTasks.size() == 0) {
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
		 * From each operator layer (stage of computation) add one node.
		 * If the layer consists of JOIN operators, add one for each relation
		 */
		itr = layerTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			ArrayList<String> layerTasks = pair.getValue();
			/**
			 * Check if this is a layer of Join operators (dispatchers)
			 */
			Integer candidateTask = Integer.parseInt(layerTasks.get(0).split("[:@]")[1]);
			if(taskToJoinRelation.containsKey(candidateTask) && 
					taskToJoinRelation.get(candidateTask).getStep().equals(JoinOperator.Step.JOIN)) {
				String relation = taskToJoinRelation.get(candidateTask).getRelation();
				activeTasks.add(layerTasks.get(0));
				for(int i = 1; i < layerTasks.size(); i++) {
					Integer otherCandidateTask = Integer.parseInt(layerTasks.get(i).split("[:@]")[1]);
					if(taskToJoinRelation.containsKey(otherCandidateTask) && 
							taskToJoinRelation.get(otherCandidateTask).getStep().equals(JoinOperator.Step.JOIN) && 
							taskToJoinRelation.get(otherCandidateTask).getRelation().equals(relation) == false) {
						activeTasks.add(layerTasks.get(i));
						break;
					}
				}
			}else {
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
