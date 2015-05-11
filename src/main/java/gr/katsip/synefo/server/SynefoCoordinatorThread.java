package gr.katsip.synefo.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import gr.katsip.cestorm.db.CEStormDatabaseManager;
import gr.katsip.synefo.server.time.TimeServer;
import gr.katsip.synefo.storm.api.Pair;

public class SynefoCoordinatorThread implements Runnable {

	private HashMap<String, ArrayList<String>> physicalTopology;

	private HashMap<String, ArrayList<String>> activeTopology;

	private HashMap<String, Integer> taskNameToIdMap;

	private Integer totalTaskNum = -1;

	private ZooMaster tamer;

	private HashMap<String, String> taskIPs;

	private HashMap<String, Pair<Number, Number>> resourceThresholds;

	private String zooHost;

	private Integer zooPort;

	private Thread userInterfaceThread;

	private AtomicBoolean operationFlag;
	
	private boolean demoMode;
	
	private AtomicInteger queryId;
	
	private CEStormDatabaseManager ceDb = null;

	public SynefoCoordinatorThread(String zooHost, Integer zooPort, 
			HashMap<String, Pair<Number, Number>> resourceThresholds, 
			HashMap<String, ArrayList<String>> physicalTopology, 
			HashMap<String, ArrayList<String>> runningTopology, 
			HashMap<String, Integer> taskNameToIdMap, 
			HashMap<String, String> taskIPs,
			AtomicBoolean operationFlag, 
			boolean demoMode, 
			AtomicInteger queryId, 
			CEStormDatabaseManager ceDb) {
		this.physicalTopology = physicalTopology;
		this.activeTopology = runningTopology;
		this.taskNameToIdMap = taskNameToIdMap;
		this.taskIPs = taskIPs;
		this.resourceThresholds = resourceThresholds;
		this.zooHost = zooHost;
		this.zooPort = zooPort;
		this.operationFlag = operationFlag;
		this.demoMode = demoMode;
		this.queryId = queryId;
		this.ceDb = ceDb;
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
		System.out.println("+efo coordinator thread: Received physical topology (size: " + 
				totalTaskNum + ").");
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
		tamer = new ZooMaster(zooHost, zooPort, physicalTopology, activeTopology);

		tamer.start();
		tamer.setScaleOutThresholds((double) resourceThresholds.get("cpu").upperBound, 
				(double) resourceThresholds.get("memory").upperBound, 
				(int) resourceThresholds.get("latency").upperBound, 
				(int) resourceThresholds.get("throughput").upperBound);
		tamer.setScaleInThresholds((double) resourceThresholds.get("cpu").lowerBound, 
				(double) resourceThresholds.get("memory").lowerBound, 
				(int) resourceThresholds.get("latency").lowerBound, 
				(int) resourceThresholds.get("throughput").lowerBound);

		System.out.println("+efo coordinator thread: received task name allocation from Storm cluster. Updating internal structures...");
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
					}
					updatedTopology.put(parentTask, downStreamIds);
				}else {
					updatedTopology.put(parentTask, new ArrayList<String>());
				}
			}
			activeUpdatedTopology = ScaleFunction.getInitialActiveTopology(updatedTopology, 
					ScaleFunction.getInverseTopology(updatedTopology));
			itr = activeUpdatedTopology.entrySet().iterator();
			System.out.println("Initial active topology:");
			while(itr.hasNext()) {
				Entry<String, ArrayList<String>> pair = itr.next();
				System.out.print(pair.getKey() + " -> {");
				for(String downTask : pair.getValue()) {
					System.out.print(downTask + " ");
				}
				System.out.println("}");
			}
			physicalTopology.clear();
			physicalTopology.putAll(updatedTopology);
			activeTopology.clear();
			activeTopology.putAll(activeUpdatedTopology);
			tamer.setPhysicalTopology();
			tamer.setActiveTopology();
			System.out.println("ZooMaster initial active topology: ");
			itr = (new HashMap<String, ArrayList<String>>(
					tamer.scaleFunction.getActiveTopology())).entrySet().iterator();
			while(itr.hasNext()) {
				Entry<String, ArrayList<String>> pair = itr.next();
				System.out.print(pair.getKey() + " -> {");
				for(String downTask : pair.getValue()) {
					System.out.print(downTask + " ");
				}
				System.out.println("}");
			}

			/**
			 * If demoMode is true: Need to populate the database with the 
			 * initial values.
			 * 
			 */
			if(demoMode == true) {
				/**
				 * First update all operator information
				 */
				Iterator<Entry<String, ArrayList<String>>> operatorItr = 
						physicalTopology.entrySet().iterator();
				while(operatorItr.hasNext()) {
					Entry<String, ArrayList<String>> operatorEntry = operatorItr.next();
					String operatorName = operatorEntry.getKey();
					String[] operatorNameTokens = operatorName.split(":@");
					this.ceDb.updateOperatorInformation(queryId.get(), operatorNameTokens[0], 
							operatorName, operatorNameTokens[2]);
				}
				this.ceDb.destroy();
			}
			operationFlag.set(true);

			taskNameToIdMap.clear();
			taskNameToIdMap.notifyAll();
		}
		tamer.setScaleOutEventWatch();
		tamer.setScaleInEventWatch();

		userInterfaceThread = new Thread(new SynEFOUserInterface(tamer, physicalTopology, demoMode, queryId, ceDb));
		userInterfaceThread.start();
		(new Thread(new TimeServer(5556))).start();
	}

}
