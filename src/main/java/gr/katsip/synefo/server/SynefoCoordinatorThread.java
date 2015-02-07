package gr.katsip.synefo.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
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

	Thread userInterfaceThread;

	public SynefoCoordinatorThread(String zooHost, Integer zooPort, HashMap<String, Pair<Number, Number>> resourceThresholds, 
			HashMap<String, ArrayList<String>> physicalTopology, HashMap<String, ArrayList<String>> runningTopology, 
			HashMap<String, Integer> taskNameToIdMap, 
			HashMap<String, String> taskIPs) {
		this.physicalTopology = physicalTopology;
		this.activeTopology = runningTopology;
		this.taskNameToIdMap = taskNameToIdMap;
		this.taskIPs = taskIPs;
		this.resourceThresholds = resourceThresholds;
		this.zooHost = zooHost;
		this.zooPort = zooPort;
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
		tamer = new ZooMaster(zooHost, zooPort, new ScaleFunction(physicalTopology, activeTopology));

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
				if(downStreamNames != null && downStreamNames.size() > 0) {
					ArrayList<String> downStreamIds = new ArrayList<String>();
					ArrayList<String> activeDownStreamIds = new ArrayList<String>();
					for(String name : downStreamNames) {
						downStreamIds.add(name + ":" + Integer.toString(taskNameToIdMap.get(name)) + "@" + 
								taskIPs.get(name + ":" + Integer.toString(taskNameToIdMap.get(name))));
						if(activeDownStreamIds.size() == 0) {
							activeDownStreamIds.add(name + ":" + Integer.toString(taskNameToIdMap.get(name)) + "@" + 
									taskIPs.get(name + ":" + Integer.toString(taskNameToIdMap.get(name))));
						}
					}
					updatedTopology.put(taskName + ":" + Integer.toString(taskNameToIdMap.get(taskName)) + "@" + 
							taskIPs.get(taskName + ":" + Integer.toString(taskNameToIdMap.get(taskName))), downStreamIds);
					activeUpdatedTopology.put(taskName + ":" + Integer.toString(taskNameToIdMap.get(taskName)) + "@" + 
							taskIPs.get(taskName + ":" + Integer.toString(taskNameToIdMap.get(taskName))), activeDownStreamIds);
				}
			}
			physicalTopology.clear();
			physicalTopology.putAll(updatedTopology);
			activeTopology.clear();
			activeTopology.putAll(activeUpdatedTopology);
			tamer.setPhysicalTopology();
			tamer.setActiveTopology();

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
		return physicalTopology.get(taskName + ":" + task_id + "@" + task_ip);
	}

}
