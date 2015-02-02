package gr.katsip.synefo.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import gr.katsip.synefo.storm.api.Pair;
import gr.katsip.synefo.storm.lib.Topology;

public class SynEFOCoordinatorThread implements Runnable {

	private Topology _physicalTopology;

	private Topology _runningTopology;

	private HashMap<String, Integer> _nameToIdMap;

	private Integer _totalTaskNum = -1;

	private ZooMaster tamer;

	private HashMap<String, String> _task_ips;

	private HashMap<String, Pair<Number, Number>> resource_thresholds;

	private String zooHost;

	private Integer zooPort;

	public SynEFOCoordinatorThread(String zooHost, Integer zooPort, HashMap<String, Pair<Number, Number>> _resource_thresholds, Topology physicalTopology, Topology runningTopology, 
			HashMap<String, Integer> nameToIdMap, HashMap<String, String> task_ips) {
		_physicalTopology = physicalTopology;
		_runningTopology = runningTopology;
		_nameToIdMap = nameToIdMap;
		_task_ips = task_ips;
		resource_thresholds = _resource_thresholds;
		this.zooHost = zooHost;
		this.zooPort = zooPort;
	}

	public void run() {
		System.out.println("+EFO coordinator thread: initiates execution...");
		synchronized(_physicalTopology) {
			while(_physicalTopology._topology.size() == 0) {
				try {
					_physicalTopology.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			_totalTaskNum = _physicalTopology._topology.size();
		}
		System.out.println("+EFO coordinator thread: Received physical topology (size: " + _totalTaskNum + ").");
		synchronized(_nameToIdMap) {
			while(_nameToIdMap.size() < _totalTaskNum) {
				try {
					_nameToIdMap.wait();
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
		System.out.println("+EFO coordinator thread: received tast name allocation from Storm cluster. Updating internal structures...");
		Topology updatedTopology = new Topology();
		Topology activeUpdatedTopology = new Topology();
		synchronized(_nameToIdMap) {
			Iterator<Entry<String, ArrayList<String>>> itr = _physicalTopology._topology.entrySet().iterator();
			while(itr.hasNext()) {
				Map.Entry<String, ArrayList<String>> pair = itr.next();
				String taskName = pair.getKey();
				ArrayList<String> downStreamNames = pair.getValue();
				if(downStreamNames != null && downStreamNames.size() > 0) {
					ArrayList<String> downStreamIds = new ArrayList<String>();
					ArrayList<String> activeDownStreamIds = new ArrayList<String>();
					for(String name : downStreamNames) {
						downStreamIds.add(name + ":" + Integer.toString(_nameToIdMap.get(name)) + "@" + 
								_task_ips.get(name + ":" + Integer.toString(_nameToIdMap.get(name))));
						if(activeDownStreamIds.size() == 0) {
							activeDownStreamIds.add(name + ":" + Integer.toString(_nameToIdMap.get(name)) + "@" + 
									_task_ips.get(name + ":" + Integer.toString(_nameToIdMap.get(name))));
						}
					}
					updatedTopology._topology.put(taskName + ":" + Integer.toString(_nameToIdMap.get(taskName)) + "@" + 
							_task_ips.get(taskName + ":" + Integer.toString(_nameToIdMap.get(taskName))), downStreamIds);
					activeUpdatedTopology._topology.put(taskName + ":" + Integer.toString(_nameToIdMap.get(taskName)) + "@" + 
							_task_ips.get(taskName + ":" + Integer.toString(_nameToIdMap.get(taskName))), activeDownStreamIds);
				}
			}
			tamer = new ZooMaster(zooHost, zooPort, new ScaleFunction(updatedTopology._topology, activeUpdatedTopology._topology));
			/**
			 * Update ZooKeeper entries and Nodes
			 */
			tamer.start();
			tamer.set_scaleout_thresholds((double) resource_thresholds.get("cpu").upperBound, 
					(double) resource_thresholds.get("memory").upperBound, 
					(int) resource_thresholds.get("latency").upperBound, 
					(int) resource_thresholds.get("throughput").upperBound);
			tamer.set_scaleout_thresholds((double) resource_thresholds.get("cpu").lowerBound, 
					(double) resource_thresholds.get("memory").lowerBound, 
					(int) resource_thresholds.get("latency").lowerBound, 
					(int) resource_thresholds.get("throughput").lowerBound);
			tamer.setScaleOutEventWatch();
			tamer.setScaleInEventWatch();
			tamer.set_physical_top(_physicalTopology._topology);
			tamer.set_active_top(_runningTopology._topology);

			_physicalTopology._topology.clear();
			_physicalTopology._topology = new HashMap<String, ArrayList<String>>(updatedTopology._topology);
			_runningTopology._topology.clear();
			_runningTopology._topology = new HashMap<String, ArrayList<String>>(activeUpdatedTopology._topology);
			_nameToIdMap.clear();
			_nameToIdMap.notifyAll();
		}

		/**
		 * At this point Usage of tasks is monitored
		 */
		System.out.println("+EFO coordinator thread: concluded naming resolution.");
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		//		(new Thread(new SynEFOUserInterface(_physicalTopology, _runningTopology, _taskUsage, _control_map, _task_ips))).start();
	}

	public Topology getTopology() {
		return this._runningTopology;
	}

	public int getTaskId(String taskName) {
		Iterator<Entry<String, ArrayList<String>>> itr = _physicalTopology._topology.entrySet().iterator();
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
		return _physicalTopology._topology.get(taskName + ":" + task_id + "@" + task_ip);
	}

}
