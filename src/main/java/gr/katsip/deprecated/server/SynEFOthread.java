package gr.katsip.deprecated.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import gr.katsip.synefo.utils.SynefoMessage;

/**
 * @deprecated
 */
public class SynEFOthread implements Runnable {

	private InputStream in;

	private OutputStream out;

	private ObjectInputStream input;

	private ObjectOutputStream output;

	private HashMap<String, ArrayList<String>> physicalTopology;

	private HashMap<String, ArrayList<String>> activeTopology;

	private HashMap<String, Integer> taskNameToIdMap;

	private Integer taskId;

	private String taskName;

	private String taskIP;

	private HashMap<String, String> taskIPs;
	
	private AtomicBoolean operationFlag;
	
	private boolean demoMode;
	
	private AtomicInteger queryId;
	
	private AtomicInteger taskNumber = null;

	public SynEFOthread(HashMap<String, ArrayList<String>> physicalTopology, 
			HashMap<String, ArrayList<String>> activeTopology, 
			HashMap<String, Integer> taskNameToIdMap, 
			InputStream in, OutputStream out,  
			HashMap<String, String> taskIPs, 
			AtomicBoolean operationFlag, 
			boolean demoMode, 
			AtomicInteger queryId, 
			AtomicInteger taskNumber) {
		this.in = in;
		this.out = out;
		this.taskNameToIdMap = taskNameToIdMap;
		this.taskIPs = taskIPs;
		try {
			output = new ObjectOutputStream(this.out);
			input = new ObjectInputStream(this.in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.physicalTopology = physicalTopology;
		this.activeTopology = activeTopology;
		this.operationFlag = operationFlag;
		this.demoMode = demoMode;
		this.queryId = queryId;
		this.taskNumber = taskNumber;
	}

	public void run() {
		SynefoMessage msg = null;
		System.out.println("+efo worker: Accepted connection. Initiating handler thread...");
		try {
			msg = (SynefoMessage) input.readObject();
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		if(msg != null) {
			String _componentType = ((SynefoMessage) msg)._values.get("TASK_TYPE");
			switch(_componentType) {
			case "SPOUT":
				spoutProcess(msg._values);
				break;
			case "BOLT":
				boltProcess(msg._values);
				break;
			case "TOPOLOGY":
				if(demoMode)
					queryId.set(Integer.parseInt(msg._values.get("QUERY_ID")));
				topologyProcess(msg._values);
				break;
			case "TIME":
				timeProcess();
				break;
			default:
				System.err.println("+efo worker: unrecognized connection (" +
						_componentType + "). Terminating operation...");
			}
		}
	}
	
	public void timeProcess() {
		Long currentTimestamp = new Long(System.currentTimeMillis());
		try {
			output.writeObject(currentTimestamp);
			output.flush();
			input.close();
			output.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void spoutProcess(HashMap<String, String> values) {
		taskId = Integer.parseInt(values.get("TASK_ID"));
		taskName = values.get("TASK_NAME");
		taskIP = values.get("TASK_IP");
		/**
		 * This node has previously died so it is going to come back-up
		 */
		if(operationFlag.get() == true) {
			System.out.println("+EFO-SPOUT: " + taskName + "(" + taskId + "@" + taskIP + 
					") has RE-connected.");
			ArrayList<String> _downStream = null;
			ArrayList<String> _activeDownStream = null;
			if(physicalTopology.containsKey(taskName + ":" + taskId + "@" + taskIP)) {
				_downStream = new ArrayList<String>(physicalTopology.get(taskName + ":" + taskId + "@" + taskIP));
				if(activeTopology.containsKey(taskName + ":" + taskId + "@" + taskIP)) {
					System.out.println("+efo SPOUT: " + taskName + "(" + taskId + "@" + taskIP + 
							") retrieved active topology after RE-CONNECTION");
					_activeDownStream = new ArrayList<String>(activeTopology.get(taskName + ":" + taskId + "@" + taskIP));
				}
			}else {
				System.out.println("+efo SPOUT: " + taskName + "(" + taskId + "@" + taskIP + 
						") no physical-topology record has been found for RE-CONNECTED bolt.");
			}
			/**
			 * Send back the downstream topology info
			 */
			try {
				output.writeObject(_downStream);
				output.flush();
				output.writeObject(_activeDownStream);
				output.flush();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			/**
			 * After coordination, keep listening 
			 * for received task statistics messages
			 */
			System.out.println("+efo SPOUT: " + taskName + "@" + taskIP + 
					"(" + taskId + ") RE-CONNECTED successfully.");
			try {
				output.flush();
				output.close();
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return;
		}
		synchronized(taskIPs) {
			taskIPs.put(taskName + ":" + taskId, taskIP);
		}
		synchronized(taskNameToIdMap) {
			if(taskNameToIdMap.containsKey(taskName) == false) {
				taskNameToIdMap.put(taskName, taskId);
			}
			taskNameToIdMap.notifyAll();
		}
		System.out.println("+efo SPOUT: " + taskName + "(" + taskId + "@" + taskIP + 
				") connected.");
		/**
		 * Wait until the Coordinator thread 
		 * updates the physicalTopology with 
		 * the Task IDs. This is done by 
		 * emptying the _nameToIdMap
		 */
		synchronized(taskNameToIdMap) {
			while(taskNameToIdMap.size() > 0) {
				try {
					taskNameToIdMap.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		ArrayList<String> _downStream = null;
		ArrayList<String> _activeDownStream = null;
		if(physicalTopology.containsKey(taskName + ":" + taskId + "@" + taskIP)) {
			_downStream = new ArrayList<String>(physicalTopology.get(taskName + ":" + taskId + "@" + taskIP));
			if(activeTopology.containsKey(taskName + ":" + taskId + "@" + taskIP)) {
				System.out.println("+efo SPOUT: " + taskName + "(" + taskId + "@" + taskIP + 
						") retrieving active topology");
				_activeDownStream = new ArrayList<String>(activeTopology.get(taskName + ":" + taskId + "@" + taskIP));
			}
			System.out.println("+efo SPOUT: " + taskName + "@" + taskIP + 
					"(" + taskId + ") downstream task list: " + _activeDownStream.toString());
		}else {
			_downStream = new ArrayList<String>();
			_activeDownStream = new ArrayList<String>();
		}
		/**
		 * Send back the downstream topology info
		 */
		try {
			output.writeObject(_downStream);
			output.flush();
			output.writeObject(_activeDownStream);
			output.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println("+efo SPOUT: " + taskName + "(" + taskId + "@" + taskIP + 
				") registered successfully.");
		try {
			output.flush();
			output.close();
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void boltProcess(HashMap<String, String> values) {
		taskId = Integer.parseInt(values.get("TASK_ID"));
		taskName = values.get("TASK_NAME");
		taskIP = values.get("TASK_IP");
		/**
		 * This node has previously died so it is going to come back-up
		 */
		if(operationFlag.get() == true) {
			System.out.println("+efo BOLT: " + taskName + "(" + taskId + "@" + taskIP + 
					") has RE-connected.");
			ArrayList<String> _downStream = null;
			ArrayList<String> _activeDownStream = null;
			if(physicalTopology.containsKey(taskName + ":" + taskId + "@" + taskIP)) {
				_downStream = new ArrayList<String>(physicalTopology.get(taskName + ":" + taskId + "@" + taskIP));
				if(activeTopology.containsKey(taskName + ":" + taskId + "@" + taskIP)) {
					System.out.println("+efo BOLT: " + taskName + "(" + taskId + "@" + taskIP + 
							") retrieved active topology record after RE-CONNECTION");
					_activeDownStream = new ArrayList<String>(activeTopology.get(taskName + ":" + taskId + "@" + taskIP));
				}else { 
					System.out.println("+efo BOLT: " + taskName + "(" + taskId + "@" + taskIP + 
							") no active-topology record has been found for RE-CONNECTED bolt.");
					_activeDownStream = new ArrayList<String>();
					for(String task : _downStream) {
						if(activeTopology.containsKey(task))
							_activeDownStream.add(task);
					}
				}
			}else {
				System.out.println("+efo BOLT: " + taskName + "(" + taskId + "@" + taskIP + 
						") no physical-topology record has been found for RE-CONNECTED bolt.");
			}
			/**
			 * Send back the downstream topology info
			 */
			try {
				output.writeObject(_downStream);
				output.flush();
				output.writeObject(_activeDownStream);
				output.flush();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			/**
			 * After coordination, keep listening 
			 * for received task statistics messages
			 */
			System.out.println("+efo BOLT: " + taskName + "@" + taskIP + 
					"(" + taskId + ") RE-CONNECTED successfully.");
			try {
				output.flush();
				output.close();
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return;
		}
		synchronized(taskIPs) {
			taskIPs.put(taskName + ":" + taskId, taskIP);
		}
		synchronized(taskNameToIdMap) {
			if(taskNameToIdMap.containsKey(taskName) == false) {
				taskNameToIdMap.put(taskName, taskId);
			}
			taskNameToIdMap.notifyAll();
		}
		System.out.println("+efo BOLT: " + taskName + "(" + taskId + "@" + taskIP + 
				") connected.");
		/**
		 * Wait until the Coordinator thread 
		 * updates the physicalTopology with 
		 * the Task IDs. This is done by 
		 * emptying the _nameToIdMap
		 */
		synchronized(taskNameToIdMap) {
			while(taskNameToIdMap.size() > 0) {
				try {
					taskNameToIdMap.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		ArrayList<String> _downStream = null;
		ArrayList<String> _activeDownStream = null;
		if(physicalTopology.containsKey(taskName + ":" + taskId + "@" + taskIP)) {
			_downStream = new ArrayList<String>(physicalTopology.get(taskName + ":" + taskId + "@" + taskIP));
			if(activeTopology.containsKey(taskName + ":" + taskId + "@" + taskIP)) {
				System.out.println("+efo BOLT: " + taskName + "(" + taskId + "@" + taskIP + 
						") retrieving active topology.");
				_activeDownStream = new ArrayList<String>(activeTopology.get(taskName + ":" + taskId + "@" + taskIP));
			}else { 
				_activeDownStream = new ArrayList<String>();
				for(String task : _downStream) {
					if(activeTopology.containsKey(task)) {
						_activeDownStream.add(task);
					}
				}
			}
			System.out.println("+efo BOLT: " + taskName + "@" + taskIP + 
					"(" + taskId + ") downstream task list: " + _activeDownStream.toString());
		}else {
			_downStream = new ArrayList<String>();
			_activeDownStream = new ArrayList<String>();
		}
		/**
		 * Send back the downstream topology info
		 */
		try {
			output.writeObject(_downStream);
			output.flush();
			output.writeObject(_activeDownStream);
			output.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		/**
		 * After coordination, keep listening 
		 * for received task statistics messages
		 */
		System.out.println("+efo BOLT: " + taskName + "@" + taskIP + 
				"(" + taskId + ") registered successfully.");
		try {
			output.flush();
			output.close();
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public void topologyProcess(HashMap<String, String> values) {
		/**
		 * This is the total number of tasks (threads) that will be spawned in the 
		 * given topology. It is set atomically by the topology-process thread.
		 */
		Integer providedTaskNumber = Integer.parseInt(values.get("TASK_NUM"));
		this.taskNumber.compareAndSet(-1, providedTaskNumber);
		HashMap<String, ArrayList<String>> topology = null;
		System.out.println("+efo worker-TOPOLOGY: connected.");
		try {
			topology = (HashMap<String, ArrayList<String>>) input.readObject();
		} catch (ClassNotFoundException | IOException e1) {
			e1.printStackTrace();
		}
		if(topology == null) {
			System.err.println("+efo worker-TOPOLOGY: received empty topology object.");
			return;
		}
		System.out.println("+efo worker-TOPOLOGY: received topology information.");
		synchronized(physicalTopology) {
			if(physicalTopology.size() == 0 && topology != null) {
				physicalTopology.clear();
				physicalTopology.putAll(topology);
				physicalTopology.notifyAll();
			}
		}
		String _ack = "+EFO_ACK";
		try {
			output.writeObject(_ack);
			output.flush();
			output.close();
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
