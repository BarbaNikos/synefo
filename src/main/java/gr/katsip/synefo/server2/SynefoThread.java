package gr.katsip.synefo.server2;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import gr.katsip.synefo.server2.JoinOperator.Step;
import gr.katsip.synefo.storm.lib.SynefoMessage;


public class SynefoThread implements Runnable {

	private InputStream in;

	private OutputStream out;

	private ObjectInputStream input;

	private ObjectOutputStream output;

	private ConcurrentHashMap<String, ArrayList<String>> physicalTopology;

	private ConcurrentHashMap<String, ArrayList<String>> activeTopology;

	private ConcurrentHashMap<String, Integer> taskIdentifierIndex;

	private Integer identifier;

	private String taskName;

	private String taskIP;

	private ConcurrentHashMap<String, String> taskAddressIndex;

	private AtomicBoolean operationFlag;

	private boolean demoMode;

	private AtomicInteger queryId;

	private AtomicInteger taskNumber = null;

	private ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation = null;

	private ConcurrentLinkedQueue<String> pendingAddressUpdates;

	public SynefoThread(ConcurrentHashMap<String, ArrayList<String>> physicalTopology, 
			ConcurrentHashMap<String, ArrayList<String>> activeTopology, 
			ConcurrentHashMap<String, Integer> taskIdentifierIndex, 
			InputStream in, OutputStream out, 
			ConcurrentHashMap<String, String> taskAddressIndex, 
			AtomicBoolean operationFlag, 
			boolean demoMode, 
			AtomicInteger queryId, 
			AtomicInteger taskNumber, 
			ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation, 
			ConcurrentLinkedQueue<String> pendingAddressUpdates) {
		this.in = in;
		this.out = out;
		this.taskIdentifierIndex = taskIdentifierIndex;
		this.taskAddressIndex = taskAddressIndex;
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
		this.taskToJoinRelation = taskToJoinRelation;
		this.pendingAddressUpdates = pendingAddressUpdates;
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
				boltProcess("BOLT", msg._values);
				break;
			case "JOIN_BOLT":
				boltProcess("JOIN_BOLT", msg._values);
				break;
			case "TOPOLOGY":
				if(demoMode)
					queryId.set(Integer.parseInt(msg._values.get("QUERY_ID")));
				topologyProcess(msg._values);
				break;
			default:
				System.err.println("+efo worker: unrecognized connection (" +
						_componentType + "). Terminating operation...");
			}
		}
	}

	public void spoutProcess(HashMap<String, String> values) {
		identifier = Integer.parseInt(values.get("TASK_ID"));
		taskName = values.get("TASK_NAME");
		taskIP = values.get("TASK_IP");
		/**
		 * This node has previously died so it is going to come back-up
		 */
		if(operationFlag.get() == true) {
			System.out.println("+EFO-SPOUT: " + taskName + "(" + identifier + "@" + taskIP + 
					") has RE-connected.");
			ArrayList<String> _downStream = null;
			ArrayList<String> _activeDownStream = null;
			/**
			 * Update internal structures with new ip (if it has changed)
			 * physical-topology (can be updated by synefo-thread)
			 * active-topology (should be updated by scale-function)
			 * task-address-index (can be updated by synefo-thread)
			 */
			if(taskAddressIndex.get(taskName + ":" + identifier).equals(taskIP) == false) {
				this.pendingAddressUpdates.offer(taskName + ":" + identifier + "@" + taskIP);
				while(taskAddressIndex.get(taskName + ":" + identifier).equals(taskIP) == false)
					try {
						Thread.sleep(300);
					} catch (InterruptedException e2) {
						e2.printStackTrace();
					}
			}
			if(physicalTopology.containsKey(taskName + ":" + identifier + "@" + taskIP)) {
				_downStream = new ArrayList<String>(physicalTopology.get(taskName + ":" + identifier + "@" + taskIP));
				if(activeTopology.containsKey(taskName + ":" + identifier + "@" + taskIP)) {
					System.out.println("+efo SPOUT: " + taskName + "(" + identifier + "@" + taskIP + 
							") retrieved active topology after RE-CONNECTION");
					_activeDownStream = new ArrayList<String>(activeTopology.get(taskName + ":" + identifier + "@" + taskIP));
				}
			}else {
				System.out.println("+efo SPOUT: " + taskName + "(" + identifier + "@" + taskIP + 
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
			System.out.println("+efo SPOUT: " + taskName + "@" + taskIP + 
					"(" + identifier + ") RE-CONNECTED successfully.");
			try {
				output.flush();
				output.close();
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return;
		}
		taskAddressIndex.putIfAbsent(taskName + ":" + identifier, taskIP);
		taskIdentifierIndex.putIfAbsent(taskName, identifier);
		System.out.println("+efo SPOUT: " + taskName + "(" + identifier + "@" + taskIP + 
				") connected.");
		/**
		 * Wait until the Coordinator thread 
		 * updates the physicalTopology with 
		 * the Task IDs. This is done by 
		 * emptying the _nameToIdMap
		 */
		while(taskIdentifierIndex.size() > 0)
			try {
				Thread.sleep(100);
			} catch (InterruptedException e2) {
				e2.printStackTrace();
			}
		ArrayList<String> _downStream = null;
		ArrayList<String> _activeDownStream = null;
		if(physicalTopology.containsKey(taskName + ":" + identifier + "@" + taskIP)) {
			_downStream = new ArrayList<String>(physicalTopology.get(taskName + ":" + identifier + "@" + taskIP));
			if(activeTopology.containsKey(taskName + ":" + identifier + "@" + taskIP)) {
				System.out.println("+efo SPOUT: " + taskName + "(" + identifier + "@" + taskIP + 
						") retrieving active topology");
				_activeDownStream = new ArrayList<String>(activeTopology.get(taskName + ":" + identifier + "@" + taskIP));
			}
			System.out.println("+efo SPOUT: " + taskName + "@" + taskIP + 
					"(" + identifier + ") downstream task list: " + _activeDownStream.toString());
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
		System.out.println("+efo SPOUT: " + taskName + "(" + identifier + "@" + taskIP + 
				") registered successfully.");
		try {
			output.flush();
			output.close();
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void boltProcess(String type, HashMap<String, String> values) {
		JoinOperator operator = null;
		if(type.equals("JOIN_BOLT")) {
			String joinStep = values.get("JOIN_STEP");
			String relation = values.get("JOIN_RELATION");
			identifier = Integer.parseInt(values.get("TASK_ID"));
			operator = new JoinOperator(identifier, 
					(joinStep.equals("DISPATCH") ? Step.DISPATCH : Step.JOIN), 
					relation);
			this.taskToJoinRelation.putIfAbsent(identifier, operator);
		}

		identifier = Integer.parseInt(values.get("TASK_ID"));
		taskName = values.get("TASK_NAME");
		taskIP = values.get("TASK_IP");
		/**
		 * This node has previously died so it is going to come back-up
		 */
		if(operationFlag.get() == true) {
			/**
			 * Update internal structures with new ip (if it has changed)
			 * physical-topology (can be updated by synefo-thread)
			 * active-topology (should be updated by scale-function)
			 * task-address-index (can be updated by synefo-thread)
			 */
			if(taskAddressIndex.get(taskName + ":" + identifier).equals(taskIP) == false) {
				this.pendingAddressUpdates.offer(taskName + ":" + identifier + "@" + taskIP);
				while(taskAddressIndex.get(taskName + ":" + identifier).equals(taskIP) == false)
					try {
						Thread.sleep(300);
					} catch (InterruptedException e2) {
						e2.printStackTrace();
					}
			}
			System.out.println("+efo BOLT: " + taskName + "(" + identifier + "@" + taskIP + 
					") has RE-connected.");
			HashMap<String, ArrayList<String>> relationTaskIndex = null;
			if(operator != null && operator.getStep() == JoinOperator.Step.DISPATCH) {
				relationTaskIndex = new HashMap<String, ArrayList<String>>();
				for(String task : physicalTopology.get(taskName + ":" + "@" + taskIP)) {
					Integer identifier = Integer.parseInt(task.split("[:@]")[1]);
					JoinOperator op = taskToJoinRelation.get(identifier);
					if(relationTaskIndex.containsKey(op.getRelation())) {
						ArrayList<String> ops = relationTaskIndex.get(op.getRelation());
						ops.add(task);
						relationTaskIndex.put(op.getRelation(), ops);
					}else {
						ArrayList<String> ops = new ArrayList<String>();
						ops.add(task);
						relationTaskIndex.put(op.getRelation(), ops);
					}
				}
			}
			ArrayList<String> _downStream = null;
			ArrayList<String> _activeDownStream = null;
			if(physicalTopology.containsKey(taskName + ":" + identifier + "@" + taskIP)) {
				_downStream = new ArrayList<String>(physicalTopology.get(taskName + ":" + identifier + "@" + taskIP));
				if(activeTopology.containsKey(taskName + ":" + identifier + "@" + taskIP)) {
					System.out.println("+efo BOLT: " + taskName + "(" + identifier + "@" + taskIP + 
							") retrieved active topology record after RE-CONNECTION");
					_activeDownStream = new ArrayList<String>(activeTopology.get(taskName + ":" + identifier + "@" + taskIP));
				}else { 
					System.out.println("+efo BOLT: " + taskName + "(" + identifier + "@" + taskIP + 
							") no active-topology record has been found for RE-CONNECTED bolt.");
					_activeDownStream = new ArrayList<String>();
					for(String task : _downStream) {
						if(activeTopology.containsKey(task))
							_activeDownStream.add(task);
					}
				}
			}else {
				System.out.println("+efo BOLT: " + taskName + "(" + identifier + "@" + taskIP + 
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
			 * In case of a JOIN (Dispatcher) operator, we need to send the 
			 * information of the downstream operators.
			 */
			if(operator != null && operator.getStep() == JoinOperator.Step.DISPATCH) {
				try {
					output.writeObject(relationTaskIndex);
					output.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			System.out.println("+efo BOLT: " + taskName + "@" + taskIP + 
					"(" + identifier + ") RE-CONNECTED successfully.");
			try {
				output.flush();
				output.close();
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return;
		}
		taskAddressIndex.putIfAbsent(taskName + ":" + identifier, taskIP);
		taskIdentifierIndex.putIfAbsent(taskName, identifier);
		System.out.println("+efo BOLT: " + taskName + "(" + identifier + "@" + taskIP + 
				") connected.");
		/**
		 * Wait until the Coordinator thread 
		 * updates the physicalTopology with 
		 * the Task IDs. This is done by 
		 * emptying the _nameToIdMap
		 */
		while(taskIdentifierIndex.size() > 0)
			try {
				Thread.sleep(100);
			} catch (InterruptedException e2) {
				e2.printStackTrace();
			}
		ArrayList<String> _downStream = null;
		ArrayList<String> _activeDownStream = null;
		/**
		 * In case the operator is a JOIN (DISPATCH) we need to populate 
		 * the information of its two downstream join operators
		 */
		HashMap<String, ArrayList<String>> relationTaskIndex = null;
		if(operator != null && operator.getStep() == JoinOperator.Step.DISPATCH) {
			relationTaskIndex = new HashMap<String, ArrayList<String>>();
			for(String task : physicalTopology.get(taskName + ":" + identifier + "@" + taskIP)) {
				Integer identifier = Integer.parseInt(task.split("[:@]")[1]);
				JoinOperator op = taskToJoinRelation.get(identifier);
				if(relationTaskIndex.containsKey(op.getRelation())) {
					ArrayList<String> ops = relationTaskIndex.get(op.getRelation());
					ops.add(task);
					relationTaskIndex.put(op.getRelation(), ops);
				}else {
					ArrayList<String> ops = new ArrayList<String>();
					ops.add(task);
					relationTaskIndex.put(op.getRelation(), ops);
				}
			}
		}
		if(physicalTopology.containsKey(taskName + ":" + identifier + "@" + taskIP)) {
			_downStream = new ArrayList<String>(physicalTopology.get(taskName + ":" + identifier + "@" + taskIP));
			if(activeTopology.containsKey(taskName + ":" + identifier + "@" + taskIP)) {
				System.out.println("+efo BOLT: " + taskName + "(" + identifier + "@" + taskIP + 
						") retrieving active topology.");
				_activeDownStream = new ArrayList<String>(activeTopology.get(taskName + ":" + identifier + "@" + taskIP));
			}else {
				_activeDownStream = new ArrayList<String>();
				for(String task : _downStream) {
					if(activeTopology.containsKey(task)) {
						_activeDownStream.add(task);
					}
				}
			}
			System.out.println("+efo BOLT: " + taskName + "@" + taskIP + 
					"(" + identifier + ") downstream task list: " + _activeDownStream.toString());
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
		 * In case of a JOIN (Dispatcher) operator, we need to send the 
		 * information of the downstream operators.
		 */
		if(operator != null && operator.getStep() == JoinOperator.Step.DISPATCH) {
			try {
				output.writeObject(relationTaskIndex);
				output.flush();
				String response = (String) input.readObject();
				if(!response.equals("OK"))
					System.out.println("+efo BOLT: " + taskName + "@" + taskIP + " sent back a N-ACK.");
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		/**
		 * After coordination, keep listening 
		 * for received task statistics messages
		 */
		System.out.println("+efo BOLT: " + taskName + "@" + taskIP + 
				"(" + identifier + ") registered successfully.");
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
		physicalTopology.clear();
		physicalTopology.putAll(topology);
		Integer providedTaskNumber = Integer.parseInt(values.get("TASK_NUM"));
		this.taskNumber.compareAndSet(-1, providedTaskNumber);
		System.out.println("+efo worker-TOPOLOGY: received topology information.");
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
