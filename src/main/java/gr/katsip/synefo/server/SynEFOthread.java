package gr.katsip.synefo.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import gr.katsip.synefo.storm.lib.SynEFOMessage;
import gr.katsip.synefo.storm.lib.Topology;


public class SynEFOthread implements Runnable {

	private InputStream _in;

	private OutputStream _out;

	private ObjectInputStream _input;

	private ObjectOutputStream _output;

	private Topology _physicalTopology;

	private Topology _runningTopology;

	private HashMap<String, Integer> _nameToIdMap;

	private Integer _taskId;

	private String _taskName;

	private String _componentIp;
	
	private HashMap<String, String> _task_ips;

	public SynEFOthread(Topology physicalTopology, Topology runningTopology, 
			HashMap<String, Integer> nameToIdMap, 
			InputStream in, OutputStream out,  
			HashMap<String, String> task_ips) {
		_in = in;
		_out = out;
		_nameToIdMap = nameToIdMap;
		_task_ips = task_ips;
		try {
			_output = new ObjectOutputStream(_out);
			_input = new ObjectInputStream(_in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		_physicalTopology = physicalTopology;
		_runningTopology = runningTopology;
	}

	public void run() {
		SynEFOMessage msg = null;
		System.out.println("+EFO worker: Accepted connection. Initiating handler thread...");
		try {
			msg = (SynEFOMessage) _input.readObject();
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		if(msg != null) {
			String _componentType = ((SynEFOMessage) msg)._values.get("TASK_TYPE");
			switch(_componentType) {
			case "SPOUT":
				spoutProcess(msg._values);
				break;
			case "BOLT":
				boltProcess(msg._values);
				break;
			case "TOPOLOGY":
				topologyProcess();
				break;
			default:
				System.err.println("+EFO worker: unrecognized connection (" +
						_componentType + "). Terminating operation...");
			}
		}
	}

	public void spoutProcess(HashMap<String, String> values) {
		_taskId = Integer.parseInt(values.get("TASK_ID"));
		_taskName = values.get("TASK_NAME");
		_componentIp = values.get("TASK_IP");
		synchronized(_task_ips) {
			_task_ips.put(_taskName + ":" + _taskId, _componentIp);
		}
		//_componentPort = Integer.parseInt(values.get("TASK_PORT"));
		synchronized(_nameToIdMap) {
			_nameToIdMap.put(_taskName, _taskId);
			_nameToIdMap.notifyAll();
		}
		System.out.println("+EFO worker-SPOUT: " + _taskName + "(" + _taskId + "@" + _componentIp + 
				") connected.");
		/**
		 * Wait until the Coordinator thread 
		 * updates the physicalTopology with 
		 * the Task IDs. This is done by 
		 * emptying the _nameToIdMap
		 */
		synchronized(_nameToIdMap) {
			while(_nameToIdMap.size() > 0) {
				try {
					_nameToIdMap.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		ArrayList<String> _downStream = null;
		ArrayList<String> _activeDownStream = null;
		if(_physicalTopology._topology.containsKey(_taskName + ":" + _taskId + "@" + _componentIp)) {
			_downStream = new ArrayList<String>(_physicalTopology._topology.get(_taskName + ":" + _taskId + "@" + _componentIp));
			_activeDownStream = new ArrayList<String>(_runningTopology._topology.get(_taskName + ":" + _taskId + "@" + _componentIp));
		}else {
			_downStream = new ArrayList<String>();
			_activeDownStream = new ArrayList<String>();
		}
		/**
		 * Send back the downstream topology info
		 */
		try {
			_output.writeObject(_downStream);
			_output.flush();
			_output.writeObject(_activeDownStream);
			_output.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println("SPOUT: " + _taskName + "(" + _taskId + "@" + _componentIp + 
				") registered successfully.");
		try {
			_output.flush();
			_output.close();
			_input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void boltProcess(HashMap<String, String> values) {
		_taskId = Integer.parseInt(values.get("TASK_ID"));
		_taskName = values.get("TASK_NAME");
		_componentIp = values.get("TASK_IP");
		synchronized(_task_ips) {
			_task_ips.put(_taskName + ":" + _taskId, _componentIp);
		}
		synchronized(_nameToIdMap) {
			_nameToIdMap.put(_taskName, _taskId);
			_nameToIdMap.notifyAll();
		}
		System.out.println("+EFO worker-BOLT: " + _taskName + "(" + _taskId + "@" + _componentIp + 
				") connected.");
		/**
		 * Wait until the Coordinator thread 
		 * updates the physicalTopology with 
		 * the Task IDs. This is done by 
		 * emptying the _nameToIdMap
		 */
		synchronized(_nameToIdMap) {
			while(_nameToIdMap.size() > 0) {
				try {
					_nameToIdMap.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		ArrayList<String> _downStream = null;
		ArrayList<String> _activeDownStream = null;
		if(_physicalTopology._topology.containsKey(_taskName + ":" + _taskId + "@" + _componentIp)) {
			_downStream = new ArrayList<String>(_physicalTopology._topology.get(_taskName + ":" + _taskId + "@" + _componentIp));
			_activeDownStream = new ArrayList<String>(_runningTopology._topology.get(_taskName + ":" + _taskId + "@" + _componentIp));
		}else {
			_downStream = new ArrayList<String>();
			_activeDownStream = new ArrayList<String>();
		}
		/**
		 * Send back the downstream topology info
		 */
		try {
			_output.writeObject(_downStream);
			_output.flush();
			_output.writeObject(_activeDownStream);
			_output.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		/**
		 * After coordination, keep listening 
		 * for received task statistics messages
		 */
		System.out.println("+EFO worker-BOLT: " + _taskName + "@" + _componentIp + 
				"(" + _taskId + ") registered successfully.");
		try {
			_output.flush();
			_output.close();
			_input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void topologyProcess() {
		Topology _topology = null;
		System.out.println("+EFO worker-TOPOLOGY: connected.");
		try {
			_topology = (Topology) _input.readObject();
		} catch (ClassNotFoundException | IOException e1) {
			e1.printStackTrace();
		}
		if(_topology._topology == null) {
			System.err.println("+EFO worker-TOPOLOGY: received empty topology object.");
			return;
		}
		System.out.println("+EFO worker-TOPOLOGY: received topology information.");
		synchronized(_physicalTopology) {
			if(_physicalTopology._topology.size() == 0 && _topology._topology != null) {
				_physicalTopology._topology = _topology._topology;
				_physicalTopology.notifyAll();
			}
		}
		String _ack = "+EFO_ACK";
		try {
			_output.writeObject(_ack);
			_output.flush();
			_output.close();
			_input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
