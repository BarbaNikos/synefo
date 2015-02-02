package gr.katsip.synefo.storm.api;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import gr.katsip.synefo.metric.SynEFOMetric;
import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.lib.SynEFOMessage;
import gr.katsip.synefo.storm.lib.SynEFOMessage.Type;
import gr.katsip.synefo.storm.operators.AbstractTupleProducer;
import gr.katsip.synefo.utils.SynEFOConstant;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SynEFOSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7244170192535254357L;

	private String _task_name;

	private SpoutOutputCollector _collector;

	private int _task_id;
	
	private String _task_ip;

	private ArrayList<String> _downstream_tasks = null;

	private ArrayList<Integer> _int_downstream_tasks = null;

	private ArrayList<String> _active_downstream_tasks = null;

	private ArrayList<Integer> _int_active_downstream_tasks = null;

	private int idx;

	private String _synEFO_ip;

	private Integer _synEFO_port;

	private Socket socket;

	private ObjectOutputStream _output = null;

	private ObjectInputStream _input = null;

	private TaskStatistics _stats;

	private AbstractTupleProducer _tuple_producer;

	private long _tuple_counter;

	private SynEFOMetric metricObject;

	transient private ZooPet pet;

	public SynEFOSpout(String task_name, String synEFO_ip, Integer synEFO_port, AbstractTupleProducer tupleProducer) {
		_task_name = task_name;
		_downstream_tasks = null;
		_active_downstream_tasks = null;
		_synEFO_ip = synEFO_ip;
		_synEFO_port = synEFO_port;
		_stats = new TaskStatistics();
		_tuple_producer = tupleProducer;
		_tuple_counter = 0;
	}

	@SuppressWarnings("unchecked")
	public void registerToSynEFO() {
		socket = null;
		SynEFOMessage msg = new SynEFOMessage();
		msg._type = Type.REG;
		msg._values.put("TASK_TYPE", "SPOUT");
		msg._values.put("TASK_NAME", _task_name);
		try {
			_task_ip = InetAddress.getLocalHost().getHostAddress();
			msg._values.put("TASK_IP", _task_ip);
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		msg._values.put("TASK_ID", Integer.toString(_task_id));
		try {
			socket = new Socket(_synEFO_ip, _synEFO_port);
			_output = new ObjectOutputStream(socket.getOutputStream());
			_input = new ObjectInputStream(socket.getInputStream());
			_output.writeObject(msg);
			_output.flush();
			msg = null;
			System.out.println("SPOUT (" + _task_name + ") wait 1");
			ArrayList<String> _downstream = null;
			_downstream = (ArrayList<String>) _input.readObject();
			if(_downstream != null && _downstream.size() > 0) {
				_downstream_tasks = new ArrayList<String>(_downstream);
				_int_downstream_tasks = new ArrayList<Integer>();
				for(String task : _downstream_tasks) {
					StringTokenizer strTok = new StringTokenizer(task, ":");
					strTok.nextToken();
					String taskWithIp = strTok.nextToken();
					strTok = new StringTokenizer(taskWithIp, "@");
					_int_downstream_tasks.add(Integer.parseInt(strTok.nextToken()));
				}
			}else {
				_downstream_tasks = new ArrayList<String>();
				_int_downstream_tasks = new ArrayList<Integer>();
			}
			ArrayList<String> _active_downstream = null;
			_active_downstream = (ArrayList<String>) _input.readObject();
			if(_active_downstream != null && _active_downstream.size() > 0) {
				_active_downstream_tasks = new ArrayList<String>(_active_downstream);
				_int_active_downstream_tasks = new ArrayList<Integer>();
				for(String task : _active_downstream_tasks) {
					StringTokenizer strTok = new StringTokenizer(task, ":");
					strTok.nextToken();
					String taskWithIp = strTok.nextToken();
					strTok = new StringTokenizer(taskWithIp, "@");
					_int_active_downstream_tasks.add(Integer.parseInt(strTok.nextToken()));
				}
				idx = 0;
			}else {
				_active_downstream_tasks = new ArrayList<String>();
				_int_active_downstream_tasks = new ArrayList<Integer>();
				idx = 0;
			}
			/**
			 * Closing channels of communication with 
			 * SynEFO server
			 */
			_output.flush();
			_output.close();
			_input.close();
			socket.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		/**
		 * Handshake with ZooKeeper
		 */
		pet.start();

		System.out.println("+EFO-SPOUT (" + 
				_task_name + ":" + _task_id + 
				") registered to +EFO successfully.");
	}

	public void nextTuple() {
		/**
		 * In this execution names are sent in a round-robin 
		 * fashion to all downstream tasks. The words array is 
		 * iterated from start to beginning.
		 */
		if(_int_active_downstream_tasks != null && _int_active_downstream_tasks.size() > 0) {
			Values values = new Values();
			/**
			 * Add SYNEFO_HEADER in the beginning
			 */
			values.add(new String(""));
			values.addAll(_tuple_producer.nextTuple(_int_active_downstream_tasks.get(idx)));
			_collector.emitDirect(_int_active_downstream_tasks.get(idx), values);
			if(idx >= (_int_active_downstream_tasks.size() - 1)) {
				idx = 0;
			}else {
				idx += 1;
			}
		}
		_tuple_counter += 1;
		metricObject.updateMetrics(_tuple_counter);
		_stats.update_memory();
		_stats.update_cpu_load();
		_stats.update_latency();
		_stats.update_throughput(_tuple_counter);
		String scaleCommand = "";
		synchronized(pet) {
			if(pet.pendingCommand != null) {
				scaleCommand = pet.returnScaleCommand();
			}
		}
		if(scaleCommand != null && scaleCommand.length() > 0) {
			StringTokenizer strTok = new StringTokenizer(scaleCommand, "~");
			String action = strTok.nextToken();
			String taskWithIp = strTok.nextToken();
			strTok = new StringTokenizer(taskWithIp, "@");
			String taskWithId = strTok.nextToken();
			String taskIp = strTok.nextToken();
			strTok = new StringTokenizer(taskWithId, ":");
			String task = strTok.nextToken();
			Integer task_id = Integer.parseInt(strTok.nextToken());
			StringBuilder strBuild = new StringBuilder();
			strBuild.append(SynEFOConstant.PUNCT_TUPLE_TAG + "/");
			idx = 0;
			if(action.toLowerCase().contains("add")) {
				_active_downstream_tasks.add(task);
				_int_active_downstream_tasks.add(task_id);
				strBuild.append(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.ADD_ACTION + "/");
			}else if(action.toLowerCase().contains("remove")) {
				_active_downstream_tasks.remove(_active_downstream_tasks.indexOf(task));
				_int_active_downstream_tasks.remove(_int_active_downstream_tasks.indexOf(task_id));
				strBuild.append(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.REMOVE_ACTION + "/");
			}
			strBuild.append(SynEFOConstant.COMP_TAG + ":" + task + "/");
			strBuild.append(SynEFOConstant.COMP_NUM_TAG + ":" + _downstream_tasks.size() + "/");
			strBuild.append(SynEFOConstant.COMP_IP_TAG + ":" + taskIp + "/");
			for(Integer d_task : _int_downstream_tasks) {
				_collector.emitDirect(d_task, new Values(strBuild.toString()));
			}
		}
		/*
		_stat_tuple_counter += 1;
		if(_stat_tuple_counter == this._stat_report_timestamp) {
			try {
				_output.writeObject(_stats);
				_output.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
			SynEFOMessage command = null;
			try {
				command = (SynEFOMessage) _input.readObject();
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
			if(command != null && command._type.equals(SynEFOMessage.Type.SCLOUT)) {
				String newTask = null;
				String newTaskIp = null;
				if(command._values.containsKey("ACTION")) {
					if(command._values.get("ACTION").equals("ADD")) {
						if(command._values.containsKey("NEW_TASK")) {
							newTask = command._values.get("NEW_TASK");
							newTaskIp = command._values.get("NEW_TASK_IP");
							if(_downstream_tasks.lastIndexOf(newTask) != -1 && _active_downstream_tasks.lastIndexOf(newTask) == -1) {
								_active_downstream_tasks.add(newTask);
								StringTokenizer strTok = new StringTokenizer(newTask, ":");
								strTok.nextToken();
								_int_active_downstream_tasks.add(Integer.parseInt(strTok.nextToken()));
								idx = 0;
								// Populate values for punctuation tuple
								StringBuilder strBuild = new StringBuilder();
								strBuild.append(SynEFOConstant.PUNCT_TUPLE_TAG + "/");
								strBuild.append(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.ADD_ACTION + "/");
								strBuild.append(SynEFOConstant.COMP_TAG + ":" + newTask + "/");
								strBuild.append(SynEFOConstant.COMP_NUM_TAG + ":" + _downstream_tasks.size() + "/");
								strBuild.append(SynEFOConstant.COMP_IP_TAG + ":" + newTaskIp + "/");
								for(Integer d_task : _int_downstream_tasks) {
									_collector.emitDirect(d_task, new Values(strBuild.toString()));
								}
							}
						}
					}else if(command._values.get("ACTION").equals("REMOVE")) {
						if(command._values.containsKey("NEW_TASK")) {
							newTask = command._values.get("NEW_TASK");
							newTaskIp = command._values.get("NEW_TASK_IP");
							if(_downstream_tasks.lastIndexOf(newTask) != -1 && _active_downstream_tasks.lastIndexOf(newTask) != -1) {
								_active_downstream_tasks.remove(_active_downstream_tasks.indexOf(newTask));
								StringTokenizer strTok = new StringTokenizer(newTask, ":");
								strTok.nextToken();
								_int_active_downstream_tasks.remove(_int_active_downstream_tasks.indexOf(Integer.parseInt(strTok.nextToken())));
								idx = 0;
								// Populate values for punctuation tuple
								StringBuilder strBuild = new StringBuilder();
								strBuild.append(SynEFOConstant.PUNCT_TUPLE_TAG + "/");
								strBuild.append(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.REMOVE_ACTION + "/");
								strBuild.append(SynEFOConstant.COMP_TAG + ":" + newTask + "/");
								strBuild.append(SynEFOConstant.COMP_NUM_TAG + ":" + _downstream_tasks.size() + "/");
								strBuild.append(SynEFOConstant.COMP_IP_TAG + ":" + newTaskIp + "/");
								for(Integer d_task : _int_downstream_tasks) {
									_collector.emitDirect(d_task, new Values(strBuild.toString()));
								}
							}
						}
					}else {
						System.out.println("+EFO-SPOUT: Received a SCLOUT command with unrecognizable ACTION (" + 
								command._values.get("ACTION") + ")");
					}
				}else {
					System.out.println("+EFO-SPOUT: Received a SCLOUT command with no ACTION value");
				}
			}else if(command != null && command._type.equals(SynEFOMessage.Type.DUMMY)) {
				// Nothing to do in case of DUMMY variable
				System.out.println("+EFO-SPOUT: Received a DUMMY command");
			}else {
				// Unrecognized message type
				System.out.println("+EFO-SPOUT: Received an unrecognized message");
			}
			_stat_tuple_counter = 0;
		}
		*/
	}

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		pet = new ZooPet("127.0.0.1", 2181, _task_name, _task_id, _task_ip);
		if(_active_downstream_tasks == null && _downstream_tasks == null) {
			registerToSynEFO();
		}
		_collector = collector;
		_task_id = context.getThisTaskId();
		metricObject = new SynEFOMetric();
		metricObject.initMetrics(context, _task_name, Integer.toString(_task_id));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> producerSchema = new ArrayList<String>();
		producerSchema.add("SYNEFO_HEADER");
		producerSchema.addAll(_tuple_producer.getSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

}