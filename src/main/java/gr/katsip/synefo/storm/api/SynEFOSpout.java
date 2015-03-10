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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.katsip.synefo.metric.SynefoMetric;
import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.lib.SynEFOMessage;
import gr.katsip.synefo.storm.lib.SynEFOMessage.Type;
import gr.katsip.synefo.storm.producers.AbstractTupleProducer;
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
	
	Logger logger = LoggerFactory.getLogger(SynEFOSpout.class);

	private String _task_name;

	private SpoutOutputCollector _collector;

	private int taskId;

	private String taskIP;

	private ArrayList<String> _downstream_tasks = null;

	private ArrayList<Integer> _int_downstream_tasks = null;

	private ArrayList<String> _active_downstream_tasks = null;

	private ArrayList<Integer> _int_active_downstream_tasks = null;

	private int idx;

	private String synefoIP;

	private Integer synefoPort;

	private Socket socket;

	private ObjectOutputStream output = null;

	private ObjectInputStream input = null;

	private TaskStatistics stats;

	private AbstractTupleProducer tupleProducer;

	private long _tuple_counter;

	private SynefoMetric metricObject;

	transient private ZooPet pet;

	private String zooIP;

	private Integer zooPort;

	public SynEFOSpout(String task_name, String synEFO_ip, Integer synEFO_port, 
			AbstractTupleProducer tupleProducer, String zooIP, Integer zooPort) {
		_task_name = task_name;
		_downstream_tasks = null;
		_active_downstream_tasks = null;
		synefoIP = synEFO_ip;
		synefoPort = synEFO_port;
		stats = new TaskStatistics();
		this.tupleProducer = tupleProducer;
		_tuple_counter = 0;
		this.zooIP = zooIP;
		this.zooPort = zooPort;
	}

	@SuppressWarnings("unchecked")
	public void registerToSynEFO() {
		logger.info("+EFO-SPOUT " + _task_name + ":" + taskId + "@" + taskIP + " in registerToSynEFO().");
		socket = null;
		SynEFOMessage msg = new SynEFOMessage();
		msg._type = Type.REG;
		msg._values.put("TASK_TYPE", "SPOUT");
		msg._values.put("TASK_NAME", _task_name);
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
			msg._values.put("TASK_IP", taskIP);
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		msg._values.put("TASK_ID", Integer.toString(taskId));
		try {
			socket = new Socket(synefoIP, synefoPort);
			output = new ObjectOutputStream(socket.getOutputStream());
			input = new ObjectInputStream(socket.getInputStream());
			output.writeObject(msg);
			output.flush();
			msg = null;
			System.out.println("SPOUT (" + _task_name + ") wait 1");
			ArrayList<String> _downstream = null;
			_downstream = (ArrayList<String>) input.readObject();
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
			_active_downstream = (ArrayList<String>) input.readObject();
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
			output.flush();
			output.close();
			input.close();
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
		pet.getScaleCommand();
		System.out.println("+EFO-SPOUT (" + 
				_task_name + ":" + taskId + 
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
			values.add("SYNEFO_HEADER");
			Values returnedValues = tupleProducer.nextTuple();
			for(int i = 0; i < returnedValues.size(); i++) {
				values.add(returnedValues.get(i));
			}
			_collector.emitDirect(_int_active_downstream_tasks.get(idx), values);
			if(idx >= (_int_active_downstream_tasks.size() - 1)) {
				idx = 0;
			}else {
				idx += 1;
			}
		}
		_tuple_counter += 1;
		metricObject.updateMetrics(_tuple_counter);
		stats.updateMemory();
		stats.updateCpuLoad();
		stats.updateLatency();
		stats.updateThroughput(_tuple_counter);
		String scaleCommand = "";
		synchronized(pet) {
			if(pet.pendingCommand != null) {
				scaleCommand = pet.returnScaleCommand();
			}
		}
		if(scaleCommand != null && scaleCommand.length() > 0) {
			logger.info("+EFO-SPOUT(" + this._task_name + ":" + this.taskId + "@" + this.taskIP + 
					") located scale-command: " + scaleCommand + ", about to produce punctuation tuple");
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
				_active_downstream_tasks.add(taskWithIp);
				_int_active_downstream_tasks.add(task_id);
				strBuild.append(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.ADD_ACTION + "/");
			}else if(action.toLowerCase().contains("remove")) {
				strBuild.append(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.REMOVE_ACTION + "/");
			}
			strBuild.append(SynEFOConstant.COMP_TAG + ":" + task + ":" + task_id + "/");
			strBuild.append(SynEFOConstant.COMP_NUM_TAG + ":" + _int_active_downstream_tasks.size() + "/");
			strBuild.append(SynEFOConstant.COMP_IP_TAG + ":" + taskIp + "/");
			/**
			 * Populate other schema fields with null values, 
			 * after SYNEFO_HEADER
			 */
			Values punctValue = new Values();
			punctValue.add(strBuild.toString());
			for(int i = 0; i < tupleProducer.getSchema().size(); i++) {
				punctValue.add(null);
			}
			for(Integer d_task : _int_active_downstream_tasks) {
				_collector.emitDirect(d_task, punctValue);
			}
			/**
			 * In the case of removing a downstream task 
			 * we remove it after sending the punctuation tuples, so 
			 * that the removed task is notified to share state
			 */
			if(action.toLowerCase().contains("remove") && _active_downstream_tasks.indexOf(taskWithIp) >= 0) {
				_active_downstream_tasks.remove(_active_downstream_tasks.indexOf(taskWithIp));
				_int_active_downstream_tasks.remove(_int_active_downstream_tasks.indexOf(task_id));
			}
		}
	}

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		pet = new ZooPet(zooIP, zooPort, _task_name, taskId, taskIP);
		if(_active_downstream_tasks == null && _downstream_tasks == null) {
			registerToSynEFO();
		}
		_collector = collector;
		taskId = context.getThisTaskId();
		metricObject = new SynefoMetric();
		metricObject.initMetrics(context, _task_name, Integer.toString(taskId));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> producerSchema = new ArrayList<String>();
		producerSchema.add("SYNEFO_HEADER");
		producerSchema.addAll(tupleProducer.getSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

}