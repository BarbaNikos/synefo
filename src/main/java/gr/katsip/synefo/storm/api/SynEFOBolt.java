package gr.katsip.synefo.storm.api;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import gr.katsip.synefo.metric.SynefoMetric;
import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.lib.SynEFOMessage;
import gr.katsip.synefo.storm.lib.SynEFOMessage.Type;
import gr.katsip.synefo.storm.operators.AbstractOperator;
import gr.katsip.synefo.utils.SynEFOConstant;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SynEFOBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4011052074675303959L;

	private String taskName;

	private int idx;

	private long tupleCounter;

	private OutputCollector _collector;

	private ArrayList<String> downstreamTasks;

	private ArrayList<Integer> intDownstreamTasks;

	private ArrayList<String> activeDownstreamTasks;

	private ArrayList<Integer> intActiveDownstreamTasks;

	private String _synEFO_ip = null;

	private int _task_id = -1;

	private String _task_ip;

	private Integer _synEFO_port = -1;

	private Socket socket;

	private ObjectOutputStream _output;

	private ObjectInputStream _input;

	private TaskStatistics _stats;

	private AbstractOperator _operator;

	private SynefoMetric metricObject;

	private List<Values> stateValues;

	private ZooPet pet;

	public SynEFOBolt(String task_name, String synEFO_ip, Integer synEFO_port, AbstractOperator operator) {
		taskName = task_name;
		_synEFO_ip = synEFO_ip;
		_synEFO_port = synEFO_port;
		downstreamTasks = null;
		intDownstreamTasks = null;
		activeDownstreamTasks = null;
		intActiveDownstreamTasks = null;
		_stats = new TaskStatistics();
		_operator = operator;
		tupleCounter = 0;
		stateValues = new ArrayList<Values>();
		operator.init(stateValues);
	}

	/**
	 * The function for registering to SynEFO server
	 */
	@SuppressWarnings("unchecked")
	public void registerToSynEFO() {
		socket = null;
		SynEFOMessage msg = new SynEFOMessage();
		msg._type = Type.REG;
		try {
			_task_ip = InetAddress.getLocalHost().getHostAddress();
			msg._values.put("TASK_IP", _task_ip);
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		msg._values.put("TASK_TYPE", "BOLT");
		msg._values.put("TASK_NAME", taskName);
		msg._values.put("TASK_ID", Integer.toString(_task_id));
		try {
			socket = new Socket(_synEFO_ip, _synEFO_port);
			_output = new ObjectOutputStream(socket.getOutputStream());
			_input = new ObjectInputStream(socket.getInputStream());
			_output.writeObject(msg);
			_output.flush();
			msg = null;
			ArrayList<String> _downstream = null;
			_downstream = (ArrayList<String>) _input.readObject();
			if(_downstream != null && _downstream.size() > 0) {
				downstreamTasks = new ArrayList<String>(_downstream);
				intDownstreamTasks = new ArrayList<Integer>();
				Iterator<String> itr = downstreamTasks.iterator();
				while(itr.hasNext()) {
					StringTokenizer strTok = new StringTokenizer(itr.next(), ":");
					strTok.nextToken();
					String taskWithIp = strTok.nextToken();
					strTok = new StringTokenizer(taskWithIp, "@");
					Integer task = Integer.parseInt(strTok.nextToken());
					intDownstreamTasks.add(task);
				}
			}else {
				downstreamTasks = new ArrayList<String>();
				intDownstreamTasks = new ArrayList<Integer>();
			}
			ArrayList<String> _active_downstream = null;
			_active_downstream = (ArrayList<String>) _input.readObject();
			if(_active_downstream != null && _active_downstream.size() > 0) {
				activeDownstreamTasks = new ArrayList<String>(_active_downstream);
				intActiveDownstreamTasks = new ArrayList<Integer>();
				Iterator<String> itr = activeDownstreamTasks.iterator();
				while(itr.hasNext()) {
					StringTokenizer strTok = new StringTokenizer(itr.next(), ":");
					strTok.nextToken();
					String taskWithIp = strTok.nextToken();
					strTok = new StringTokenizer(taskWithIp, "@");
					Integer task = Integer.parseInt(strTok.nextToken());
					intActiveDownstreamTasks.add(task);
				}
				idx = 0;
			}else {
				activeDownstreamTasks = new ArrayList<String>();
				intActiveDownstreamTasks = new ArrayList<Integer>();
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
		pet.setBoltNodeWatch();
		System.out.println("+EFO-BOLT (" + 
				taskName + ":" + _task_id + 
				") registered to synEFO successfully.");
	}

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_task_id = context.getThisTaskId();
		try {
			_task_ip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		pet = new ZooPet("127.0.0.1", 2181, taskName, _task_id, _task_ip);
		if(downstreamTasks == null && activeDownstreamTasks == null) {
			registerToSynEFO();
		}
		this.metricObject = new SynefoMetric();
		metricObject.initMetrics(context, taskName, Integer.toString(_task_id));
	}


	public void execute(Tuple tuple) {
		/**
		 * If punctuation tuple is received:
		 * Perform Share of state and return execution
		 */
		String synefoHeader = tuple.getString(tuple.getFields().fieldIndex("SYNEFO_HEADER"));
		if(synefoHeader != null && synefoHeader.equals("") == false) {
			StringTokenizer txt = new StringTokenizer(synefoHeader, "/");
			String prefix = txt.nextToken();
			if(prefix.equals(SynEFOConstant.PUNCT_TUPLE_TAG)) {
				handlePunctuationTuple(tuple);
				return;
			}
		}
		Values produced_values = null;
		Values values = new Values(tuple.getValues().toArray());
		values.remove(tuple.getFields().fieldIndex("SYNEFO_HEADER"));
		List<String> fieldList = tuple.getFields().toList();
		fieldList.remove(0);
		Fields fields = new Fields(fieldList);
		if(intActiveDownstreamTasks != null && intActiveDownstreamTasks.size() > 0) {
			List<Values> returnedTuples = _operator.execute(fields, values);
			for(Values v : returnedTuples) {
				produced_values = new Values();
				produced_values.add("SYNEFO_HEADER");
				for(int i = 0; i < v.size(); i++) {
					produced_values.add(v.get(i));
				}
				_collector.emitDirect(intActiveDownstreamTasks.get(idx), produced_values);
			}
			_collector.ack(tuple);
			if(idx >= (intActiveDownstreamTasks.size() - 1)) {
				idx = 0;
			}else {
				idx += 1;
			}
		}else {
			List<Values> returnedTuples = _operator.execute(fields, values);
			for(Values v : returnedTuples) {
				produced_values = new Values();
				produced_values.add("SYNEFO_HEADER");
				for(int i = 0; i < v.size(); i++) {
					produced_values.add(v.get(i));
				}
//				_collector.emit(produced_values);
//				System.out.println("synefo-bolt(" + this.taskName + ":" + this._task_id + "@" + this._task_ip + ") emits: " + produced_values);
			}
			_collector.ack(tuple);
		}
		tupleCounter += 1;
		metricObject.updateMetrics(tupleCounter);
		_stats.updateMemory();
		_stats.updateCpuLoad();
		_stats.updateLatency();
		_stats.updateThroughput(tupleCounter);

		pet.setStatisticData(_stats.getCpuLoad(), _stats.getMemory(), (int) _stats.getLatency(), (int) _stats.getThroughput());
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
				activeDownstreamTasks.add(taskWithIp);
				intActiveDownstreamTasks.add(task_id);
				strBuild.append(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.ADD_ACTION + "/");
			}else if(action.toLowerCase().contains("remove")) {
				strBuild.append(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.REMOVE_ACTION + "/");
			}
			strBuild.append(SynEFOConstant.COMP_TAG + ":" + task + ":" + task_id + "/");
			strBuild.append(SynEFOConstant.COMP_NUM_TAG + ":" + intActiveDownstreamTasks.size() + "/");
			strBuild.append(SynEFOConstant.COMP_IP_TAG + ":" + taskIp + "/");
			/**
			 * Populate other schema fields with null values, 
			 * after SYNEFO_HEADER
			 */
			Values punctValue = new Values();
			punctValue.add(strBuild.toString());
			for(int i = 0; i < _operator.getOutputSchema().size(); i++) {
				punctValue.add(null);
			}
			for(Integer d_task : intActiveDownstreamTasks) {
				_collector.emitDirect(d_task, punctValue);
			}
			/**
			 * In the case of removing a downstream task 
			 * we remove it after sending the punctuation tuples, so 
			 * that the removed task is notified to share state
			 */
			if(action.toLowerCase().contains("remove") && activeDownstreamTasks.indexOf(taskWithIp) >= 0) {
				activeDownstreamTasks.remove(activeDownstreamTasks.indexOf(taskWithIp));
				intActiveDownstreamTasks.remove(intActiveDownstreamTasks.indexOf(task_id));
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> producerSchema = new ArrayList<String>();
		producerSchema.add("SYNEFO_HEADER");
		producerSchema.addAll(_operator.getOutputSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

	public void handlePunctuationTuple(Tuple tuple) {
		/**
		 * Initiate migration of state
		 */
		String action = null;
		String component_name = null;
		String component_id = null;
		Integer comp_num = -1;
		String ip = null;
		StringTokenizer str_tok = new StringTokenizer((String) tuple.getValues().get(tuple.getFields().fieldIndex("SYNEFO_HEADER")), "/");
		while(str_tok.hasMoreTokens()) {
			String s = str_tok.nextToken();
			if((s.equals(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.ADD_ACTION) || 
					s.equals(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.REMOVE_ACTION)) && 
					s.equals(SynEFOConstant.PUNCT_TUPLE_TAG) == false) {
				action = s;
			}else if((s.equals(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.ADD_ACTION) == false && 
					s.equals(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.REMOVE_ACTION) == false) && 
					s.equals(SynEFOConstant.PUNCT_TUPLE_TAG) == false && s.startsWith(SynEFOConstant.COMP_TAG) 
					&& s.startsWith(SynEFOConstant.COMP_NUM_TAG) == false && s.startsWith(SynEFOConstant.COMP_IP_TAG) == false) {
				StringTokenizer strTok = new StringTokenizer(s, ":");
				component_id = strTok.nextToken();
				component_name = strTok.nextToken();
				component_id = strTok.nextToken();
			}else if((s.equals(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.ADD_ACTION) == false && 
					s.equals(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.REMOVE_ACTION) == false) && 
					s.equals(SynEFOConstant.PUNCT_TUPLE_TAG) == false && s.startsWith(SynEFOConstant.COMP_TAG) && 
					s.startsWith(SynEFOConstant.COMP_NUM_TAG) && s.startsWith(SynEFOConstant.COMP_IP_TAG) == false) {
				StringTokenizer strTok = new StringTokenizer(s, ":");
				strTok.nextToken();
				comp_num = Integer.parseInt(strTok.nextToken());
			}else if((s.equals(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.ADD_ACTION) == false && 
					s.equals(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.REMOVE_ACTION) == false) && 
					s.equals(SynEFOConstant.PUNCT_TUPLE_TAG) == false && s.startsWith(SynEFOConstant.COMP_TAG) && 
					s.startsWith(SynEFOConstant.COMP_IP_TAG)) {
				StringTokenizer strTok = new StringTokenizer(s, ":");
				strTok.nextToken();
				ip = strTok.nextToken();
			}
		}
		/**
		 * 
		 */
		if(action != null && action.equals(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.ADD_ACTION)) {
			String selfComp = this.taskName + ":" + this._task_id;
			if(selfComp.equals(component_name + ":" + component_id)) {
				/**
				 * If this component is added, open a ServerSocket
				 */
				try {
					ServerSocket _socket = new ServerSocket(6000 + _task_id);
					int numOfStatesReceived = 0;
					while(numOfStatesReceived < (comp_num - 1)) {
						Socket client = _socket.accept();
						ObjectOutputStream _stateOutput = new ObjectOutputStream(client.getOutputStream());
						ObjectInputStream _stateInput = new ObjectInputStream(client.getInputStream());
						@SuppressWarnings("unchecked")
						List<Values> newState = (List<Values>) _stateInput.readObject();
						_operator.mergeState(_operator.getOutputSchema(), newState);
						_stateOutput.writeObject("+EFO_ACK");
						_stateOutput.flush();
						_stateInput.close();
						_stateOutput.close();
						client.close();
						numOfStatesReceived += 1;
					}
					_socket.close();
				} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}
			}else {
				Socket client = new Socket();
				Integer comp_task_id = Integer.parseInt(component_id);
				boolean attempt_flag = true;
				while (attempt_flag == true) {
					try {
						client = new Socket(ip, 6000 + comp_task_id);
						attempt_flag = false;
					} catch (IOException e) {
						System.out.println("+EFO:BOLT(" + _task_id + "): Connect failed (1), waiting and trying again");
						try
						{
							Thread.sleep(500);
						}
						catch(InterruptedException ie){
							ie.printStackTrace();
						}
					}
				}
				try {
					ObjectOutputStream _stateOutput = new ObjectOutputStream(client.getOutputStream());
					ObjectInputStream _stateInput = new ObjectInputStream(client.getInputStream());
					_stateOutput.writeObject(_operator.getStateValues());
					_stateOutput.flush();
					String response = (String) _stateInput.readObject();
					if(response.equals("+EFO_ACK")) {

					}
					_stateInput.close();
					_stateOutput.close();
					client.close();
				} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}else if(action != null && action.equals(SynEFOConstant.ACTION_PREFIX + ":" + SynEFOConstant.REMOVE_ACTION)) {
			String selfComp = this.taskName + ":" + this._task_id;
			if(selfComp.equals(component_name + ":" + component_id)) {
				try {
					ServerSocket _socket = new ServerSocket(6000 + _task_id);
					int numOfStatesReceived = 0;
					while(numOfStatesReceived < (comp_num - 1)) {
						Socket client = _socket.accept();
						ObjectOutputStream _stateOutput = new ObjectOutputStream(client.getOutputStream());
						ObjectInputStream _stateInput = new ObjectInputStream(client.getInputStream());
						_stateOutput.writeObject(_operator.getStateValues());
						_stateOutput.flush();
						String response = (String) _stateInput.readObject();
						if(response.equals("+EFO_ACK")) {

						}
						_stateInput.close();
						_stateOutput.close();
						client.close();
						numOfStatesReceived += 1;
					}
					_socket.close();
				} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}
			}else {
				Socket client = new Socket();
				Integer comp_task_id = Integer.parseInt(component_id);
				boolean attempt_flag = true;
				while (attempt_flag == true) {
					try {
						client = new Socket(ip, 6000 + comp_task_id);
						attempt_flag = false;
					} catch (IOException e) {
						System.out.println("+EFO:BOLT(" + _task_id + "): Connect failed (2), waiting and trying again");
						try
						{
							Thread.sleep(500);
						}
						catch(InterruptedException ie){
							ie.printStackTrace();
						}
					}
				}
				try {
					ObjectOutputStream _stateOutput = new ObjectOutputStream(client.getOutputStream());
					ObjectInputStream _stateInput = new ObjectInputStream(client.getInputStream());
					@SuppressWarnings("unchecked")
					List<Values> newState = (List<Values>) _stateInput.readObject();
					_operator.mergeState(_operator.getOutputSchema(), newState);
					_stateOutput.writeObject("+EFO_ACK");
					_stateOutput.flush();
					_stateInput.close();
					_stateOutput.close();
					client.close();
				} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		pet.resetSubmittedScaleFlag();
	}

	public List<Values> getStateValue() {
		_operator.getStateValues();
		return stateValues;
	}

	public void printState() {
		List<Values> state = _operator.getStateValues();
		System.out.println("+EFO_BOLT(" + this.taskName + ":" + this._task_id + ") printState() :");
		Iterator<Values> itr = state.iterator();
		while(itr.hasNext()) {
			Values val = itr.next();
			System.out.println("<" + val.toString() + ">");
		}
		System.out.println("+EFO_BOLT CONCLUDED printState()");
	}

}