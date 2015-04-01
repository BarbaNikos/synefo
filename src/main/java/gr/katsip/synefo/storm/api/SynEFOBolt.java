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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.katsip.synefo.metric.SynefoMetric;
import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.lib.SynEFOMessage;
import gr.katsip.synefo.storm.lib.SynEFOMessage.Type;
import gr.katsip.synefo.storm.operators.AbstractOperator;
import gr.katsip.synefo.utils.SynefoConstant;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 
 * @author Nick R. Katsipoulakis
 *
 */
public class SynEFOBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4011052074675303959L;

	Logger logger = LoggerFactory.getLogger(SynEFOBolt.class);

	private String taskName;

	private int downStreamIndex;

	private long tupleCounter;

	private OutputCollector collector;

	private ArrayList<String> downstreamTasks;

	private ArrayList<Integer> intDownstreamTasks;

	private ArrayList<String> activeDownstreamTasks;

	private ArrayList<Integer> intActiveDownstreamTasks;

	private String synefoServerIP = null;

	private int taskID = -1;

	private String taskIP;

	private Integer synefoServerPort = -1;

	private Socket socket;

	private ObjectOutputStream _output;

	private ObjectInputStream _input;

	private TaskStatistics statistics;

	private AbstractOperator operator;

	private SynefoMetric metricObject;

	private List<Values> stateValues;

	private ZooPet zooPet;

	private String zooIP;

	private Integer zooPort;

	private int reportCounter;

	public SynEFOBolt(String task_name, String synEFO_ip, Integer synEFO_port, 
			AbstractOperator operator, String zooIP, Integer zooPort) {
		taskName = task_name;
		synefoServerIP = synEFO_ip;
		synefoServerPort = synEFO_port;
		downstreamTasks = null;
		intDownstreamTasks = null;
		activeDownstreamTasks = null;
		intActiveDownstreamTasks = null;
		statistics = new TaskStatistics();
		this.operator = operator;
		tupleCounter = 0;
		stateValues = new ArrayList<Values>();
		operator.init(stateValues);
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		reportCounter = 0;
	}

	/**
	 * The function for registering to SynEFO server
	 */
	@SuppressWarnings("unchecked")
	public void registerToSynEFO() {
		logger.info("+EFO-BOLT (" + taskName + ":" + taskID + "@" + taskIP + ") in registerToSynEFO().");
		socket = null;
		SynEFOMessage msg = new SynEFOMessage();
		msg._type = Type.REG;
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
			msg._values.put("TASK_IP", taskIP);
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		msg._values.put("TASK_TYPE", "BOLT");
		msg._values.put("TASK_NAME", taskName);
		msg._values.put("TASK_ID", Integer.toString(taskID));
		try {
			socket = new Socket(synefoServerIP, synefoServerPort);
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
				downStreamIndex = 0;
			}else {
				activeDownstreamTasks = new ArrayList<String>();
				intActiveDownstreamTasks = new ArrayList<Integer>();
				downStreamIndex = 0;
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
		zooPet.start();
		zooPet.getScaleCommand();
		logger.info("+EFO-BOLT (" + 
				taskName + ":" + taskID + 
				") registered to synEFO successfully.");
	}

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		taskID = context.getThisTaskId();
		if(conf.containsKey("TOPOLOGY_DEBUG") || conf.containsKey("topology_debug")) {
			String debug = (String) conf.get("TOPOLOGY_DEBUG");
			logger.info("+EFO-BOLT (" + taskName + ":" + taskID + "@" + taskIP + ") topology debug flag: " + debug);
		}
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		zooPet = new ZooPet(zooIP, zooPort, taskName, taskID, taskIP);
		if(downstreamTasks == null && activeDownstreamTasks == null) {
			registerToSynEFO();
		}
		this.metricObject = new SynefoMetric();
		metricObject.initMetrics(context, taskName, Integer.toString(taskID));
		logger.info("+EFO-BOLT (" + taskName + ":" + taskID + "@" + taskIP + ") in prepare().");
	}


	public void execute(Tuple tuple) {
		/**
		 * If punctuation tuple is received:
		 * Perform Share of state and return execution
		 */
		if(tuple.getFields().contains("SYNEFO_HEADER") == false) {
			logger.error("+EFO-BOLT (" + taskName + ":" + taskID + "@" + taskIP + 
					") in execute(): received tuple with fields: " + tuple.getFields().toString() + 
					" from component: " + tuple.getSourceComponent() + " with task-id: " + tuple.getSourceTask());
		}
		String synefoHeader = tuple.getString(tuple.getFields().fieldIndex("SYNEFO_HEADER"));
		Long synefoTimestamp = tuple.getLong(tuple.getFields().fieldIndex("SYNEFO_TIMESTAMP"));
		if(synefoHeader != null && synefoHeader.equals("") == false && synefoHeader.contains("/") == true) {
			StringTokenizer txt = new StringTokenizer(synefoHeader, "/");
			String prefix = txt.nextToken();
			if(prefix.equals(SynefoConstant.PUNCT_TUPLE_TAG)) {
				handlePunctuationTuple(tuple);
				return;
			}
		}
		/**
		 * Remove from both values and fields SYNEFO_HEADER & SYNEFO_TIMESTAMP
		 */
		Values produced_values = null;
		Values values = new Values(tuple.getValues().toArray());
		values.remove(tuple.getFields().fieldIndex("SYNEFO_HEADER"));
		values.remove(tuple.getFields().fieldIndex("SYNEFO_TIMESTAMP"));
		List<String> fieldList = tuple.getFields().toList();
		fieldList.remove(0);
		fieldList.remove(0);
		Fields fields = new Fields(fieldList);
		if(intActiveDownstreamTasks != null && intActiveDownstreamTasks.size() > 0) {
			List<Values> returnedTuples = operator.execute(fields, values);
			for(Values v : returnedTuples) {
				produced_values = new Values();
				produced_values.add("SYNEFO_HEADER");
				produced_values.add(new Long(System.currentTimeMillis()));
				for(int i = 0; i < v.size(); i++) {
					produced_values.add(v.get(i));
				}
				collector.emitDirect(intActiveDownstreamTasks.get(downStreamIndex), produced_values);
			}
			collector.ack(tuple);
			if(downStreamIndex >= (intActiveDownstreamTasks.size() - 1)) {
				downStreamIndex = 0;
			}else {
				downStreamIndex += 1;
			}
		}else {
			List<Values> returnedTuples = operator.execute(fields, values);
			for(Values v : returnedTuples) {
				produced_values = new Values();
				produced_values.add("SYNEFO_HEADER");
				produced_values.add(new Long(System.currentTimeMillis()));
				for(int i = 0; i < v.size(); i++) {
					produced_values.add(v.get(i));
				}
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + 
						this.taskIP + ") emits: " + produced_values);
			}
			collector.ack(tuple);
		}
		tupleCounter += 1;
		metricObject.updateMetrics(tupleCounter);
		statistics.updateMemory();
		statistics.updateCpuLoad();
		if(synefoTimestamp != null) {
			long latency = System.currentTimeMillis() - synefoTimestamp;
			statistics.updateLatency(latency);
		}else {
			statistics.updateLatency();
		}
		statistics.updateThroughput(1);

		if(reportCounter >= 1000) {
			logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
					") timestamp: " + System.currentTimeMillis() + ", " + 
					"cpu: " + statistics.getCpuLoad() + 
					", memory: " + statistics.getMemory() + 
					", latency: " + statistics.getLatency() + 
					", throughput: " + statistics.getThroughput());
			reportCounter = 0;
		}else {
			reportCounter += 1;
		}

		//zooPet.setStatisticData(statistics.getCpuLoad(), statistics.getMemory(), 
		//		(int) statistics.getLatency(), 
		//		(int) statistics.getThroughput());
		zooPet.setLatency(statistics.getLatency());
		String scaleCommand = "";
		synchronized(zooPet) {
			if(zooPet.pendingCommands.isEmpty() == false) {
				scaleCommand = zooPet.returnScaleCommand();
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
			strBuild.append(SynefoConstant.PUNCT_TUPLE_TAG + "/");
			downStreamIndex = 0;
			if(action.toLowerCase().contains("activate") || action.toLowerCase().contains("deactivate")) {
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + ") located scale-command: " + 
						scaleCommand + ", about to update routing tables.");
				if(action.toLowerCase().equals("activate")) {
					if(activeDownstreamTasks.contains(taskWithIp) == false && intActiveDownstreamTasks.contains(task_id) == false) {
						activeDownstreamTasks.add(taskWithIp);
						intActiveDownstreamTasks.add(task_id);
					}
				}else if(action.toLowerCase().equals("deactivate")) {
					activeDownstreamTasks.remove(activeDownstreamTasks.indexOf(taskWithIp));
					intActiveDownstreamTasks.remove(intActiveDownstreamTasks.indexOf(task_id));
				}
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + ") located scale-command: " + 
						scaleCommand + ", updated routing tables: " + intActiveDownstreamTasks);
			}else {
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + ") located scale-command: " + 
						scaleCommand + ", about to produce punctuation tuple");
				if(action.toLowerCase().contains("add")) {
					if(activeDownstreamTasks.contains(taskWithIp) == false && intActiveDownstreamTasks.contains(task_id) == false) {
						activeDownstreamTasks.add(taskWithIp);
						intActiveDownstreamTasks.add(task_id);
					}
					strBuild.append(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.ADD_ACTION + "/");
				}else if(action.toLowerCase().contains("remove")) {
					strBuild.append(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.REMOVE_ACTION + "/");
				}
				strBuild.append(SynefoConstant.COMP_TAG + ":" + task + ":" + task_id + "/");
				strBuild.append(SynefoConstant.COMP_NUM_TAG + ":" + intActiveDownstreamTasks.size() + "/");
				strBuild.append(SynefoConstant.COMP_IP_TAG + ":" + taskIp + "/");
				/**
				 * Populate other schema fields with null values, 
				 * after SYNEFO_HEADER
				 */
				Values punctValue = new Values();
				punctValue.add(strBuild.toString());
				/**
				 * Add typical SYNEFO_TIMESTAMP value
				 */
				punctValue.add(new Long(System.currentTimeMillis()));
				for(int i = 0; i < operator.getOutputSchema().size(); i++) {
					punctValue.add(null);
				}
				for(Integer d_task : intActiveDownstreamTasks) {
					collector.emitDirect(d_task, punctValue);
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
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> producerSchema = new ArrayList<String>();
		producerSchema.add("SYNEFO_HEADER");
		producerSchema.add("SYNEFO_TIMESTAMP");
		producerSchema.addAll(operator.getOutputSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

	@SuppressWarnings("unchecked")
	public void handlePunctuationTuple(Tuple tuple) {
		/**
		 * Initiate migration of state
		 */
		String action = null;
		String component_name = null;
		String component_id = null;
		Integer comp_num = -1;
		String ip = null;
		logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
				") received punctuation tuple: " + tuple.toString());
		StringTokenizer str_tok = new StringTokenizer((String) tuple.getValues()
				.get(tuple.getFields().fieldIndex("SYNEFO_HEADER")), "/");
		while(str_tok.hasMoreTokens()) {
			String s = str_tok.nextToken();
			if((s.equals(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.ADD_ACTION) || 
					s.equals(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.REMOVE_ACTION)) && 
					s.equals(SynefoConstant.PUNCT_TUPLE_TAG) == false) {
				action = s;
			}else if((s.equals(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.ADD_ACTION) == false && 
					s.equals(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.REMOVE_ACTION) == false) && 
					s.equals(SynefoConstant.PUNCT_TUPLE_TAG) == false && s.startsWith(SynefoConstant.COMP_TAG) 
					&& s.startsWith(SynefoConstant.COMP_NUM_TAG) == false && 
					s.startsWith(SynefoConstant.COMP_IP_TAG) == false) {
				StringTokenizer strTok = new StringTokenizer(s, ":");
				component_id = strTok.nextToken();
				component_name = strTok.nextToken();
				component_id = strTok.nextToken();
			}else if((s.equals(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.ADD_ACTION) == false && 
					s.equals(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.REMOVE_ACTION) == false) && 
					s.equals(SynefoConstant.PUNCT_TUPLE_TAG) == false && s.startsWith(SynefoConstant.COMP_TAG) && 
					s.startsWith(SynefoConstant.COMP_NUM_TAG) && s.startsWith(SynefoConstant.COMP_IP_TAG) == false) {
				StringTokenizer strTok = new StringTokenizer(s, ":");
				strTok.nextToken();
				comp_num = Integer.parseInt(strTok.nextToken());
			}else if((s.equals(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.ADD_ACTION) == false && 
					s.equals(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.REMOVE_ACTION) == false) && 
					s.equals(SynefoConstant.PUNCT_TUPLE_TAG) == false && 
					s.startsWith(SynefoConstant.COMP_NUM_TAG) == false && 
					s.startsWith(SynefoConstant.COMP_TAG) == false && 
					s.startsWith(SynefoConstant.COMP_IP_TAG)) {
				StringTokenizer strTok = new StringTokenizer(s, ":");
				strTok.nextToken();
				ip = strTok.nextToken();
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
						") located peer's IP: " + ip);
			}
		}
		/**
		 * 
		 */
		if(action != null && action.equals(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.ADD_ACTION)) {
			System.out.println("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
					") received an ADD action");
			String selfComp = this.taskName + ":" + this.taskID;
			if(selfComp.equals(component_name + ":" + component_id)) {
				/**
				 * If this component is added, open a ServerSocket
				 */
				try {
					ServerSocket _socket = new ServerSocket(6000 + taskID);
					int numOfStatesReceived = 0;
					logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
							") accepting connections to receive state... (IP:" + 
							_socket.getInetAddress().getHostAddress() + ", port: " + _socket.getLocalPort());
					boolean activeListFlag = false;
					while(numOfStatesReceived < (comp_num - 1)) {
						Socket client = _socket.accept();
						ObjectOutputStream _stateOutput = new ObjectOutputStream(client.getOutputStream());
						ObjectInputStream _stateInput = new ObjectInputStream(client.getInputStream());
						List<Values> newState = (List<Values>) _stateInput.readObject();
						operator.mergeState(operator.getOutputSchema(), newState);
						if(activeListFlag == false) {
							_stateOutput.writeObject("+EFO_ACT_NODES");
							_stateOutput.flush();
							this.activeDownstreamTasks = (ArrayList<String>) _stateInput.readObject();
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
							logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
									") received active downstream task list:" + activeDownstreamTasks);
							activeListFlag = true;
							downStreamIndex = 0;
						}else {
							_stateOutput.writeObject("+EFO_ACK");
						}
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
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + ") Finished accepting connections to receive state.");
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + ") routing table:" + this.activeDownstreamTasks);
			}else {
				Socket client = new Socket();
				Integer comp_task_id = Integer.parseInt(component_id);
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
						") about to send state to about-to-be-added operator (IP: " + 
						ip + ", port: " + (6000 + comp_task_id) + ").");
				boolean attempt_flag = true;
				while (attempt_flag == true) {
					try {
						client = new Socket(ip, 6000 + comp_task_id);
						attempt_flag = false;
					} catch (IOException e) {
						logger.info("+EFO-BOLT (" + taskID + "): Connect failed (1), waiting and trying again");
						logger.info("+EFO-BOLT (" + taskID + "): " + e.getMessage());
						try
						{
							Thread.sleep(500);
						}
						catch(InterruptedException ie){
							ie.printStackTrace();
						}
					}
				}
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
						") Connection established...");
				try {
					ObjectOutputStream _stateOutput = new ObjectOutputStream(client.getOutputStream());
					ObjectInputStream _stateInput = new ObjectInputStream(client.getInputStream());
					_stateOutput.writeObject(operator.getStateValues());
					_stateOutput.flush();
					String response = (String) _stateInput.readObject();
					if(response.equals("+EFO_ACT_NODES")) {
						_stateOutput.writeObject(this.activeDownstreamTasks);
						_stateOutput.flush();
					}
					_stateInput.close();
					_stateOutput.close();
					client.close();
				} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
						") sent state to newly added node successfully...");
			}
		}else if(action != null && action.equals(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.REMOVE_ACTION)) {
			logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
					") received a REMOVE action");
			String selfComp = this.taskName + ":" + this.taskID;
			if(selfComp.equals(component_name + ":" + component_id)) {
				try {
					ServerSocket _socket = new ServerSocket(6000 + taskID);
					logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
							") accepting connections to receive state... (IP:" + 
							_socket.getInetAddress() + ", port: " + _socket.getLocalPort());
					int numOfStatesReceived = 0;
					while(numOfStatesReceived < (comp_num - 1)) {
						Socket client = _socket.accept();
						ObjectOutputStream _stateOutput = new ObjectOutputStream(client.getOutputStream());
						ObjectInputStream _stateInput = new ObjectInputStream(client.getInputStream());
						_stateOutput.writeObject(operator.getStateValues());
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
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
						") Finished accepting connections to send state.");
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
						") routing table:" + this.activeDownstreamTasks);
			}else {
				Socket client = new Socket();
				Integer comp_task_id = Integer.parseInt(component_id);
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
						") about to receive state from about-to-be-removed operator (IP: " + ip + 
						", port: " + (6000 + comp_task_id) + ").");
				boolean attempt_flag = true;
				while (attempt_flag == true) {
					try {
						client = new Socket(ip, 6000 + comp_task_id);
						attempt_flag = false;
					} catch (IOException e) {
						logger.info("+EFO:BOLT (" + taskID + "): Connect failed (2), waiting and trying again");
						logger.info("+EFO:BOLT (" + taskID + "): " + e.getMessage());
						try
						{
							Thread.sleep(500);
						}
						catch(InterruptedException ie){
							ie.printStackTrace();
						}
					}
				}
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + 
						this.taskIP + ") Connection established...");
				try {
					ObjectOutputStream _stateOutput = new ObjectOutputStream(client.getOutputStream());
					ObjectInputStream _stateInput = new ObjectInputStream(client.getInputStream());
					List<Values> newState = (List<Values>) _stateInput.readObject();
					operator.mergeState(operator.getOutputSchema(), newState);
					_stateOutput.writeObject("+EFO_ACK");
					_stateOutput.flush();
					_stateInput.close();
					_stateOutput.close();
					client.close();
				} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
						") received state from about-to-be-removed node successfully...");
			}
		}
		zooPet.resetSubmittedScaleFlag();
	}

	public List<Values> getStateValue() {
		operator.getStateValues();
		return stateValues;
	}

	public void printState() {
		List<Values> state = operator.getStateValues();
		logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + ") printState() :");
		Iterator<Values> itr = state.iterator();
		while(itr.hasNext()) {
			Values val = itr.next();
			logger.info("<" + val.toString() + ">");
		}
		logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + ") concluded printState() :");
	}

}