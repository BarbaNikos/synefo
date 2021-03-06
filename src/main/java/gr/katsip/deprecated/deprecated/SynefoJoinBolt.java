package gr.katsip.deprecated.deprecated;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.synefo.utils.SynefoMessage.Type;
import gr.katsip.deprecated.AbstractJoinOperator;
import gr.katsip.synefo.utils.SynefoConstant;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @deprecated
 */
public class SynefoJoinBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2276600254438802773L;

	private static final int TICK_TUPLE_FREQ_SEC = 10;

    private static final int METRIC_REPORT_FREQ_SEC = 15;

    private static final int WARM_UP_THRESHOLD = 10000;

    Logger logger = LoggerFactory.getLogger(SynefoJoinBolt.class);

    private static final int statReportPeriod = 1000;

	private String taskName;
	
	private Integer workerPort;

	private OutputCollector collector;

	private ArrayList<String> downstreamTasks;

	private ArrayList<Integer> intDownstreamTasks;

	private ArrayList<String> activeDownstreamTasks;

	private ArrayList<Integer> intActiveDownstreamTasks;

	private String synefoServerIP = null;

	private int taskID = -1;

	private String taskIP;

	private Integer synefoServerPort = -1;

	private TaskStatistics statistics;

	private AbstractJoinOperator operator;

	private List<Values> stateValues;

	private ZooPet zooPet;

	private String zooIP;

//	private boolean autoScale;

	private boolean warmFlag;

    private int tupleCounter;

	private Integer downStreamIndex;
	
	private HashMap<String, ArrayList<String>> relationTaskIndex;

	private HashMap<String, ArrayList<Integer>> intRelationTaskIndex;
	
	private long latestSynefoTimestamp;
	
	private transient AssignableMetric latencyMetric;

//    private transient AssignableMetric latencyDetailsMetric;

	private transient AssignableMetric throughputMetric;

	private transient AssignableMetric executeLatencyMetric;

	private transient AssignableMetric stateSizeMetric;

	public SynefoJoinBolt(String task_name, String synEFO_ip, Integer synEFO_port, 
			AbstractJoinOperator operator, String zooIP, boolean autoScale) {
		taskName = task_name;
		workerPort = -1;
		synefoServerIP = synEFO_ip;
		synefoServerPort = synEFO_port;
		downstreamTasks = null;
		intDownstreamTasks = null;
		activeDownstreamTasks = null;
		intActiveDownstreamTasks = null;
		this.operator = operator;
		stateValues = new ArrayList<Values>();
		this.operator.init(stateValues);
		this.zooIP = zooIP;
//		this.autoScale = autoScale;
		warmFlag = false;
		relationTaskIndex = null;
		intRelationTaskIndex = null;
		latestSynefoTimestamp = -1;
	}

	@SuppressWarnings("unchecked")
	public void registerToSynEFO() {
		Socket socket;
		ObjectOutputStream output;
		ObjectInputStream input;
		socket = null;
		SynefoMessage msg = new SynefoMessage();
		msg._type = Type.REG;
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
			msg._values.put("TASK_IP", taskIP);
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		msg._values.put("TASK_TYPE", "JOIN_BOLT");
		msg._values.put("JOIN_STEP", operator.operatorStep());
		msg._values.put("JOIN_RELATION", operator.relationStorage());
		msg._values.put("TASK_NAME", taskName);
		msg._values.put("TASK_ID", Integer.toString(taskID));
		msg._values.put("WORKER_PORT", Integer.toString(workerPort));
		try {
			socket = new Socket(synefoServerIP, synefoServerPort);
			output = new ObjectOutputStream(socket.getOutputStream());
			input = new ObjectInputStream(socket.getInputStream());
			output.writeObject(msg);
			output.flush();
			msg = null;
			ArrayList<String> downstream = null;
			logger.info("+EFO-JOIN-BOLT (" + 
					taskName + ":" + taskID + 
					") about to receive information from BalanceServer.");
			downstream = (ArrayList<String>) input.readObject();
			if(downstream != null && downstream.size() > 0) {
				downstreamTasks = new ArrayList<String>(downstream);
				intDownstreamTasks = new ArrayList<Integer>();
				Iterator<String> itr = downstreamTasks.iterator();
				while(itr.hasNext()) {
					String[] tokens = itr.next().split("[:@]");
					Integer task = Integer.parseInt(tokens[1]);
					intDownstreamTasks.add(task);
				}
			}else {
				downstreamTasks = new ArrayList<String>();
				intDownstreamTasks = new ArrayList<Integer>();
			}
			ArrayList<String> activeDownstream = null;
			activeDownstream = (ArrayList<String>) input.readObject();
			if(activeDownstream != null && activeDownstream.size() > 0) {
				activeDownstreamTasks = new ArrayList<String>(activeDownstream);
				intActiveDownstreamTasks = new ArrayList<Integer>();
				Iterator<String> itr = activeDownstreamTasks.iterator();
				while(itr.hasNext()) {
					String[] tokens = itr.next().split("[:@]");
					Integer task = Integer.parseInt(tokens[1]);
					intActiveDownstreamTasks.add(task);
				}
			}else {
				activeDownstreamTasks = new ArrayList<String>();
				intActiveDownstreamTasks = new ArrayList<Integer>();
			}
			/**
			 * If a dispatch step, read downstream relation mappings
			 */
			if(operator.operatorStep().equals("DISPATCH")) {
				relationTaskIndex = (HashMap<String, ArrayList<String>>) input.readObject();
				intRelationTaskIndex = new HashMap<String, ArrayList<Integer>>();
				Iterator<Entry<String, ArrayList<String>>> itr = relationTaskIndex.entrySet().iterator();
				while(itr.hasNext()) {
					Entry<String, ArrayList<String>> pair = itr.next();
					String relation = pair.getKey();
					ArrayList<Integer> identifiers = new ArrayList<Integer>();
					for(String task : pair.getValue()) {
						Integer identifier = Integer.parseInt(task.split("[:@]")[1]);
						identifiers.add(identifier);
					}
					intRelationTaskIndex.put(relation, identifiers);
				}
				output.writeObject(new String("OK"));
				output.flush();
			}
			/**
			 * Closing channels of communication with 
			 * SynEFO server
			 */
			output.close();
			input.close();
			socket.close();
		} catch (EOFException e) {
            logger.info("+EFO-JOIN-BOLT (" +
                    taskName + ":" + taskID +
                    ") just threw an exception: " + e.getMessage());
        } catch (IOException
                | ClassNotFoundException e) {
			logger.info("+EFO-JOIN-BOLT (" + 
					taskName + ":" + taskID + 
					") just threw an exception: " + e.getMessage());
			e.printStackTrace();
		} catch (NullPointerException e) {
            logger.info("+EFO-JOIN-BOLT (" +
                    taskName + ":" + taskID +
                    ") just threw an exception: " + e.getMessage());
        }
        /**
         * Handshake with ZooKeeper
         */
		zooPet.start();
		zooPet.getScaleCommand();
		StringBuilder strBuild = new StringBuilder();
		strBuild.append("+EFO-JOIN-BOLT (" + taskName + ":" + taskID + ") list of active tasks: ");
		for(String activeTask : activeDownstreamTasks) {
			strBuild.append(activeTask + " ");
		}
		logger.info(strBuild.toString());
		logger.info("+EFO-JOIN-BOLT (" + 
				taskName + ":" + taskID + 
				") registered to synEFO successfully, timestamp: " + System.currentTimeMillis() + ".");
		this.downStreamIndex = new Integer(0);
		latestSynefoTimestamp = -1;
	}

	private void initMetrics(TopologyContext context) {
		latencyMetric = new AssignableMetric(null);
		context.registerMetric("latency", latencyMetric, SynefoJoinBolt.METRIC_REPORT_FREQ_SEC);
//        latencyDetailsMetric = new AssignableMetric(null);
//        context.registerMetric("latency-details", latencyDetailsMetric, SynefoJoinBolt.METRIC_REPORT_FREQ_SEC);
        throughputMetric = new AssignableMetric(null);
		executeLatencyMetric = new AssignableMetric(null);
		stateSizeMetric = new AssignableMetric(null);
		context.registerMetric("execute-latency", executeLatencyMetric, SynefoJoinBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("throughput", throughputMetric, SynefoJoinBolt.METRIC_REPORT_FREQ_SEC);
		context.registerMetric("state-size", stateSizeMetric, SynefoJoinBolt.METRIC_REPORT_FREQ_SEC);
        statistics = new TaskStatistics(statReportPeriod);
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		taskID = context.getThisTaskId();
		workerPort = context.getThisWorkerPort();
		/**
		 * Update the taskName and extend it with the task-id (support for multi-core)
		 */
		taskName = taskName + "_" + taskID;
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		zooPet = new ZooPet(zooIP, taskName, taskID, taskIP);
		if(downstreamTasks == null && activeDownstreamTasks == null)
			registerToSynEFO();
		initMetrics(context);
        warmFlag = false;
        tupleCounter = 0;
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, SynefoJoinBolt.TICK_TUPLE_FREQ_SEC);
		return conf;
	}

	private boolean isTickTuple(Tuple tuple) {
		String sourceComponent = tuple.getSourceComponent();
		String sourceStreamId = tuple.getSourceStreamId();
		return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID) && 
				sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	public void execute(Tuple tuple) {
		Long currentTimestamp = System.currentTimeMillis();
		String synefoHeader = "";
		Long synefoTimestamp = null;
		/**
		 * Check if it is a tick tuple
		 */
		if(isTickTuple(tuple) && warmFlag) {
			/**
			 * Check latency
			 */
			if(latestSynefoTimestamp == -1) {
				latestSynefoTimestamp = currentTimestamp;
			}else {
				Long timeDifference = currentTimestamp - latestSynefoTimestamp;
//                String latencySpecifics = latestSynefoTimestamp + "-" + currentTimestamp + "-" + timeDifference;
				latestSynefoTimestamp = currentTimestamp;
				if(timeDifference >= (SynefoJoinBolt.TICK_TUPLE_FREQ_SEC * 1000)) {
					statistics.updateWindowLatency((timeDifference - (SynefoJoinBolt.TICK_TUPLE_FREQ_SEC * 1000)));
                    latencyMetric.setValue(( timeDifference - (SynefoJoinBolt.TICK_TUPLE_FREQ_SEC * 1000) ));
//                    latencyDetailsMetric.setValue(latencySpecifics);
				}
			}
			collector.ack(tuple);
			return;
		}else if(isTickTuple(tuple) && !warmFlag) {
			collector.ack(tuple);
			return;
		}
		/**
		 * If punctuation tuple is received:
		 * Perform Share of state and return execution
		 */
		if(tuple.getFields().contains("SYNEFO_HEADER") == false) {
			logger.error("+EFO-JOIN-BOLT (" + taskName + ":" + taskID + 
					") received tuple without a SYNEFO_HEADER from task: " + tuple.getSourceTask());
			collector.ack(tuple);
			return;
		}else {
			synefoHeader = tuple.getString(tuple.getFields().fieldIndex("SYNEFO_HEADER"));
			if(synefoHeader != null && synefoHeader.equals("") == false) {
				if(synefoHeader.contains("/") && synefoHeader.contains(SynefoConstant.PUNCT_TUPLE_TAG) == true 
						&& synefoHeader.contains(SynefoConstant.ACTION_PREFIX) == true
						&& synefoHeader.contains(SynefoConstant.COMP_IP_TAG) == true) {
					String[] headerFields = synefoHeader.split("/");
					if(headerFields[0].equals(SynefoConstant.PUNCT_TUPLE_TAG)) {
						handlePunctuationTuple(currentTimestamp, tuple);
						collector.ack(tuple);
						return;
					}
				}else {
					synefoTimestamp = Long.parseLong(synefoHeader);
				}
			}
		}
		/**
		 * Remove from both values and fields SYNEFO_HEADER (SYNEFO_TIMESTAMP)
		 */
		Values values = new Values(tuple.getValues().toArray());
		values.remove(0);
		List<String> fieldList = tuple.getFields().toList();
		fieldList.remove(0);
		Fields fields = new Fields(fieldList);
		if(intActiveDownstreamTasks != null && intActiveDownstreamTasks.size() > 0) {
			/**
			 * Need to provide also the tupleTimestamp
			 */
			downStreamIndex = operator.execute(tuple, collector, intRelationTaskIndex, intActiveDownstreamTasks,
					downStreamIndex, fields, values, synefoTimestamp);
			collector.ack(tuple);
		}else {
			downStreamIndex = operator.execute(tuple, collector, intRelationTaskIndex, intActiveDownstreamTasks,
					downStreamIndex, fields, values, synefoTimestamp);
			collector.ack(tuple);
		}
		Long executeEndTimestamp = System.currentTimeMillis();
		executeLatencyMetric.setValue((executeEndTimestamp - currentTimestamp));
		statistics.updateMemory();
		statistics.updateCpuLoad();
		statistics.updateWindowThroughput();
		throughputMetric.setValue(statistics.getWindowThroughput());
		stateSizeMetric.setValue(operator.getStateSize());

        tupleCounter += 1;
        if(tupleCounter >= WARM_UP_THRESHOLD && !warmFlag)
            warmFlag = true;
//		if(autoScale && warmFlag == true)
//			zooPet.setLatency(statistics.getWindowLatency());
		String scaleCommand = "";
		synchronized(zooPet) {
			if(zooPet.pendingCommands.isEmpty() == false) {
				scaleCommand = zooPet.returnScaleCommand();
			}
		}
		if(scaleCommand != null && scaleCommand.length() > 0) {
			String[] scaleCommandTokens = scaleCommand.split("[~@:]");
			String action = scaleCommandTokens[0];
			String taskWithIp = scaleCommandTokens[1] + ":" + scaleCommandTokens[2] + "@" + scaleCommandTokens[3];
			String taskIp = scaleCommandTokens[3];
			String task = scaleCommandTokens[1];
			Integer task_id = Integer.parseInt(scaleCommandTokens[2]);
			StringBuilder strBuild = new StringBuilder();
			strBuild.append(SynefoConstant.PUNCT_TUPLE_TAG + "/");
			downStreamIndex = new Integer(0);
			if(action.toLowerCase().contains("activate") || action.toLowerCase().contains("deactivate")) {
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + ") located scale-command: " +
						scaleCommand + ", about to update routing tables (timestamp: " + System.currentTimeMillis() + ").");
				if(action.toLowerCase().equals("activate")) {
					if(activeDownstreamTasks.contains(taskWithIp) == false && intActiveDownstreamTasks.contains(task_id) == false) {
						if(activeDownstreamTasks.indexOf(taskWithIp) < 0)
							activeDownstreamTasks.add(taskWithIp);
						if(intActiveDownstreamTasks.indexOf(task_id) < 0)
							intActiveDownstreamTasks.add(task_id);
					}
				}else if(action.toLowerCase().equals("deactivate")) {
					if(activeDownstreamTasks.indexOf(taskWithIp) >= 0)
						activeDownstreamTasks.remove(activeDownstreamTasks.indexOf(taskWithIp));
					if(intActiveDownstreamTasks.indexOf(task_id) >= 0)
						intActiveDownstreamTasks.remove(intActiveDownstreamTasks.indexOf(task_id));
				}
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + ") located scale-command: " +
						scaleCommand + ", updated routing tables: " + intActiveDownstreamTasks + 
						"(timestamp: " + System.currentTimeMillis() + ").");
			}else {
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + ") located scale-command: " +
						scaleCommand + ", about to produce punctuation tuple (timestamp: " + System.currentTimeMillis() + ").");
				if(action.toLowerCase().contains("add")) {
					if(activeDownstreamTasks.contains(taskWithIp) == false && intActiveDownstreamTasks.contains(task_id) == false) {
						if(activeDownstreamTasks.indexOf(taskWithIp) < 0)
							activeDownstreamTasks.add(taskWithIp);
						if(intActiveDownstreamTasks.indexOf(task_id) < 0)
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
			/**
			 * Re-initialize Operator-latency metrics
			 */
			latestSynefoTimestamp = -1;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> producerSchema = new ArrayList<String>();
		producerSchema.add("SYNEFO_HEADER");
		producerSchema.addAll(operator.getOutputSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

	@SuppressWarnings("unchecked")
	public void handlePunctuationTuple(long currentTimestamp, Tuple tuple) {
		String scaleAction = null;
		String componentName = null;
		String componentId = null;
		Integer compNum = -1;
		String ip = null;
		logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
				") received punctuation tuple: " + tuple.toString() + 
				"(timestamp: " + currentTimestamp + ").");
		/**
		 * Expected Header format: 
		 * +EFO/ACTION:{ADD, REMOVE}/COMP:{taskName}:{taskID}/COMP_NUM:{Number of Components}/IP:{taskIP}/
		 */
		String[] tokens = ((String) tuple.getValues().get(0)).split("[/:]");
		scaleAction = tokens[2];
		componentName = tokens[4];
		componentId = tokens[5];
		compNum = Integer.parseInt(tokens[7]);
		ip = tokens[9];
		if(warmFlag == true)
			warmFlag = false;
		logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
				") action: " + scaleAction + ".");
		if(scaleAction != null && scaleAction.equals(SynefoConstant.ADD_ACTION)) {
			logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
					") received an ADD action (timestamp: " + currentTimestamp + ").");
			String selfComp = this.taskName + ":" + this.taskID;
			/**
			 * If this Synefobolt is about to become Active
			 */
			if(selfComp.equals(componentName + ":" + componentId)) {
				/**
				 * If statistics are reported to a database, add a data-point 
				 * with zero statistics
				 */
                statistics = new TaskStatistics(statReportPeriod);
				try {
					ServerSocket _socket = new ServerSocket(6000 + taskID);
					int numOfStatesReceived = 0;
					logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
							") accepting connections to receive state... (IP:" + 
							_socket.getInetAddress().getHostAddress() + ", port: " + _socket.getLocalPort());
					boolean activeListFlag = false;
					while(numOfStatesReceived < (compNum - 1)) {
						Socket client = _socket.accept();
						ObjectOutputStream _stateOutput = new ObjectOutputStream(client.getOutputStream());
						ObjectInputStream _stateInput = new ObjectInputStream(client.getInputStream());
						Object responseObject = _stateInput.readObject();
						if(responseObject instanceof List) {
							List<Values> newState = (List<Values>) responseObject;
							operator.mergeState(operator.getOutputSchema(), newState);
						}
						if(activeListFlag == false) {
							_stateOutput.writeObject("+EFO_ACT_NODES");
							_stateOutput.flush();
							responseObject = _stateInput.readObject();
							if(responseObject instanceof List) {
								this.activeDownstreamTasks = (ArrayList<String>) responseObject;
								intActiveDownstreamTasks = new ArrayList<Integer>();
								Iterator<String> itr = activeDownstreamTasks.iterator();
								while(itr.hasNext()) {
									String[] downTask = itr.next().split("[:@]");
									Integer task = Integer.parseInt(downTask[1]);
									intActiveDownstreamTasks.add(task);
								}
								logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
										") received active downstream task list:" + activeDownstreamTasks + "(intActiveDownstreamTasks: " + 
										intActiveDownstreamTasks.toString() + ")");
								activeListFlag = true;
								downStreamIndex = 0;
							}
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
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
						") Finished accepting connections to receive state (timestamp: " + currentTimestamp + ").");
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + ") routing table:" +
						this.activeDownstreamTasks + " (timestamp: " + currentTimestamp + ").");
			}else {
				Socket client = new Socket();
				Integer comp_task_id = Integer.parseInt(componentId);
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
						") about to send state to about-to-be-added operator (IP: " + 
						ip + ", port: " + (6000 + comp_task_id) + ").");
				boolean attempt_flag = true;
				while (attempt_flag == true) {
					try {
						client = new Socket(ip, 6000 + comp_task_id);
						attempt_flag = false;
					} catch (IOException e) {
						logger.info("+EFO-JOIN-BOLT (" + taskID + "): Connect failed (1), waiting and trying again");
						logger.info("+EFO-JOIN-BOLT (" + taskID + "): " + e.getMessage());
						try
						{
							Thread.sleep(100);
						}
						catch(InterruptedException ie){
							ie.printStackTrace();
						}
					}
				}
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
						") Connection established...");
				try {
					ObjectOutputStream _stateOutput = new ObjectOutputStream(client.getOutputStream());
					ObjectInputStream _stateInput = new ObjectInputStream(client.getInputStream());
					_stateOutput.writeObject(operator.getStateValues());
					_stateOutput.flush();
					Object responseObject = _stateInput.readObject();
					if(responseObject instanceof String) {
						String response = (String) responseObject;
						if(response.equals("+EFO_ACT_NODES")) {
							_stateOutput.writeObject(this.activeDownstreamTasks);
							_stateOutput.flush();
						}
					}
					_stateInput.close();
					_stateOutput.close();
					client.close();
				} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
						") sent state to newly added node successfully (timestamp: " + currentTimestamp + ").");
			}
		}else if(scaleAction != null && scaleAction.equals(SynefoConstant.REMOVE_ACTION)) {
			logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
					") received a REMOVE action (timestamp: " + currentTimestamp + ").");
			String selfComp = this.taskName + ":" + this.taskID;
			if(selfComp.equals(componentName + ":" + componentId)) {
				/**
				 * If statistics are reported to a database, add a data-point 
				 * with zero statistics
				 */
                statistics = new TaskStatistics(statReportPeriod);
				try {
					ServerSocket _socket = new ServerSocket(6000 + taskID);
					logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
							") accepting connections to send state... (IP:" + 
							_socket.getInetAddress() + ", port: " + _socket.getLocalPort());
					int numOfStatesReceived = 0;
					while(numOfStatesReceived < (compNum - 1)) {
						Socket client = _socket.accept();
						ObjectOutputStream _stateOutput = new ObjectOutputStream(client.getOutputStream());
						ObjectInputStream _stateInput = new ObjectInputStream(client.getInputStream());
						_stateOutput.writeObject(operator.getStateValues());
						_stateOutput.flush();
						Object responseObject = _stateInput.readObject();
						if(responseObject instanceof String) {
							String response = (String) responseObject;
							if(response.equals("+EFO_ACK")) {
							}
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
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
						") Finished accepting connections to send state (timestamp: " + 
						currentTimestamp + ").");
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
						") routing table:" + this.activeDownstreamTasks + " (timestamp: " + currentTimestamp + ").");
			}else {
				Socket client = new Socket();
				Integer comp_task_id = Integer.parseInt(componentId);
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
						") about to receive state from about-to-be-removed operator (IP: " + ip + 
						", port: " + (6000 + comp_task_id) + ").");
				boolean attempt_flag = true;
				while (attempt_flag == true) {
					try {
						client = new Socket(ip, 6000 + comp_task_id);
						attempt_flag = false;
					} catch (IOException e) {
						logger.info("+EFO-JOIN-BOLT (" + taskID + "): Connect failed (2), waiting and trying again");
						logger.info("+EFO-JOIN-BOLT (" + taskID + "): " + e.getMessage());
						try
						{
							Thread.sleep(100);
						}
						catch(InterruptedException ie){
							ie.printStackTrace();
						}
					}
				}
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + 
						this.taskIP + ") Connection established (timestamp: " + currentTimestamp + ").");
				try {
					ObjectOutputStream _stateOutput = new ObjectOutputStream(client.getOutputStream());
					ObjectInputStream _stateInput = new ObjectInputStream(client.getInputStream());
					Object responseObject = _stateInput.readObject();
					if(responseObject instanceof List) {
						List<Values> newState = (List<Values>) responseObject;
						operator.mergeState(operator.getOutputSchema(), newState);
					}
					_stateOutput.writeObject("+EFO_ACK");
					_stateOutput.flush();
					_stateInput.close();
					_stateOutput.close();
					client.close();
				} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}
				logger.info("+EFO-JOIN-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP +
						") received state from about-to-be-removed node successfully (timestamp: " + currentTimestamp + ").");
			}
		}
//		zooPet.resetSubmittedScaleFlag();
		latestSynefoTimestamp = -1;
	}

}
