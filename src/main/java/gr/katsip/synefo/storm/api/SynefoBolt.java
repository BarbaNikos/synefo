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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.lib.SynefoMessage.Type;
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
public class SynefoBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4011052074675303959L;

	Logger logger = LoggerFactory.getLogger(SynefoBolt.class);

	private String taskName;

	private int downStreamIndex;

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

	private AbstractOperator operator;

	private List<Values> stateValues;

	private ZooPet zooPet;

	private String zooIP;

	private Integer zooPort;

	private int reportCounter;

	private boolean autoScale;

	public SynefoBolt(String task_name, String synEFO_ip, Integer synEFO_port, 
			AbstractOperator operator, String zooIP, Integer zooPort, boolean autoScale) {
		taskName = task_name;
		synefoServerIP = synEFO_ip;
		synefoServerPort = synEFO_port;
		downstreamTasks = null;
		intDownstreamTasks = null;
		activeDownstreamTasks = null;
		intActiveDownstreamTasks = null;
		statistics = new TaskStatistics();
		this.operator = operator;
		stateValues = new ArrayList<Values>();
		this.operator.init(stateValues);
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		reportCounter = 0;
		this.autoScale = autoScale;
	}

	/**
	 * The function for registering to SynEFO server
	 */
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
		msg._values.put("TASK_TYPE", "BOLT");
		msg._values.put("TASK_NAME", taskName);
		msg._values.put("TASK_ID", Integer.toString(taskID));
		try {
			socket = new Socket(synefoServerIP, synefoServerPort);
			output = new ObjectOutputStream(socket.getOutputStream());
			input = new ObjectInputStream(socket.getInputStream());
			output.writeObject(msg);
			output.flush();
			msg = null;
			ArrayList<String> downstream = null;
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
		zooPet.start();
		zooPet.getScaleCommand();
		logger.info("+EFO-BOLT (" + 
				taskName + ":" + taskID + 
				") registered to synEFO successfully, timestamp: " + System.currentTimeMillis() + ".");
	}

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		taskID = context.getThisTaskId();
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		zooPet = new ZooPet(zooIP, zooPort, taskName, taskID, taskIP);
		if(downstreamTasks == null && activeDownstreamTasks == null)
			registerToSynEFO();
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
		Long synefoTimestamp = null;
		if(synefoHeader != null && synefoHeader.equals("") == false && synefoHeader.contains("/") == true) {
			if(synefoHeader.contains("/")) {
				String[] headerFields = synefoHeader.split("/");
				if(headerFields[0].equals(SynefoConstant.PUNCT_TUPLE_TAG)) {
					handlePunctuationTuple(tuple);
					return;
				}
			}else {
				synefoTimestamp = Long.parseLong(synefoHeader);
			}
		}
		/**
		 * Remove from both values and fields SYNEFO_HEADER (SYNEFO_TIMESTAMP)
		 */
		Values produced_values = null;
		Values values = new Values(tuple.getValues().toArray());
		values.remove(0);
		List<String> fieldList = tuple.getFields().toList();
		fieldList.remove(0);
		Fields fields = new Fields(fieldList);
		if(intActiveDownstreamTasks != null && intActiveDownstreamTasks.size() > 0) {
			List<Values> returnedTuples = operator.execute(fields, values);
			for(Values v : returnedTuples) {
				produced_values = new Values();
				produced_values.add((new Long(System.currentTimeMillis())).toString());
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
				produced_values.add((new Long(System.currentTimeMillis())).toString());
				for(int i = 0; i < v.size(); i++) {
					produced_values.add(v.get(i));
				}
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + 
						this.taskIP + ") emits: " + produced_values);
			}
			collector.ack(tuple);
		}
		statistics.updateMemory();
		statistics.updateCpuLoad();
		long latency = -1;
		if(synefoTimestamp != null) {
			long currentTimestamp = System.currentTimeMillis();
			latency = currentTimestamp - synefoTimestamp;
			if(latency < 0) {
				latency = Math.abs(latency);
			}
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
//					", latency: " + statistics.getLatency() + 
					", latency: " + latency + 
					", throughput: " + statistics.getThroughput());
			reportCounter = 0;
		}else {
			reportCounter += 1;
		}
//		if(autoScale)
//			zooPet.setLatency(statistics.getLatency());
		if(autoScale)
			zooPet.setThroughput(statistics.getThroughput());
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
			downStreamIndex = 0;
			if(action.toLowerCase().contains("activate") || action.toLowerCase().contains("deactivate")) {
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + ") located scale-command: " + 
						scaleCommand + ", about to update routing tables (timestamp: " + System.currentTimeMillis() + ").");
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
						scaleCommand + ", updated routing tables: " + intActiveDownstreamTasks + 
						"(timestamp: " + System.currentTimeMillis() + ").");
			}else {
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + ") located scale-command: " + 
						scaleCommand + ", about to produce punctuation tuple (timestamp: " + System.currentTimeMillis() + ").");
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
		producerSchema.addAll(operator.getOutputSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

	@SuppressWarnings("unchecked")
	public void handlePunctuationTuple(Tuple tuple) {
		String scaleAction = null;
		String componentName = null;
		String componentId = null;
		Integer compNum = -1;
		String ip = null;
		logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
				") received punctuation tuple: " + tuple.toString() + "(timestamp: " + System.currentTimeMillis() + ").");
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
		/**
		 * 
		 */
		if(scaleAction != null && scaleAction.equals(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.ADD_ACTION)) {
			System.out.println("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
					") received an ADD action (timestamp: " + System.currentTimeMillis() + ").");
			String selfComp = this.taskName + ":" + this.taskID;
			if(selfComp.equals(componentName + ":" + componentId)) {
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
					while(numOfStatesReceived < (compNum - 1)) {
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
								String[] downTask = itr.next().split("[:@]");
								Integer task = Integer.parseInt(downTask[1]);
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
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
						") Finished accepting connections to receive state (timestamp: " + System.currentTimeMillis() + ").");
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + ") routing table:" + 
						this.activeDownstreamTasks + " (timestamp: " + System.currentTimeMillis() + ").");
			}else {
				Socket client = new Socket();
				Integer comp_task_id = Integer.parseInt(componentId);
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
							Thread.sleep(100);
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
						") sent state to newly added node successfully (timestamp: " + System.currentTimeMillis() + ").");
			}
		}else if(scaleAction != null && scaleAction.equals(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.REMOVE_ACTION)) {
			logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
					") received a REMOVE action (timestamp: " + System.currentTimeMillis() + ").");
			String selfComp = this.taskName + ":" + this.taskID;
			if(selfComp.equals(componentName + ":" + componentId)) {
				try {
					ServerSocket _socket = new ServerSocket(6000 + taskID);
					logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
							") accepting connections to receive state... (IP:" + 
							_socket.getInetAddress() + ", port: " + _socket.getLocalPort());
					int numOfStatesReceived = 0;
					while(numOfStatesReceived < (compNum - 1)) {
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
						") Finished accepting connections to send state (timestamp: " + 
						System.currentTimeMillis() + ").");
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
						") routing table:" + this.activeDownstreamTasks + " (timestamp: " + System.currentTimeMillis() + ").");
			}else {
				Socket client = new Socket();
				Integer comp_task_id = Integer.parseInt(componentId);
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + this.taskIP + 
						") about to receive state from about-to-be-removed operator (IP: " + ip + 
						", port: " + (6000 + comp_task_id) + ").");
				boolean attempt_flag = true;
				while (attempt_flag == true) {
					try {
						client = new Socket(ip, 6000 + comp_task_id);
						attempt_flag = false;
					} catch (IOException e) {
						logger.info("+EFO-BOLT (" + taskID + "): Connect failed (2), waiting and trying again");
						logger.info("+EFO-BOLT (" + taskID + "): " + e.getMessage());
						try
						{
							Thread.sleep(100);
						}
						catch(InterruptedException ie){
							ie.printStackTrace();
						}
					}
				}
				logger.info("+EFO-BOLT (" + this.taskName + ":" + this.taskID + "@" + 
						this.taskIP + ") Connection established (timestamp: " + System.currentTimeMillis() + ").");
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
						") received state from about-to-be-removed node successfully (timestamp: " + System.currentTimeMillis() + ").");
			}
		}
		zooPet.resetSubmittedScaleFlag();
	}

}