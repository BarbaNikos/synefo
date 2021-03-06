package gr.katsip.deprecated.deprecated;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import backtype.storm.metric.api.AssignableMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.synefo.utils.SynefoMessage.Type;
import gr.katsip.synefo.utils.SynefoConstant;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 
 * @author Nick R. Katsipoulakis
 *
 * @deprecated
 */
public class SynefoSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7244170192535254357L;

	Logger logger = LoggerFactory.getLogger(SynefoSpout.class);

	private static final int METRIC_FREQ_SEC = 5;

	private static final int statReportPeriod = 1000;

	private String taskName;

	private Integer workerPort;

	private SpoutOutputCollector collector;

	private int taskId;

	private String taskIP;

	private ArrayList<String> downstreamTasks = null;

	private ArrayList<Integer> intDownstreamTasks = null;

	private ArrayList<String> activeDownstreamTasks = null;

	private ArrayList<Integer> intActiveDownstreamTasks = null;

	private int idx;

	private String synefoIP;

	private Integer synefoPort;

	private TaskStatistics stats;

	private AbstractTupleProducer tupleProducer;

	transient private ZooPet pet;

	private String zooIP;

	private int reportCounter;

	private AssignableMetric completeLatency;

	private HashMap<Values, Long> tupleStatistics;

	public SynefoSpout(String taskName, String synefoIpAddress, Integer synefoPort,
			AbstractTupleProducer tupleProducer, String zooIP) {
		this.taskName = taskName;
		workerPort = -1;
		downstreamTasks = null;
		activeDownstreamTasks = null;
		synefoIP = synefoIpAddress;
		this.synefoPort = synefoPort;
		stats = new TaskStatistics(statReportPeriod);
		this.tupleProducer = tupleProducer;
		this.zooIP = zooIP;
		reportCounter = 0;
	}

	/**
	 * The function for registering to BalanceServer server
	 */
	@SuppressWarnings("unchecked")
	public void registerToSynEFO() {
		Socket socket;
		ObjectOutputStream output = null;
		ObjectInputStream input = null;
		socket = null;
		SynefoMessage msg = new SynefoMessage();
		msg._type = Type.REG;
		msg._values.put("TASK_TYPE", "SPOUT");
		msg._values.put("TASK_NAME", taskName);
		msg._values.put("WORKER_PORT", Integer.toString(workerPort));
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
			ArrayList<String> downstream = null;
			downstream = (ArrayList<String>) input.readObject();
			if(downstream != null && downstream.size() > 0) {
				downstreamTasks = new ArrayList<String>(downstream);
				intDownstreamTasks = new ArrayList<Integer>();
				for(String task : downstreamTasks) {
					String[] tokens = task.split("[:@]");
					intDownstreamTasks.add(Integer.parseInt(tokens[1]));
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
				for(String task : activeDownstreamTasks) {
					String[] tokens = task.split("[:@]");
					intActiveDownstreamTasks.add(Integer.parseInt(tokens[1]));
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
			output.flush();
			output.close();
			input.close();
			socket.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (EOFException e) {
            logger.info("+EFO-SPOUT (" +
                    taskName + ":" + taskId +
                    ") just threw an exception: " + e.getMessage());
        } catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
            logger.info("+EFO-SPOUT (" +
                    taskName + ":" + taskId +
                    ") just threw an exception: " + e.getMessage());
		}
        /**
         * Handshake with ZooKeeper
         */
		pet.start();
		pet.getScaleCommand();
		logger.info("+EFO-SPOUT (" +
				taskName + ":" + taskId + "@" + taskIP +
				") registered to +EFO (timestamp: " +
				System.currentTimeMillis() + ").");
	}

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
					 SpoutOutputCollector collector) {
		this.collector = collector;
		taskId = context.getThisTaskId();
		workerPort = context.getThisWorkerPort();
		/**
		 * Update taskName with task-id so that multi-core is supported
		 */
		taskName = taskName + "_" + taskId;
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		pet = new ZooPet(zooIP, taskName, taskId, taskIP);
		if(activeDownstreamTasks == null && downstreamTasks == null) {
			registerToSynEFO();
		}
		initMetrics(context);
	}

	private void initMetrics(TopologyContext context) {
		completeLatency = new AssignableMetric(null);
		context.registerMetric("comp-latency", completeLatency, SynefoSpout.METRIC_FREQ_SEC);
		tupleStatistics = new HashMap<Values, Long>();
	}

	public void ack(Object msgId) {
		Long currentTimestamp = System.currentTimeMillis();
		Values values = (Values) msgId;
		if(tupleStatistics.containsKey(values)) {
			Long emitTimestamp = tupleStatistics.remove(values);
			completeLatency.setValue((currentTimestamp - emitTimestamp));
		}
	}

	public void nextTuple() {
		/**
		 * In this execution names are sent in a round-robin 
		 * fashion to all downstream tasks. The words array is 
		 * iterated from start to beginning.
		 */
		Long currentTimestamp = System.currentTimeMillis();
		if(intActiveDownstreamTasks != null && intActiveDownstreamTasks.size() > 0) {
			Values values = new Values();
			/**
			 * Add SYNEFO_HEADER (SYNEFO_TIMESTAMP) value in the beginning
			 */
			values.add(currentTimestamp.toString());
			Values returnedValues = null;
			returnedValues = tupleProducer.nextTuple();
			if(returnedValues != null) {
				for(int i = 0; i < returnedValues.size(); i++) {
					values.add(returnedValues.get(i));
				}
				collector.emitDirect(intActiveDownstreamTasks.get(idx), values, values);
				tupleStatistics.put(values, System.currentTimeMillis());
				if(idx >= (intActiveDownstreamTasks.size() - 1)) {
					idx = 0;
				}else {
					idx += 1;
				}
			}
		}
		stats.updateMemory();
		stats.updateCpuLoad();
		stats.updateWindowThroughput();
		
		if(reportCounter >= SynefoSpout.statReportPeriod) {
			reportCounter = 0;
			this.stats = new TaskStatistics(statReportPeriod);
		}else {
			reportCounter += 1;
		}
		String scaleCommand = "";
		synchronized(pet) {
			if(pet.pendingCommands.isEmpty() == false) {
				scaleCommand = pet.returnScaleCommand();
			}
		}
		if(scaleCommand != null && scaleCommand.length() > 0) {
			String[] scaleCommandTokens = scaleCommand.split("[~:@]");
			String action = scaleCommandTokens[0];
			String taskWithIp = scaleCommandTokens[1] + ":" + scaleCommandTokens[2] + "@" + scaleCommandTokens[3];
			String taskIp = scaleCommandTokens[3];
			String task = scaleCommandTokens[1];
			Integer task_id = Integer.parseInt(scaleCommandTokens[2]);
			StringBuilder strBuild = new StringBuilder();
			strBuild.append(SynefoConstant.PUNCT_TUPLE_TAG + "/");
			idx = 0;
			if(action.toLowerCase().contains("activate") || action.toLowerCase().contains("deactivate")) {
				logger.info("+EFO-SPOUT (" + this.taskName + ":" + this.taskId + "@" + this.taskIP + 
						") located scale-command: " + scaleCommand + ", about to update routing tables (timestamp: " + 
						System.currentTimeMillis() + ").");
				logger.info("+EFO-SPOUT (" + this.taskName + ":" + this.taskId + "@" + this.taskIP + 
						") active downstream tasks list: " + activeDownstreamTasks.toString());
				if(action.toLowerCase().equals("activate")) {
					activeDownstreamTasks.add(taskWithIp);
					intActiveDownstreamTasks.add(task_id);
				}else if(action.toLowerCase().equals("deactivate")) {
					activeDownstreamTasks.remove(activeDownstreamTasks.indexOf(taskWithIp));
					intActiveDownstreamTasks.remove(intActiveDownstreamTasks.indexOf(task_id));
				}
				logger.info("+EFO-SPOUT (" + this.taskName + ":" + this.taskId + "@" + this.taskIP + 
						") active downstream tasks list after activating/deactivating node: " + activeDownstreamTasks.toString());
			}else {
				logger.info("+EFO-SPOUT (" + this.taskName + ":" + this.taskId + "@" + this.taskIP + 
						") located scale-command: " + scaleCommand + ", about to produce punctuation tuple (timestamp: " + 
						System.currentTimeMillis() + ").");
				if(action.toLowerCase().contains("add")) {
					activeDownstreamTasks.add(taskWithIp);
					intActiveDownstreamTasks.add(task_id);
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
				for(int i = 0; i < tupleProducer.getSchema().size(); i++) {
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
				logger.info("+EFO-SPOUT (" + this.taskName + ":" + this.taskId + "@" + this.taskIP + 
						") active downstream tasks list after adding/removing node: " + activeDownstreamTasks.toString());
			}
			reportCounter = 0;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> producerSchema = new ArrayList<String>();
		producerSchema.add("SYNEFO_HEADER");
		producerSchema.addAll(tupleProducer.getSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

}