package gr.katsip.synefo.storm.api;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.lib.SynefoMessage.Type;
import gr.katsip.synefo.storm.producers.AbstractStatTupleProducer;
import gr.katsip.synefo.storm.producers.AbstractTupleProducer;
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
 */
public class SynefoSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7244170192535254357L;

	Logger logger = LoggerFactory.getLogger(SynefoSpout.class);

	private static final String stormHome = "/opt/apache-storm-0.9.4/logs/";

	private AsynchronousFileChannel statisticFileChannel = null;

	private CompletionHandler<Integer, Object> statisticFileHandler = null;

	private AsynchronousFileChannel scaleEventFileChannel = null;

	private CompletionHandler<Integer, Object> scaleEventFileHandler = null;

	private Long statisticFileOffset = 0L;

	private Long scaleEventFileOffset = 0L;

	private static final int statReportPeriod = 5000;

	private static final int latencySequencePeriod = 250;

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

	private int latencyPeriodCounter;

	private boolean statTupleProducerFlag;

	private enum OpLatencyState {
		na,
		s_1,
		s_2,
		s_3,
		r_1,
		r_2,
		r_3
	}

	private OpLatencyState opLatencySendState;

	private long opLatencySendTimestamp;
	
	private int sequenceNumber;

	public SynefoSpout(String task_name, String synEFO_ip, Integer synEFO_port, 
			AbstractTupleProducer tupleProducer, String zooIP) {
		taskName = task_name;
		workerPort = -1;
		downstreamTasks = null;
		activeDownstreamTasks = null;
		synefoIP = synEFO_ip;
		synefoPort = synEFO_port;
		stats = new TaskStatistics();
		this.tupleProducer = tupleProducer;
		this.zooIP = zooIP;
		reportCounter = 0;
		latencyPeriodCounter = 0;
		opLatencySendState = OpLatencyState.na;
		opLatencySendTimestamp = 0L;
		if(tupleProducer instanceof AbstractStatTupleProducer)
			statTupleProducerFlag = true;
		else
			statTupleProducerFlag = false;
		sequenceNumber = 0;
	}

	/**
	 * The function for registering to Synefo server
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
		logger.info("+EFO-SPOUT (" + 
				taskName + ":" + taskId + "@" + taskIP + 
				") registered to +EFO successfully (timestamp: " + 
				System.currentTimeMillis() + ").");
		/**
		 * Updating operator name for saving the statistics data 
		 * in the database server accordingly
		 */
		if(statTupleProducerFlag == true)
			((AbstractStatTupleProducer) tupleProducer).updateProducerName(
					taskName + ":" + taskId + "@" + taskIP);
		sequenceNumber = 0;
	}

	public void initiateLatencyMonitor() {
		opLatencySendState = OpLatencyState.s_1;
		this.opLatencySendTimestamp = System.currentTimeMillis();
		Values v = new Values();
		v.add(SynefoConstant.OP_LATENCY_METRIC + "-" + taskId + "#" + sequenceNumber + ":" + 
				OpLatencyState.s_1.toString() + ":" + opLatencySendTimestamp);
		for(int i = 0; i < tupleProducer.getSchema().size(); i++) {
			v.add(null);
		}
		for(Integer d_task : intActiveDownstreamTasks) {
			collector.emitDirect(d_task, v);
		}
//		String logLine = System.currentTimeMillis() + " initiating sequence number: " + sequenceNumber + ", op-state: " + opLatencySendState + "\n";
//		byte[] buffer = logLine.getBytes();
//		if(this.scaleEventFileChannel != null && this.scaleEventFileHandler != null) {
//			scaleEventFileChannel.write(
//					ByteBuffer.wrap(buffer), this.scaleEventFileOffset, "stat write", scaleEventFileHandler);
//			scaleEventFileOffset += buffer.length;
//		}
	}

	public void progressLatencySequence(long currentTimestamp) {
		Values latencyMetricTuple = new Values();
		if(opLatencySendState.equals(OpLatencyState.s_1) && 
				Math.abs(currentTimestamp - opLatencySendTimestamp) >= 1000) {
			this.opLatencySendState = OpLatencyState.s_2;
			this.opLatencySendTimestamp = currentTimestamp;
			latencyMetricTuple.add(SynefoConstant.OP_LATENCY_METRIC + "-" + taskId + "#" + sequenceNumber + ":" + 
					OpLatencyState.s_2.toString() + ":" + opLatencySendTimestamp);
			for(int i = 0; i < tupleProducer.getSchema().size(); i++) {
				latencyMetricTuple.add(null);
			}
			for(Integer d_task : intActiveDownstreamTasks) {
				collector.emitDirect(d_task, latencyMetricTuple);
			}
		}else if(opLatencySendState.equals(OpLatencyState.s_2) && 
				Math.abs(currentTimestamp - opLatencySendTimestamp) >= 1000) {
			this.opLatencySendTimestamp = currentTimestamp;
			latencyMetricTuple.add(SynefoConstant.OP_LATENCY_METRIC + "-" + taskId + "#" + sequenceNumber + ":" + 
					OpLatencyState.s_3.toString() + ":" + opLatencySendTimestamp);
			for(int i = 0; i < tupleProducer.getSchema().size(); i++) {
				latencyMetricTuple.add(null);
			}
			for(Integer d_task : intActiveDownstreamTasks) {
				collector.emitDirect(d_task, latencyMetricTuple);
			}
			this.opLatencySendState = OpLatencyState.na;
			this.latencyPeriodCounter = 0;
			sequenceNumber += 1;
		}
	}

	public void nextTuple() {
		/**
		 * In this execution names are sent in a round-robin 
		 * fashion to all downstream tasks. The words array is 
		 * iterated from start to beginning.
		 */
		if(intActiveDownstreamTasks != null && intActiveDownstreamTasks.size() > 0) {
			Values values = new Values();
			/**
			 * Add SYNEFO_HEADER (SYNEFO_TIMESTAMP) value in the beginning
			 */
			values.add((new Long(System.currentTimeMillis())).toString());
			Values returnedValues = null;
			if(statTupleProducerFlag == true)
				returnedValues = ((AbstractStatTupleProducer) tupleProducer).nextTuple(stats);
			else
				returnedValues = tupleProducer.nextTuple();
			if(returnedValues != null) {
				for(int i = 0; i < returnedValues.size(); i++) {
					values.add(returnedValues.get(i));
				}
				collector.emitDirect(intActiveDownstreamTasks.get(idx), values);
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
		long currentTimestamp = System.currentTimeMillis();
		progressLatencySequence(currentTimestamp);
		/**
		 * Initiation of operator latency sequence
		 */
		if(latencyPeriodCounter >= SynefoSpout.latencySequencePeriod && 
				opLatencySendState == OpLatencyState.na) {
			initiateLatencyMonitor();
		}else {
			latencyPeriodCounter += 1;
		}
		/**
		 * END of Initiation of operator latency sequence
		 */
		if(reportCounter >= SynefoSpout.statReportPeriod) {
			if(statTupleProducerFlag == false) {
				byte[] buffer = (System.currentTimeMillis() + "," + stats.getCpuLoad() + "," + 
						stats.getMemory() + "," + stats.getWindowLatency() + "," + 
						stats.getWindowThroughput() + "\n").toString().getBytes();
				if(this.statisticFileChannel != null && this.statisticFileHandler != null) {
					statisticFileChannel.write(
							ByteBuffer.wrap(buffer), this.statisticFileOffset, "stat write", statisticFileHandler);
					statisticFileOffset += buffer.length;
				}
			}
			reportCounter = 0;
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
			byte[] buffer = ("timestamp: " + System.currentTimeMillis() + "," + action + "~" + task + ":" + task_id + "\n").toString().getBytes();
			if(this.scaleEventFileChannel != null && this.scaleEventFileHandler != null) {
				scaleEventFileChannel.write(
						ByteBuffer.wrap(buffer), this.scaleEventFileOffset, "stat write", scaleEventFileHandler);
				scaleEventFileOffset += buffer.length;
			}
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
			/**
			 * Re-initialize Operator-latency metrics
			 */
			opLatencySendState = OpLatencyState.na;
			opLatencySendTimestamp = 0L;
			reportCounter = 0;
			latencyPeriodCounter = 0;
		}
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
		if(this.statisticFileChannel == null) {
			try {
				File f = new File(stormHome + 
						taskName + ":" + taskId + "@" + taskIP + "-stats.log");
				if(f.exists() == false)
					statisticFileChannel = AsynchronousFileChannel.open(Paths.get(stormHome + 
							taskName + ":" + taskId + "@" + taskIP + "-stats.log"), 
							StandardOpenOption.WRITE, StandardOpenOption.CREATE);
				else {
					statisticFileChannel = AsynchronousFileChannel.open(Paths.get(stormHome + 
							taskName + ":" + taskId + "@" + taskIP + "-stats.log"), 
							StandardOpenOption.WRITE);
					this.statisticFileOffset = statisticFileChannel.size();
					byte[] buffer = (System.currentTimeMillis() + "," + "STATS-EXIST\n").toString().getBytes();
					if(this.statisticFileChannel != null && this.statisticFileHandler != null) {
						statisticFileChannel.write(
								ByteBuffer.wrap(buffer), this.statisticFileOffset, "stat write", statisticFileHandler);
						statisticFileOffset += buffer.length;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			statisticFileHandler = new CompletionHandler<Integer, Object>() {
				@Override
				public void completed(Integer result, Object attachment) {}
				@Override
				public void failed(Throwable exc, Object attachment) {}
			};
			statisticFileOffset = 0L;
		}
		if(this.scaleEventFileChannel == null) {
			try {
				File f = new File(stormHome + 
						taskName + ":" + taskId + "@" + taskIP + "-scale-events.log");
				if(f.exists() == false)
					scaleEventFileChannel = AsynchronousFileChannel.open(Paths.get(stormHome + 
							taskName + ":" + taskId + "@" + taskIP + "-scale-events.log"), 
							StandardOpenOption.WRITE, StandardOpenOption.CREATE);
				else {
					scaleEventFileChannel = AsynchronousFileChannel.open(Paths.get(stormHome + 
							taskName + ":" + taskId + "@" + taskIP + "-scale-events.log"), 
							StandardOpenOption.WRITE);
					this.scaleEventFileOffset = scaleEventFileChannel.size();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			scaleEventFileHandler = new CompletionHandler<Integer, Object>() {
				@Override
				public void completed(Integer result, Object attachment) {}
				@Override
				public void failed(Throwable exc, Object attachment) {}
			};
			scaleEventFileOffset = 0L;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> producerSchema = new ArrayList<String>();
		producerSchema.add("SYNEFO_HEADER");
		producerSchema.addAll(tupleProducer.getSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

}