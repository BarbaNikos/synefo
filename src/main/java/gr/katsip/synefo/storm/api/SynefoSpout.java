package gr.katsip.synefo.storm.api;

import java.io.BufferedReader;
import java.io.IOException;
//import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
//import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
//import java.nio.ByteBuffer;
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

	private String taskName;

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

	private Integer zooPort;

	private int reportCounter;

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

	public SynefoSpout(String task_name, String synEFO_ip, Integer synEFO_port, 
			AbstractTupleProducer tupleProducer, String zooIP, Integer zooPort) {
		taskName = task_name;
		downstreamTasks = null;
		activeDownstreamTasks = null;
		synefoIP = synEFO_ip;
		synefoPort = synEFO_port;
		stats = new TaskStatistics();
		this.tupleProducer = tupleProducer;
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		reportCounter = 0;
		opLatencySendState = OpLatencyState.na;
		opLatencySendTimestamp = 0L;
		if(tupleProducer instanceof AbstractStatTupleProducer)
			statTupleProducerFlag = true;
		else
			statTupleProducerFlag = false;
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
		if(opLatencySendState.equals(OpLatencyState.s_1) && Math.abs(currentTimestamp - opLatencySendTimestamp) >= 1000) {
			this.opLatencySendState = OpLatencyState.s_2;
			this.opLatencySendTimestamp = currentTimestamp;
			Values v = new Values();
			v.add(SynefoConstant.OP_LATENCY_METRIC + ":" + OpLatencyState.s_2.toString() + ":" + opLatencySendTimestamp);
			for(int i = 0; i < tupleProducer.getSchema().size(); i++) {
				v.add(null);
			}
			for(Integer d_task : intActiveDownstreamTasks) {
				collector.emitDirect(d_task, v);
			}
		}else if(opLatencySendState.equals(OpLatencyState.s_2) && Math.abs(currentTimestamp - opLatencySendTimestamp) >= 1000) {
			this.opLatencySendState = OpLatencyState.na;
			this.opLatencySendTimestamp = currentTimestamp;
			Values v = new Values();
			v.add(SynefoConstant.OP_LATENCY_METRIC + ":" + OpLatencyState.s_3.toString() + ":" + opLatencySendTimestamp);
			for(int i = 0; i < tupleProducer.getSchema().size(); i++) {
				v.add(null);
			}
			for(Integer d_task : intActiveDownstreamTasks) {
				collector.emitDirect(d_task, v);
			}
		}
		if(reportCounter >= 500) {
			if(statTupleProducerFlag == false)
				logger.info("+EFO-SPOUT (" + this.taskName + ":" + this.taskId + "@" + this.taskIP + 
						") timestamp: " + System.currentTimeMillis() + ", " + 
						"cpu: " + stats.getCpuLoad() + 
						", memory: " + stats.getMemory() +  
						", input-rate: " + stats.getWindowThroughput());
			reportCounter = 0;
			/**
			 * Send out a QUERY_LATENCY_METRIC tuple to measure the latency per query
			 */
			try {
				Socket timeClient = new Socket(synefoIP, 5556);
//				OutputStream out = timeClient.getOutputStream();
				PrintWriter out = new PrintWriter(timeClient.getOutputStream(), true);
//				InputStream in = timeClient.getInputStream();
				BufferedReader in = new BufferedReader(new InputStreamReader(timeClient.getInputStream()));
//				byte[] buffer = new byte[8];
//				Long receivedTimestamp = (long) 0;
				Long receivedTimestamp = Long.parseLong(in.readLine());
//				if(in.read(buffer) == 8) {
//					ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
//					receivedTimestamp = byteBuffer.getLong();
//				}
//				receivedTimestamp = (long) 1L;
//				buffer = ByteBuffer.allocate(8).putLong(receivedTimestamp).array();
				out.println("OK");
//				out.write(buffer);
				out.flush();
				in.close();
				out.close();
				timeClient.close();
				Values latencyTuple = new Values();
				latencyTuple.add(new String(SynefoConstant.QUERY_LATENCY_METRIC + ":" + receivedTimestamp));
				for(int i = 0; i < tupleProducer.getSchema().size(); i++) {
					latencyTuple.add(null);
				}
				logger.info("+EFO-SPOUT (" + this.taskName + ":" + this.taskId + "@" + this.taskIP + 
						") about to emit query-latency tuple: " + latencyTuple.toString());
				for(int task : intActiveDownstreamTasks) {
					collector.emitDirect(task, latencyTuple);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			/**
			 * Initiate operator latency sequence
			 */
			if(opLatencySendState.equals(OpLatencyState.na)) {
				this.opLatencySendState = OpLatencyState.s_1;
				this.opLatencySendTimestamp = System.currentTimeMillis();
				Values v = new Values();
				v.add(SynefoConstant.OP_LATENCY_METRIC + ":" + OpLatencyState.s_1.toString() + ":" + opLatencySendTimestamp);
				for(int i = 0; i < tupleProducer.getSchema().size(); i++) {
					v.add(null);
				}
				for(Integer d_task : intActiveDownstreamTasks) {
					collector.emitDirect(d_task, v);
				}
			}
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
			/**
			 * Re-initialize Operator-latency metrics
			 */
			opLatencySendState = OpLatencyState.na;
			opLatencySendTimestamp = 0L;
		}
	}

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		taskId = context.getThisTaskId();
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		pet = new ZooPet(zooIP, zooPort, taskName, taskId, taskIP);
		if(activeDownstreamTasks == null && downstreamTasks == null) {
			registerToSynEFO();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> producerSchema = new ArrayList<String>();
		producerSchema.add("SYNEFO_HEADER");
		producerSchema.addAll(tupleProducer.getSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

}