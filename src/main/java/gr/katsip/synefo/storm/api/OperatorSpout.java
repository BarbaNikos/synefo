package gr.katsip.synefo.storm.api;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.lib.SynefoMessage.Type;
import gr.katsip.synefo.storm.producers.AbstractTupleProducer;
import gr.katsip.synefo.utils.SynefoConstant;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class OperatorSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5401244472233520294L;

	Logger logger = LoggerFactory.getLogger(OperatorSpout.class);

	private String taskName;

	private SpoutOutputCollector _collector;

	private int taskId;

	private String taskIP;

	private ArrayList<String> downstreamTasks = null;

	private ArrayList<Integer> intDownstreamTasks = null;

	private int idx;

	private String synefoIP;

	private Integer synefoPort;

	private AbstractTupleProducer tupleProducer;

	private TaskStatistics stats;

	private int reportCounter;
	
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

	public OperatorSpout(String taskName, String synefoIP, Integer synefoPort, 
			AbstractTupleProducer tupleProducer) {
		this.taskName = taskName;
		downstreamTasks = null;
		this.synefoIP = synefoIP;
		this.synefoPort = synefoPort;
		stats = new TaskStatistics();
		this.tupleProducer = tupleProducer;
		reportCounter = 0;
		opLatencySendState = OpLatencyState.na;
		opLatencySendTimestamp = 0L;
	}

	@Override
	public void nextTuple() {
		if(intDownstreamTasks != null && intDownstreamTasks.size() > 0) {
			Values values = new Values();
			/**
			 * Add OPERATOR_HEADER value in the beginning
			 */
			values.add((new Long(System.currentTimeMillis())).toString());
			Values returnedValues = tupleProducer.nextTuple();
			if(returnedValues != null) {
				for(int i = 0; i < returnedValues.size(); i++) {
					values.add(returnedValues.get(i));
				}
				_collector.emitDirect(intDownstreamTasks.get(idx), values);
				if(idx >= (intDownstreamTasks.size() - 1)) {
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
			for(Integer d_task : intDownstreamTasks) {
				_collector.emitDirect(d_task, v);
			}
		}else if(opLatencySendState.equals(OpLatencyState.s_2) && Math.abs(currentTimestamp - opLatencySendTimestamp) >= 1000) {
			this.opLatencySendState = OpLatencyState.na;
			this.opLatencySendTimestamp = currentTimestamp;
			Values v = new Values();
			v.add(SynefoConstant.OP_LATENCY_METRIC + ":" + OpLatencyState.s_3.toString() + ":" + opLatencySendTimestamp);
			for(int i = 0; i < tupleProducer.getSchema().size(); i++) {
				v.add(null);
			}
			for(Integer d_task : intDownstreamTasks) {
				_collector.emitDirect(d_task, v);
			}
		}
		if(reportCounter >= 10000) {
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
				OutputStream out = timeClient.getOutputStream();
				InputStream in = timeClient.getInputStream();
				byte[] buffer = new byte[8];
				Long receivedTimestamp = (long) 0;
				if(in.read(buffer) == 8) {
					ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
					receivedTimestamp = byteBuffer.getLong();
				}
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
				for(int task : intDownstreamTasks) {
					_collector.emitDirect(task, latencyTuple);
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
				for(Integer d_task : intDownstreamTasks) {
					_collector.emitDirect(d_task, v);
				}
			}
		}else {
			reportCounter += 1;
		}
	}

	@SuppressWarnings("unchecked")
	private void retrieveDownstreamTasks() {
		ArrayList<String> activeDownstreamTasks = null;
		ArrayList<Integer> intActiveDownstreamTasks = null;
		Socket socket;
		ObjectOutputStream output = null;
		ObjectInputStream input = null;
		logger.info("+OPERATOR-SPOUT (" + taskName + ":" + taskId + "@" + taskIP + 
				") in retrieveDownstreamTasks().");
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
			ArrayList<String> _downstream = null;
			_downstream = (ArrayList<String>) input.readObject();
			if(_downstream != null && _downstream.size() > 0) {
				downstreamTasks = new ArrayList<String>(_downstream);
				intDownstreamTasks = new ArrayList<Integer>();
				for(String task : downstreamTasks) {
					String[] downTask = task.split("[:@]");
					intDownstreamTasks.add(Integer.parseInt(downTask[1]));
				}
			}else {
				downstreamTasks = new ArrayList<String>();
				intDownstreamTasks = new ArrayList<Integer>();
			}
			ArrayList<String> _active_downstream = null;
			_active_downstream = (ArrayList<String>) input.readObject();
			if(_active_downstream != null && _active_downstream.size() > 0) {
				activeDownstreamTasks = new ArrayList<String>(_active_downstream);
				intActiveDownstreamTasks = new ArrayList<Integer>();
				for(String task : activeDownstreamTasks) {
					String[] downTask = task.split("[:@]");
					intActiveDownstreamTasks.add(Integer.parseInt(downTask[1]));
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
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		taskId = context.getThisTaskId();
		try {
			taskIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		if(downstreamTasks == null || intDownstreamTasks == null) {
			retrieveDownstreamTasks();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> producerSchema = new ArrayList<String>();
		producerSchema.add("OPERATOR_HEADER");
		producerSchema.addAll(tupleProducer.getSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

}
