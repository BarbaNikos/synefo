package gr.katsip.synefo.storm.api;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.lib.SynefoMessage.Type;
import gr.katsip.synefo.storm.operators.AbstractOperator;
import gr.katsip.synefo.storm.operators.AbstractStatOperator;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class OperatorBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7291921309382367329L;

	Logger logger = LoggerFactory.getLogger(OperatorBolt.class);

	private String taskName;

	private int taskId;

	private String taskIP;

	private OutputCollector collector;

	private ArrayList<String> downstreamTasks = null;

	private ArrayList<Integer> intDownstreamTasks = null;

	private TaskStatistics statistics;

	private AbstractOperator operator;

	private List<Values> stateValues;

	private int downStreamIndex;

	private int reportCounter;

	private String synefoServerIP = null;

	private Integer synefoServerPort = -1;

	private boolean statOperatorFlag;

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

	private OpLatencyState opLatencyReceiveState;

	private long[] opLatencyReceivedTimestamp = new long[3];

	private long[] opLatencyLocalTimestamp = new long[3];

	private long opLatencySendTimestamp;

	public OperatorBolt(String taskName, String synEFO_ip, Integer synEFO_port, 
			AbstractOperator operator) {
		this.taskName = taskName;
		this.operator = operator;
		stateValues = new ArrayList<Values>();
		this.operator.init(stateValues);
		reportCounter = 0;
		this.downstreamTasks = null;
		this.intDownstreamTasks = null;
		synefoServerIP = synEFO_ip;
		synefoServerPort = synEFO_port;
		statistics = new TaskStatistics();
		opLatencyReceiveState = OpLatencyState.na;
		opLatencySendState = OpLatencyState.na;
		opLatencySendTimestamp = 0L;
		opLatencyReceivedTimestamp = new long[3];
		opLatencyLocalTimestamp = new long[3];
		if(operator instanceof AbstractStatOperator)
			statOperatorFlag = true;
		else
			statOperatorFlag = false;
	}

	@Override
	public void execute(Tuple tuple) {
		boolean queryLatencyFlag = false;
		String operatorHeader = tuple.getString(tuple.fieldIndex("OPERATOR_HEADER"));
		Long operatorTimestamp = null;
		if(operatorHeader != null && operatorHeader.equals("") == false) {
			if(operatorHeader.contains(SynefoConstant.QUERY_LATENCY_METRIC) == true) {
				queryLatencyFlag = true;
				String[] tokens = operatorHeader.split(":");
				operatorTimestamp = Long.parseLong(tokens[1]);
				if(intDownstreamTasks != null && intDownstreamTasks.size() > 0) {
					Values v = new Values();
					v.add(operatorHeader);
					for(int i = 0; i < operator.getOutputSchema().size(); i++) {
						v.add(null);
					}
					for(Integer d_task : intDownstreamTasks) {
						collector.emitDirect(d_task, v);
					}
					//					logger.info("OPERATOR-BOLT (" + taskName + ":" + taskId + "@" + taskIP + 
					//							") just forwarded a QUERY-LATENCY-TUPLE.");
					collector.ack(tuple);
					return;
				}
			}else if(operatorHeader.contains(SynefoConstant.OP_LATENCY_METRIC)) {
				String[] tokens = operatorHeader.split(":");
				String opLatState = tokens[1];
				Long opLatTs = Long.parseLong(tokens[2]);
				if(opLatencyReceiveState.equals(OpLatencyState.na) && opLatState.equals(OpLatencyState.s_1.toString())) {
					this.opLatencyReceivedTimestamp[0] = opLatTs;
					this.opLatencyLocalTimestamp[0] = System.currentTimeMillis();
					opLatencyReceiveState = OpLatencyState.r_1;
				}else if(opLatencyReceiveState.equals(OpLatencyState.r_1) && opLatState.equals(OpLatencyState.s_2.toString())) {
					this.opLatencyReceivedTimestamp[1] = opLatTs;
					this.opLatencyLocalTimestamp[1] = System.currentTimeMillis();
					opLatencyReceiveState = OpLatencyState.r_2;
				}else if(opLatencyReceiveState.equals(OpLatencyState.r_2) && opLatState.equals(OpLatencyState.s_3.toString())) {
					this.opLatencyReceivedTimestamp[2] = opLatTs;
					this.opLatencyLocalTimestamp[2] = System.currentTimeMillis();
					opLatencyReceiveState = OpLatencyState.r_3;
					long latency = -1;
					/**
					 * Calculate latency
					 */
					latency = ( 
							Math.abs(this.opLatencyLocalTimestamp[2] - this.opLatencyLocalTimestamp[1] - 1000 - (this.opLatencyReceivedTimestamp[2] - this.opLatencyReceivedTimestamp[1] - 1000)) + 
							Math.abs(this.opLatencyLocalTimestamp[1] - this.opLatencyLocalTimestamp[0] - 1000 - (this.opLatencyReceivedTimestamp[1] - this.opLatencyReceivedTimestamp[0] - 1000))
							) / 2;
					statistics.updateWindowLatency(latency);
					this.opLatencyReceiveState = OpLatencyState.na;
					opLatencyLocalTimestamp = new long[3];
					opLatencyReceivedTimestamp = new long[3];
					//					logger.info("OPERATOR-BOLT (" + this.taskName + ":" + taskId + "@" + 
					//							this.taskIP + ") calculated OPERATOR-LATENCY-METRIC: " + latency + ".");
				}
				collector.ack(tuple);
				return;
			}else {
				operatorTimestamp = Long.parseLong(operatorHeader);
			}
		}

		Values produced_values = null;
		Values values = new Values(tuple.getValues().toArray());
		values.remove(0);
		List<String> fieldList = tuple.getFields().toList();
		fieldList.remove(0);
		Fields fields = new Fields(fieldList);
		if(intDownstreamTasks != null && intDownstreamTasks.size() > 0) {
			List<Values> returnedTuples = null;
			if(statOperatorFlag)
				returnedTuples = ((AbstractStatOperator) operator).execute(statistics, fields, values);
			else
				returnedTuples = operator.execute(fields, values);
			if(returnedTuples != null && returnedTuples.size() > 0) {
				for(Values v : returnedTuples) {
					produced_values = new Values();
					produced_values.add((new Long(System.currentTimeMillis())).toString());
					for(int i = 0; i < v.size(); i++) {
						produced_values.add(v.get(i));
					}
					collector.emitDirect(intDownstreamTasks.get(downStreamIndex), produced_values);
				}
				if(downStreamIndex >= (intDownstreamTasks.size() - 1)) {
					downStreamIndex = 0;
				}else {
					downStreamIndex += 1;
				}
			}
			statistics.updateSelectivity(( (double) returnedTuples.size() / 1.0));
			collector.ack(tuple);
		}else {
			if(queryLatencyFlag == true) {
				long latency = -1;
				try {
					Socket timeClient = new Socket(synefoServerIP, 5556);
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
					latency = Math.abs(receivedTimestamp - operatorTimestamp);
					logger.info("OPERATOR-BOLT (" + this.taskName + ":" + taskId + "@" + 
							this.taskIP + ") calculated PER-QUERY-LATENCY: " + latency + " (msec)");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}else {
				List<Values> returnedTuples = null;
				if(statOperatorFlag)
					returnedTuples = ((AbstractStatOperator) operator).execute(statistics, fields, values);
				else
					returnedTuples = operator.execute(fields, values);
				if(returnedTuples != null && returnedTuples.size() > 0) {
					for(Values v : returnedTuples) {
						produced_values = new Values();
						produced_values.add(Long.toString(System.currentTimeMillis()));
						for(int i = 0; i < v.size(); i++) {
							produced_values.add(v.get(i));
						}
						logger.info("OPERATOR-BOLT (" + this.taskName + ":" + taskId + "@" + 
								this.taskIP + ") emits: " + produced_values);
					}
				}
				statistics.updateSelectivity(( (double) returnedTuples.size() / 1.0));
			}
			collector.ack(tuple);
		}
		statistics.updateMemory();
		statistics.updateCpuLoad();
		statistics.updateWindowThroughput();
		/**
		 * Part where additional timestamps are sent for operator-latency metric
		 */
		long currentTimestamp = System.currentTimeMillis();
		if(opLatencySendState.equals(OpLatencyState.s_1) && Math.abs(currentTimestamp - opLatencySendTimestamp) >= 1000) {
			this.opLatencySendState = OpLatencyState.s_2;
			this.opLatencySendTimestamp = currentTimestamp;
			Values v = new Values();
			v.add(SynefoConstant.OP_LATENCY_METRIC + ":" + OpLatencyState.s_2.toString() + ":" + opLatencySendTimestamp);
			for(int i = 0; i < operator.getOutputSchema().size(); i++) {
				v.add(null);
			}
			for(Integer d_task : intDownstreamTasks) {
				collector.emitDirect(d_task, v);
			}
		}else if(opLatencySendState.equals(OpLatencyState.s_2) && Math.abs(currentTimestamp - opLatencySendTimestamp) >= 1000) {
			this.opLatencySendState = OpLatencyState.na;
			this.opLatencySendTimestamp = currentTimestamp;
			Values v = new Values();
			v.add(SynefoConstant.OP_LATENCY_METRIC + ":" + OpLatencyState.s_3.toString() + ":" + opLatencySendTimestamp);
			for(int i = 0; i < operator.getOutputSchema().size(); i++) {
				v.add(null);
			}
			for(Integer d_task : intDownstreamTasks) {
				collector.emitDirect(d_task, v);
			}
		}

		if(reportCounter >= 500) {
			if(statOperatorFlag == false)
				logger.info("OPERATOR-BOLT (" + this.taskName + ":" + this.taskId + "@" + this.taskIP + 
						") timestamp: " + System.currentTimeMillis() + ", " + 
						"cpu: " + statistics.getCpuLoad() + 
						", memory: " + statistics.getMemory() + 
						", latency: " + statistics.getWindowLatency() + 
						", throughput: " + statistics.getWindowThroughput());
			reportCounter = 0;
			/**
			 * Initiate operator latency metric sequence
			 */
			if(opLatencySendState.equals(OpLatencyState.na)) {
				this.opLatencySendState = OpLatencyState.s_1;
				this.opLatencySendTimestamp = System.currentTimeMillis();
				Values v = new Values();
				v.add(SynefoConstant.OP_LATENCY_METRIC + ":" + OpLatencyState.s_1.toString() + 
						":" + opLatencySendTimestamp);
				for(int i = 0; i < operator.getOutputSchema().size(); i++) {
					v.add(null);
				}
				for(Integer d_task : intDownstreamTasks) {
					collector.emitDirect(d_task, v);
				}
			}
		}else {
			reportCounter += 1;
		}
	}

	@SuppressWarnings("unchecked")
	private void retrieveDownstreamTasks() {
		ArrayList<String> activeDownstreamTasks;
		ArrayList<Integer> intActiveDownstreamTasks;
		Socket socket;
		ObjectOutputStream _output;
		ObjectInputStream _input;
		logger.info("+OPERATOR-BOLT (" + taskName + ":" + taskId + "@" + taskIP + ") in retrieveDownstreamTasks().");
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
		msg._values.put("TASK_ID", Integer.toString(taskId));
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
					String[] downTask = itr.next().split("[:@]");
					Integer task = Integer.parseInt(downTask[1]);
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
					String[] downTask = itr.next().split("[:@]");
					Integer task = Integer.parseInt(downTask[1]);
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
		 * Updating operator name for saving the statistics data in the 
		 * database server accordingly
		 */
		if(statOperatorFlag == true)
			((AbstractStatOperator) operator).updateOperatorName(
					taskName + ":" + taskId + "@" + taskIP);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.downStreamIndex = 0;
		taskId = context.getThisTaskId();
		if(conf.containsKey("TOPOLOGY_DEBUG") || conf.containsKey("topology_debug")) {
			String debug = (String) conf.get("TOPOLOGY_DEBUG");
			logger.info("OPERATOR-BOLT (" + taskName + ":" + taskId + "@" + taskIP + ") topology debug flag: " + debug);
		}
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
		producerSchema.addAll(operator.getOutputSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

}
