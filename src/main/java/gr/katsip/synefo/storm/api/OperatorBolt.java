package gr.katsip.synefo.storm.api;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.lib.SynefoMessage.Type;
import gr.katsip.synefo.storm.operators.AbstractOperator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

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
	}

	@Override
	public void execute(Tuple tuple) {
		Long operatorTimestamp = tuple.getLong(tuple.getFields()
				.fieldIndex("OPERATOR_TIMESTAMP"));
		Values produced_values = null;
		Values values = new Values(tuple.getValues().toArray());
		values.remove(tuple.getFields().fieldIndex("OPERATOR_TIMESTAMP"));
		List<String> fieldList = tuple.getFields().toList();
		fieldList.remove(0);
		Fields fields = new Fields(fieldList);
		if(intDownstreamTasks != null && intDownstreamTasks.size() > 0) {
			List<Values> returnedTuples = operator.execute(fields, values);
			for(Values v : returnedTuples) {
				produced_values = new Values();
				produced_values.add(new Long(System.currentTimeMillis()));
				for(int i = 0; i < v.size(); i++) {
					produced_values.add(v.get(i));
				}
				collector.emitDirect(intDownstreamTasks.get(downStreamIndex), produced_values);
			}
			collector.ack(tuple);
			if(downStreamIndex >= (intDownstreamTasks.size() - 1)) {
				downStreamIndex = 0;
			}else {
				downStreamIndex += 1;
			}
		}else {
			List<Values> returnedTuples = operator.execute(fields, values);
			for(Values v : returnedTuples) {
				produced_values = new Values();
				produced_values.add(new Long(System.currentTimeMillis()));
				for(int i = 0; i < v.size(); i++) {
					produced_values.add(v.get(i));
				}
				logger.info("OPERATOR-BOLT (" + this.taskName + ":" + this.taskId + "@" + 
						this.taskIP + ") emits: " + produced_values);
			}
			collector.ack(tuple);
		}
		statistics.updateMemory();
		statistics.updateCpuLoad();
		if(operatorTimestamp != null) {
			long latency = System.currentTimeMillis() - operatorTimestamp;
			statistics.updateLatency(latency);
		}else {
			statistics.updateLatency();
		}
		statistics.updateThroughput(1);
		
		if(reportCounter >= 1000) {
			logger.info("OPERATOR-BOLT (" + this.taskName + ":" + this.taskId + "@" + this.taskIP + 
					") timestamp: " + System.currentTimeMillis() + ", " + 
					"cpu: " + statistics.getCpuLoad() + 
					", memory: " + statistics.getMemory() + 
					", latency: " + statistics.getLatency() + 
					", throughput: " + statistics.getThroughput());
			reportCounter = 0;
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
		producerSchema.add("OPERATOR_TIMESTAMP");
		producerSchema.addAll(operator.getOutputSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

}
