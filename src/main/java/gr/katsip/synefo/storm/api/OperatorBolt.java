package gr.katsip.synefo.storm.api;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.operators.AbstractOperator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
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

	private ArrayList<String> downstreamTasks;

	private ArrayList<Integer> intDownstreamTasks;

	private TaskStatistics statistics;

	private AbstractOperator operator;

	private List<Values> stateValues;
	
	private int downStreamIndex;

	private int reportCounter;

	public OperatorBolt(String taskName, AbstractOperator operator, 
			ArrayList<String> downstreamTasks, ArrayList<Integer> intDownstreamTasks) {
		this.taskName = taskName;
		this.operator = operator;
		stateValues = new ArrayList<Values>();
		this.operator.init(stateValues);
		reportCounter = 0;
		this.downstreamTasks = new ArrayList<String>(downstreamTasks);
		this.intDownstreamTasks = new ArrayList<Integer>(intDownstreamTasks);
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

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
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
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> producerSchema = new ArrayList<String>();
		producerSchema.add("OPERATOR_TIMESTAMP");
		producerSchema.addAll(operator.getOutputSchema().toList());
		declarer.declare(new Fields(producerSchema));
	}

}
