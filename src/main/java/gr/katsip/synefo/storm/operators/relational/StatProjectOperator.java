package gr.katsip.synefo.storm.operators.relational;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.operators.AbstractStatOperator;
import gr.katsip.synefo.storm.operators.crypstream.DataCollector;

public class StatProjectOperator implements AbstractStatOperator, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 754765785493018351L;

	private List<Values> stateValues;

	private Fields stateSchema;

	private Fields outputSchema;

	private Fields projectedAttributes;
	
	private int statReportPeriod;

	private DataCollector dataSender = null;

	private String zooConnectionInfo;
	
	private String operatorName = null;
	
	public StatProjectOperator(Fields projectedAttributes, String zooConnectionInfo, int statReportPeriod) {
		this.projectedAttributes = new Fields(projectedAttributes.toList());
		this.statReportPeriod = statReportPeriod;
		this.zooConnectionInfo = zooConnectionInfo;
		dataSender = null;
		this.operatorName = null;
	}

	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		this.stateSchema = new Fields(stateSchema.toList());
	}

	@Override
	public void setOutputSchema(Fields output_schema) {
		this.outputSchema = new Fields(output_schema.toList());
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		if(dataSender == null) {
			String[] zooTokens = this.zooConnectionInfo.split(":");
			dataSender = new DataCollector(zooTokens[0], Integer.parseInt(zooTokens[1]), 
					statReportPeriod, this.operatorName);
		}
		List<Values> returnTuples = new ArrayList<Values>();
		Iterator<String> itr = projectedAttributes.iterator();
		Values projected_values = new Values();
		while(itr.hasNext()) {
			String field = itr.next();
			projected_values.add(values.get(fields.fieldIndex(field)));
		}
		returnTuples.add(projected_values);
		updateData(null);
		return returnTuples;
	}

	@Override
	public List<Values> getStateValues() {
		return stateValues;
	}

	@Override
	public Fields getStateSchema() {
		return stateSchema;
	}

	@Override
	public Fields getOutputSchema() {
		return outputSchema;
	}

	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		//Stateless operator
	}

	@Override
	public List<Values> execute(TaskStatistics statistics, Fields fields,
			Values values) {
		if(dataSender == null) {
			String[] zooTokens = this.zooConnectionInfo.split(":");
			dataSender = new DataCollector(zooTokens[0], Integer.parseInt(zooTokens[1]), 
					statReportPeriod, this.operatorName);
		}
		List<Values> returnTuples = new ArrayList<Values>();
		Iterator<String> itr = projectedAttributes.iterator();
		Values projected_values = new Values();
		while(itr.hasNext()) {
			String field = itr.next();
			projected_values.add(values.get(fields.fieldIndex(field)));
		}
		returnTuples.add(projected_values);
		updateData(statistics);
		return returnTuples;
	}

	@Override
	public void updateOperatorName(String operatorName) {
		this.operatorName = operatorName;
	}

	@Override
	public void reportStatisticBeforeScaleOut() {
		float CPU = (float) 0.0;
		float memory = (float) 0.0;
		int latency = 0;
		int throughput = 0;
		float sel = (float) 0.0;
		String tuple = CPU + "," + memory + "," + latency + "," + 
				throughput + "," + sel + ",0,0,0,0,0";
		if(dataSender == null) {
			String[] zooTokens = this.zooConnectionInfo.split(":");
			dataSender = new DataCollector(zooTokens[0], Integer.parseInt(zooTokens[1]), 
					statReportPeriod, this.operatorName);
		}
		dataSender.pushStatisticData(tuple.getBytes());
	}

	@Override
	public void reportStatisticBeforeScaleIn() {
		float CPU = (float) 0.0;
		float memory = (float) 0.0;
		int latency = 0;
		int throughput = 0;
		float sel = (float) 0.0;
		String tuple = CPU + "," + memory + "," + latency + "," + 
				throughput + "," + sel + ",0,0,0,0,0";
		if(dataSender == null) {
			String[] zooTokens = this.zooConnectionInfo.split(":");
			dataSender = new DataCollector(zooTokens[0], Integer.parseInt(zooTokens[1]), 
					statReportPeriod, this.operatorName);
		}
		dataSender.pushStatisticData(tuple.getBytes());
	}
	
	public void updateData(TaskStatistics stats) {
		float CPU = (float) 0.0;
		float memory = (float) 0.0;
		int latency = 0;
		int throughput = 0;
		float sel = (float) 0.0;
		if(stats != null) {
			String tuple = 	(float) stats.getCpuLoad() + "," + (float) stats.getMemory() + "," + 
					(int) stats.getWindowLatency() + "," + (int) stats.getWindowThroughput() + "," + 
					(float) stats.getSelectivity() + ",0,0,0,0,0";
			dataSender.addToBuffer(tuple);
		}else {
			String tuple = CPU + "," + memory + "," + latency + "," + 
					throughput + "," + sel + ",0,0,0,0,0";
			dataSender.addToBuffer(tuple);
		}
	}

}
