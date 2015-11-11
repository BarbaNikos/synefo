package gr.katsip.deprecated;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.deprecated.crypstream.DataCollector;

/**
 * @deprecated
 * @param <T>
 */
public class StatFilterOperator<T> implements AbstractStatOperator, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4077505930209677317L;

	private Comparator<T> comparator;

	private T value;

	private String fieldName;

	private Fields stateSchema;
	
	private List<Values> stateValues;

	private Fields outputSchema;
	
	private int statReportPeriod;

	private DataCollector dataSender = null;

	private String zooConnectionInfo;
	
	private String operatorName = null;
	
	public StatFilterOperator(Comparator<T> comparator, String fieldName, T value, 
			String zooConnectionInfo, int statReportPeriod, String operatorName) {
		this.comparator = comparator;
		this.value = value;
		this.fieldName = fieldName;
		this.statReportPeriod = statReportPeriod;
		this.zooConnectionInfo = zooConnectionInfo;
		dataSender = null;
		this.operatorName = operatorName;
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
		this.outputSchema = new Fields(outputSchema.toList());
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		if(dataSender == null) {
			String[] zooTokens = this.zooConnectionInfo.split(":");
			dataSender = new DataCollector(zooTokens[0], Integer.parseInt(zooTokens[1]), 
					statReportPeriod, this.operatorName);
		}
		List<Values> returnTuples = new ArrayList<Values>();
		@SuppressWarnings("unchecked")
		T tValue = (T) values.get(fields.fieldIndex(fieldName));
		if(comparator.compare(value, tValue) == 0) {
			Values newValues = new Values(values.toArray());
			returnTuples.add(newValues);
			return returnTuples;
		}
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
		//Nothing to do since no state is kept
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
		@SuppressWarnings("unchecked")
		T tValue = (T) values.get(fields.fieldIndex(fieldName));
		if(comparator.compare(value, tValue) == 0) {
			Values newValues = new Values(values.toArray());
			returnTuples.add(newValues);
			return returnTuples;
		}
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
