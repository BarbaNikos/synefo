package gr.katsip.synefo.storm.operators.relational;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.operators.AbstractStatOperator;
import gr.katsip.synefo.storm.operators.crypstream.DataCollector;

public class StatCountGroupByOperator implements AbstractStatOperator, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3472221903501249100L;

	private List<Values> stateValues;

	private Fields stateSchema;

	private Fields outputSchema;

	private String[] groupByAttributes;

	private Integer window;
	
	private int statReportPeriod;

	private DataCollector dataSender = null;

	private String zooConnectionInfo;
	
	private String operatorName = null;
	
	public StatCountGroupByOperator(int window, String[] groupByAttributes, String zooConnectionInfo, 
			int statReportPeriod) {
		this.groupByAttributes = groupByAttributes;
		this.window = window;
		this.statReportPeriod = statReportPeriod;
		this.zooConnectionInfo = zooConnectionInfo;
		dataSender = null;
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
		List<Values> returnValues = new ArrayList<Values>();
		StringBuilder strBuild = new StringBuilder();
		for(int i = 0; i < groupByAttributes.length; i++) {
			strBuild.append(values.get(fields.fieldIndex(groupByAttributes[i])));
			strBuild.append(',');
		}
		strBuild.setLength(Math.max(strBuild.length() - 1, 0));
		
		String groupByAttrs = strBuild.toString();
		Integer count = -1;
		for(int i = 0; i < stateValues.size(); i++) {
			if(groupByAttrs.equalsIgnoreCase((String) stateValues.get(i).get(0))) {
				count = (Integer) stateValues.get(i).get(1);
				count += 1;
				Values v = stateValues.get(i);
				v.set(1, count);
				stateValues.set(i, v);
				Values returnVal = new Values(v.toArray());
				returnVal.remove(returnVal.size() - 1);
				returnValues.add(returnVal);
				break;
			}
		}
		if(count == -1) {
			if(stateValues.size() >= window) {
				while(stateValues.size() > window) {
					/**
					 * Locate the tuple with the earliest time 
					 * index (it is the last value added in the end of a tuple)
					 */
					long earliestTime = (long) stateValues.get(0).get(stateValues.get(0).size() - 1);
					int idx = 0;
					for(int i = 0; i < stateValues.size(); i++) {
						if(earliestTime > (long) (stateValues.get(i)).get((stateValues.get(i)).size() - 1) ) {
							earliestTime = (long) (stateValues.get(i)).get((stateValues.get(i)).size() - 1);
							idx = i;
						}
					}
					stateValues.remove(idx);
				}
				Values v = new Values();
				v.add(groupByAttrs);
				v.add(new Integer(1));
				v.add(System.currentTimeMillis());
				stateValues.add(v);
				Values returnVal = new Values(v.toArray());
				returnVal.remove(returnVal.size() - 1);
				returnValues.add(returnVal);
			}else {
				Values v = new Values();
				v.add(groupByAttrs);
				v.add(new Integer(1));
				v.add(System.currentTimeMillis());
				stateValues.add(v);
				Values returnVal = new Values(v.toArray());
				returnVal.remove(returnVal.size() - 1);
				returnValues.add(returnVal);
			}
		}
		updateData(null);
		return returnValues;
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
		stateValues.addAll(receivedStateValues);
		if(stateValues.size() <= window) {
			return;
		}else {
			while(stateValues.size() > window) {
				/**
				 * Locate the tuple with the earliest time 
				 * index (it is the last value added in the end of a tuple)
				 */
				long earliestTime = (long) stateValues.get(0).get(stateValues.get(0).size() - 1);
				int idx = 0;
				for(int i = 0; i < stateValues.size(); i++) {
					if(earliestTime > (long) (stateValues.get(i)).get((stateValues.get(i)).size() - 1) ) {
						earliestTime = (long) (stateValues.get(i)).get((stateValues.get(i)).size() - 1);
						idx = i;
					}
				}
				stateValues.remove(idx);
			}
		}
	}

	@Override
	public List<Values> execute(TaskStatistics statistics, Fields fields,
			Values values) {
		if(dataSender == null) {
			String[] zooTokens = this.zooConnectionInfo.split(":");
			dataSender = new DataCollector(zooTokens[0], Integer.parseInt(zooTokens[1]), 
					statReportPeriod, this.operatorName);
		}
		List<Values> returnValues = new ArrayList<Values>();
		StringBuilder strBuild = new StringBuilder();
		for(int i = 0; i < groupByAttributes.length; i++) {
			strBuild.append(values.get(fields.fieldIndex(groupByAttributes[i])));
			strBuild.append(',');
		}
		strBuild.setLength(Math.max(strBuild.length() - 1, 0));
		
		String groupByAttrs = strBuild.toString();
		Integer count = -1;
		for(int i = 0; i < stateValues.size(); i++) {
			if(groupByAttrs.equalsIgnoreCase((String) stateValues.get(i).get(0))) {
				count = (Integer) stateValues.get(i).get(1);
				count += 1;
				Values v = stateValues.get(i);
				v.set(1, count);
				stateValues.set(i, v);
				Values returnVal = new Values(v.toArray());
				returnVal.remove(returnVal.size() - 1);
				returnValues.add(returnVal);
				break;
			}
		}
		if(count == -1) {
			if(stateValues.size() >= window) {
				while(stateValues.size() > window) {
					/**
					 * Locate the tuple with the earliest time 
					 * index (it is the last value added in the end of a tuple)
					 */
					long earliestTime = (long) stateValues.get(0).get(stateValues.get(0).size() - 1);
					int idx = 0;
					for(int i = 0; i < stateValues.size(); i++) {
						if(earliestTime > (long) (stateValues.get(i)).get((stateValues.get(i)).size() - 1) ) {
							earliestTime = (long) (stateValues.get(i)).get((stateValues.get(i)).size() - 1);
							idx = i;
						}
					}
					stateValues.remove(idx);
				}
				Values v = new Values();
				v.add(groupByAttrs);
				v.add(new Integer(1));
				v.add(System.currentTimeMillis());
				stateValues.add(v);
				Values returnVal = new Values(v.toArray());
				returnVal.remove(returnVal.size() - 1);
				returnValues.add(returnVal);
			}else {
				Values v = new Values();
				v.add(groupByAttrs);
				v.add(new Integer(1));
				v.add(System.currentTimeMillis());
				stateValues.add(v);
				Values returnVal = new Values(v.toArray());
				returnVal.remove(returnVal.size() - 1);
				returnValues.add(returnVal);
			}
		}
		updateData(statistics);
		return returnValues;
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
