package gr.katsip.synefo.storm.operators.crypstream;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.operators.AbstractStatOperator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ModifiedJoinOperator<T extends Object> implements AbstractStatOperator, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4853379151524340252L;

	private Fields stateSchema;

	private List<Values> stateValues;

	private int window;

	private String joinAttribute;

	private Fields leftFieldSchema;

	private Fields leftStateFieldSchema;

	private ArrayList<Values> leftRelation;

	private Fields rightFieldSchema;

	private Fields rightStateFieldSchema;

	private ArrayList<Values> rightRelation;

	private Fields output_schema;

	private Comparator<T> comparator;
	
	private String zooIP;

	private Integer zooPort;

	private String ID;
	
	private DataCollector dataSender = null;
	
	private int statReportPeriod;
	
	private int statReportCount;
	
	private HashMap<String, Integer> encryptionData = new HashMap<String,Integer>();

	public ModifiedJoinOperator(String ID, Comparator<T> comparator, int window, String joinAttribute, 
			Fields leftFieldSchema, Fields rightFieldSchema, String zooIp, int zooPort, int statReportPeriod) {
		this.window = window;
		this.joinAttribute = joinAttribute;
		/**
		 * Adding timestamp field
		 */
		List<String> schema = leftFieldSchema.toList();
		this.leftFieldSchema = new Fields(leftFieldSchema.toList());
		schema.add("timestamp");
		this.leftStateFieldSchema = new Fields(schema);
		schema = rightFieldSchema.toList();
		this.rightFieldSchema = new Fields(rightFieldSchema.toList());
		schema.add("timestamp");
		this.rightStateFieldSchema = new Fields(schema);
		this.comparator = comparator;
		this.zooIP=zooIp;
		this.zooPort=zooPort;
		this.ID=ID;
		this.statReportPeriod=statReportPeriod;
		
		encryptionData.put("pln",0);
		encryptionData.put("DET",0);
		encryptionData.put("RND",0);
		encryptionData.put("OPE",0);
		encryptionData.put("HOM",0);
	}

	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
		leftRelation = new ArrayList<Values>();
		rightRelation = new ArrayList<Values>();
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		if(dataSender == null) {
			dataSender = new DataCollector(zooIP, zooPort, statReportPeriod, ID);
		}
		String[] tpl = values.get(0).toString().split(Pattern.quote("//$$$//"));
		values.remove(0);
		for(int i=0;i<tpl.length;i++){
			Values v = new Values(tpl[i]);
			values.add(i, v);
		}
		List<Values> result = new ArrayList<Values>();
		if(fields.toList().equals(leftFieldSchema.toList())) {
			for(Values rightStateTuple : rightRelation) {
				Values rightTuple = new Values(rightStateTuple.toArray());
				rightTuple.remove(rightStateFieldSchema.fieldIndex("timestamp"));
				Values resultValues = equiJoin(values, rightTuple);
				if(resultValues != null && resultValues.size() > 0) {
					result.add(resultValues);
				}
			}
			if(leftRelation.size() < window) {
				Values v = new Values(values.toArray());
				v.add(System.currentTimeMillis());
				leftRelation.add(v);
			}else {
				if(leftRelation.size() > 0)
					leftRelation.remove(0);
				Values v = new Values(values.toArray());
				v.add(System.currentTimeMillis());
				leftRelation.add(v);
			}
		}
		if(fields.toList().equals(rightFieldSchema.toList())) {
			for(Values leftStateTuple : leftRelation) {
				Values leftTuple = new Values(leftStateTuple.toArray());
				leftTuple.remove(leftStateFieldSchema.fieldIndex("timestamp"));
				Values resultValues = equiJoin(leftTuple, values);
				if(resultValues != null && resultValues.size() > 0) {
					result.add(resultValues);
				}
			}
			if(rightRelation.size() < window) {
				Values v = new Values(values.toArray());
				v.add(System.currentTimeMillis());
				rightRelation.add(v);
			}else {
				if(rightRelation.size() > 0)
					rightRelation.remove(0);
				Values v = new Values(values.toArray());
				v.add(System.currentTimeMillis());
				rightRelation.add(v);
			}
		}
		if(result.size()>0) {
			String res = ""+result.get(0);
			result.remove(0);
			for(int i=0;i<result.size();i++){
				res = res + "//$$$//"+ result.get(i);
				result.remove(i);
			}
			result.add(0, new Values(res));
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private Values equiJoin(Values leftTuple, Values rightTuple) {
		Values attributes = new Values();
		T val_1 = (T) leftTuple.get(leftFieldSchema.fieldIndex(joinAttribute));
		T val_2 = (T) rightTuple.get(rightFieldSchema.fieldIndex(joinAttribute));
		if(val_1.toString().equalsIgnoreCase(val_2.toString())) {
			for(Object attr : leftTuple) {
				attributes.add(attr);
			}
			for(Object attr : rightTuple) {
				attributes.add(attr);
			}
		}
		return attributes;
	}

	@Override
	public List<Values> getStateValues() {
		if(stateValues.isEmpty() == false)
			stateValues.clear();
		/**
		 * The first record of the stateValues list is the offset 
		 * of the leftRelation records. Therefore, if current state 
		 * has N left-relation tuples, in position 0 the number N 
		 * will be stored. Hence, after removing the offset record (position 0), 
		 * tuples from [0:N] are left relation tuples.
		 */
		int leftRelationSize = leftRelation.size();
		stateValues.add(new Values(new Integer(leftRelationSize)));
		for(Values leftRelationTuple : leftRelation) {
			stateValues.add(leftRelationTuple);
		}
		for(Values rightRelationTuple : rightRelation) {
			stateValues.add(rightRelationTuple);
		}
		return stateValues;
	}

	public Fields getStateSchema() {
		return stateSchema;
	}

	@Override
	public Fields getOutputSchema() {
		List<String> outputSchema = new ArrayList<String>();
//		for(String field : this.leftFieldSchema.toList()) {
//			outputSchema.add("l." + field);
//		}
//		for(String field : this.rightFieldSchema.toList()) {
//			outputSchema.add("r." + field);
//		}
//		output_schema = new Fields(outputSchema);
		return output_schema;
	}

	@Override
	public void mergeState(Fields receivedStateSchema, List<Values> receivedStateValues) {
		int leftRelationSize = (Integer) receivedStateValues.get(0).get(0);
		receivedStateValues.remove(0);
		if(receivedStateValues.size() > 0) {
			ArrayList<Values> receivedLeftRelation = new ArrayList<Values>(
					receivedStateValues.subList(0, leftRelationSize));
			ArrayList<Values> receivedRightRelation = null;
			if(receivedStateValues.size() > leftRelationSize)
				receivedRightRelation = new ArrayList<Values>(
						receivedStateValues.subList(leftRelationSize + 1, receivedStateValues.size()));
			if(receivedLeftRelation != null && receivedLeftRelation.size() > 0)
				this.leftRelation.addAll(receivedLeftRelation);
			if(receivedRightRelation != null && receivedRightRelation.size() > 0)
				this.rightRelation.addAll(receivedRightRelation);
			while(leftRelation.size() > window) {
				long earliestLeftTime = (long) leftRelation.get(0).get(this.leftStateFieldSchema.fieldIndex("timestamp"));
				int leftIndex = 0;
				for(int i = 0; i < leftRelation.size(); i++) {
					if(earliestLeftTime > (long) leftRelation.get(i).get(this.leftStateFieldSchema.fieldIndex("timestamp"))) {
						earliestLeftTime = (long) leftRelation.get(i).get(this.leftStateFieldSchema.fieldIndex("timestamp"));
						leftIndex = i;
					}
				}
				leftRelation.remove(leftIndex);
			}
			while(rightRelation.size() > window) {
				long earliestRightTime = (long) rightRelation.get(0).get(this.rightStateFieldSchema.fieldIndex("timestamp"));
				int rightIndex = 0;
				for(int i = 0; i < rightRelation.size(); i++) {
					if(earliestRightTime > (long) rightRelation.get(i).get(this.rightStateFieldSchema.fieldIndex("timestamp"))) {
						earliestRightTime = (long) rightRelation.get(i).get(this.rightStateFieldSchema.fieldIndex("timestamp"));
						rightIndex = i;
					}
				}
				rightRelation.remove(rightIndex);
			}
		}
	}

	@Override
	public void setOutputSchema(Fields _output_schema) {
		List<String> outputSchema = new ArrayList<String>();
		/*for(String field : this.leftFieldSchema.toList()) {
			outputSchema.add("l." + field);
		}
		for(String field : this.rightFieldSchema.toList()) {
			outputSchema.add("r." + field);
		}*/
		output_schema = new Fields(_output_schema.toList());
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		/**
		 * The following does not really make sense because the left relation might have 
		 * a different schema compared to the right relation.
		 */
	}

	@Override
	public List<Values> execute(TaskStatistics statistics, Fields fields,
			Values values) {
		if(dataSender == null) {
			dataSender = new DataCollector(zooIP, zooPort, statReportPeriod, ID);
		}	
		String[] tpl = values.get(0).toString().split(Pattern.quote("//$$$//"));
		String[] encUse= tpl[tpl.length-1].split(" ");
		String encUsed =tpl[tpl.length-1];
		for(int k = 0; k < encUse.length; k++) {
			encryptionData.put(encUse[k], encryptionData.get(encUse[k])+1);
		}
		updateData(statistics);
		values.remove(0);
		for(int i = 0; i < tpl.length; i++) {
//			Values v = new Values(tpl[i]);
//			values.add(v);
			values.add(new String(tpl[i]));
		}
		if((values.size()-1)==leftFieldSchema.size()){
			fields = new Fields(leftFieldSchema.toList());
		}else{
			fields = new Fields(rightFieldSchema.toList());
		}
		
		List<Values> result = new ArrayList<Values>();
		if(fields.toList().equals(leftFieldSchema.toList())) {
			for(Values rightStateTuple : rightRelation) {
				Values rightTuple = new Values(rightStateTuple.toArray());
				rightTuple.remove(rightStateFieldSchema.fieldIndex("timestamp"));
				Values resultValues = equiJoin(values, rightTuple);
				if(resultValues != null && resultValues.size() > 0) {
					result.add(resultValues);
				}
			}
			if(leftRelation.size() < window) {
				Values v = new Values(values.toArray());
				v.add(System.currentTimeMillis());
				leftRelation.add(v);
			}else {
				if(leftRelation.size() > 0)
					leftRelation.remove(0);
				Values v = new Values(values.toArray());
				v.add(System.currentTimeMillis());
				leftRelation.add(v);
			}
		}
		if(fields.toList().equals(rightFieldSchema.toList())) {
			for(Values leftStateTuple : leftRelation) {
				Values leftTuple = new Values(leftStateTuple.toArray());
				leftTuple.remove(leftStateFieldSchema.fieldIndex("timestamp"));
				Values resultValues = equiJoin(leftTuple, values);
				if(resultValues != null && resultValues.size() > 0) {
					result.add(resultValues);
				}
			}
			if(rightRelation.size() < window) {
				Values v = new Values(values.toArray());
				v.add(System.currentTimeMillis());
				rightRelation.add(v);
			}else {
				if(rightRelation.size() > 0)
					rightRelation.remove(0);
				Values v = new Values(values.toArray());
				v.add(System.currentTimeMillis());
				rightRelation.add(v);
			}
		}
		if(result.size()>0){
			String res = ""+result.get(0);
			result.remove(0);
			for(int i=0;i<result.size();i++){
				res = res + "//$$$//"+ result.get(i).toString().replaceAll("\\[", "").replaceAll("\\]","");
				result.remove(i);
			}
			res=res+","+encUsed;
			System.out.println(res);
			result = new ArrayList<Values>();
			result.add(new Values(res));
		}
		return result;
	}

	@Override
	public void updateOperatorName(String operatorName) {
		this.ID = operatorName;
	}
	
	public void updateData(TaskStatistics stats) {
		int CPU = 0;
		int memory = 0;
		int latency = 0;
		int throughput = 0;
		int sel = 0;
		if(stats != null) {
			String tuple = 	(float) stats.getCpuLoad() + "," + (float) stats.getMemory() + "," + 
					(int) stats.getWindowLatency() + "," + (int) stats.getWindowThroughput() + "," + 
					(float) stats.getSelectivity() + "," + 
					encryptionData.get("pln") + "," +
					encryptionData.get("DET") + "," +
					encryptionData.get("RND") + "," +
					encryptionData.get("OPE") + ","  + 
					encryptionData.get("HOM");
			dataSender.addToBuffer(tuple);
			if(statReportCount>statReportPeriod) {
				encryptionData.put("pln",0);
				encryptionData.put("DET",0);
				encryptionData.put("RND",0);
				encryptionData.put("OPE",0);
				encryptionData.put("HOM",0);
				statReportCount=0;
			}
			statReportCount++;
		}else {
			String tuple = 	CPU + "," + memory + "," + latency + "," + 
					throughput + "," + sel + "," + 
					encryptionData.get("pln") + "," + 
					encryptionData.get("DET") + "," + 
					encryptionData.get("RND") + "," +
					encryptionData.get("OPE") + ","  + 
					encryptionData.get("HOM");

			dataSender.addToBuffer(tuple);
			encryptionData.put("pln",0);
			encryptionData.put("DET",0);
			encryptionData.put("RND",0);
			encryptionData.put("OPE",0);
			encryptionData.put("HOM",0);
		}
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
			dataSender = new DataCollector(zooIP, zooPort, statReportPeriod, ID);
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
			dataSender = new DataCollector(zooIP, zooPort, statReportPeriod, ID);
		}
		dataSender.pushStatisticData(tuple.getBytes());
	}

}
