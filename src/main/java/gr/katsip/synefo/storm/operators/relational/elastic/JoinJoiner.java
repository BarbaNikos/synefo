package gr.katsip.synefo.storm.operators.relational.elastic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.AbstractJoinOperator;

public class JoinJoiner implements AbstractJoinOperator, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1309799850817632049L;

	private String storedRelation;
	
	private Fields storedRelationSchema;
	
	private String otherRelation;
	
	private Fields otherRelationSchema;
	
	private String storedJoinAttribute;
	
	private String otherJoinAttribute;
	
	private List<Values> stateValues;
	
	private Fields outputSchema;
	
	private Fields joinOutputSchema;
	
	private HashMap<String, ArrayList<Long>> tupleIndex = null;
	
//	private ArrayList<Values> tuples = null;
	
	private TreeMap<Long, Values> timestampIndex = null;
	
	/**
	 * Window size in seconds
	 */
	private int windowSize;
	
	/**
	 * Window slide in seconds
	 */
	private int slide;

	public JoinJoiner(String storedRelation, Fields storedRelationSchema, String otherRelation, 
			Fields otherRelationSchema, String storedJoinAttribute, String otherJoinAttribute, int window, int slide) {
		this.storedRelation = storedRelation;
		this.storedRelationSchema = new Fields(storedRelationSchema.toList());
		this.otherRelation = otherRelation;
		this.otherRelationSchema = new Fields(otherRelationSchema.toList());
		this.storedJoinAttribute = storedJoinAttribute;
		this.otherJoinAttribute = otherJoinAttribute;
		if(this.storedRelationSchema.fieldIndex(this.storedJoinAttribute) < 0) {
			throw new IllegalArgumentException("Not compatible stored-relation schema with the join-attribute for the stored relation");
		}
		if(this.otherRelationSchema.fieldIndex(this.otherJoinAttribute) < 0) {
			throw new IllegalArgumentException("Not compatible other-relation schema with the join-attribute for the other relation");
		}
		if(this.storedRelation.compareTo(this.otherRelation) <= 0) {
			List<String> schema = storedRelationSchema.toList();
			schema.addAll(otherRelationSchema.toList());
			joinOutputSchema = new Fields(schema);
		}else {
			List<String> schema = otherRelationSchema.toList();
			schema.addAll(storedRelationSchema.toList());
			joinOutputSchema = new Fields(schema);
		}
		this.windowSize = window;
		this.slide = slide;
		timestampIndex = new TreeMap<Long, Values>();
		tupleIndex = new HashMap<String, ArrayList<Long>>();
	}
	
	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
		timestampIndex = new TreeMap<Long, Values>();
		tupleIndex = new HashMap<String, ArrayList<Long>>();
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		//Do nothing
	}

	@Override
	public void setOutputSchema(Fields output_schema) {
		outputSchema = new Fields(output_schema.toList());
	}

	@Override
	public void execute(OutputCollector collector,
			HashMap<String, ArrayList<Integer>> taskRelationIndex,
			ArrayList<Integer> activeTasks, Integer taskIndex, Fields fields,
			Values values) {
		/**
		 * Receive a tuple that: attribute[0] : fields, attribute[1] : values
		 */
		Long currentTimestamp = System.currentTimeMillis();
		Fields attributeNames = new Fields(((Fields) values.get(0)).toList());
		Values attributeValues = (Values) values.get(1);
		if(Arrays.equals(attributeNames.toList().toArray(), storedRelationSchema.toList().toArray())) {
			/**
			 * Store the new tuple
			 */
			timestampIndex.put(currentTimestamp, attributeValues);
			String joinAttributeValue = (String) attributeValues.get(storedRelationSchema.fieldIndex(storedJoinAttribute));
			if(tupleIndex.containsKey(joinAttributeValue)) {
				ArrayList<Long> timestampArray = tupleIndex.get(joinAttributeValue);
				timestampArray.add(currentTimestamp);
				tupleIndex.put(joinAttributeValue, timestampArray);
			}else {
				ArrayList<Long> timestampArray = new ArrayList<Long>();
				timestampArray.add(currentTimestamp);
				tupleIndex.put(joinAttributeValue, timestampArray);
			}
		}else if(Arrays.equals(attributeNames.toList().toArray(), otherRelationSchema.toList().toArray()) && activeTasks != null && activeTasks.size() > 0) {
			/**
			 * Attempt to join with stored tuples
			 */
			String joinAttributeValue = (String) attributeValues.get(otherRelationSchema.fieldIndex(otherJoinAttribute));
			List<Values> joinResult = new ArrayList<Values>();
			if(tupleIndex.containsKey(joinAttributeValue)) {
				ArrayList<Long> timestampArray = tupleIndex.get(joinAttributeValue);
				if(timestampArray != null && timestampArray.size() > 0) {
					for(Long timestamp : timestampArray) {
						Values tuple = timestampIndex.get(timestamp);
						Values result = new Values(tuple.toArray());
						result.addAll(attributeValues);
						joinResult.add(result);
					}
					for(Values result : joinResult) {
						Values tuple = new Values();
						tuple.add(joinOutputSchema);
						tuple.add(result);
						collector.emitDirect(activeTasks.get(taskIndex), tuple);
						if(taskIndex >= activeTasks.size())
							taskIndex = 0;
						else
							taskIndex += 1;
					}
				}
			}
		}
		/**
		 * Maintain state: Evict tuples that are too old
		 */
		
	}

	@Override
	public List<Values> getStateValues() {
		stateValues.clear();
		Values state = new Values();
		state.add(tupleIndex);
		state.add(timestampIndex);
		stateValues.add(state);
		return stateValues;
	}

	@Override
	public Fields getStateSchema() {
		return null;
	}

	@Override
	public Fields getOutputSchema() {
		return outputSchema;
	}

	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		// TODO Auto-generated method stub

	}

	@Override
	public String operatorStep() {
		return "JOIN";
	}

	@Override
	public String relationStorage() {
		return storedRelation;
	}

}
