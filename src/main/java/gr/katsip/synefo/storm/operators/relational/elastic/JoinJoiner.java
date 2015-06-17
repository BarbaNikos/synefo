package gr.katsip.synefo.storm.operators.relational.elastic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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
	
	private HashMap<String, ArrayList<Integer>> tupleIndex = null;
	
	private ArrayList<Values> tuples = null;

	public JoinJoiner(String storedRelation, Fields storedRelationSchema, String otherRelation, 
			Fields otherRelationSchema, String storedJoinAttribute, String otherJoinAttribute) {
		this.storedRelation = storedRelation;
		this.storedRelationSchema = new Fields(storedRelationSchema.toList());
		this.otherRelation = otherRelation;
		this.otherRelationSchema = new Fields(otherRelationSchema.toList());
		this.storedJoinAttribute = storedJoinAttribute;
		this.otherJoinAttribute = otherJoinAttribute;
		if(this.storedRelation.compareTo(this.otherRelation) <= 0) {
			List<String> schema = storedRelationSchema.toList();
			schema.addAll(otherRelationSchema.toList());
			joinOutputSchema = new Fields(schema);
		}else {
			List<String> schema = otherRelationSchema.toList();
			schema.addAll(storedRelationSchema.toList());
			joinOutputSchema = new Fields(schema);
		}
	}
	
	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
		this.tupleIndex = new HashMap<String, ArrayList<Integer>>();
		this.tuples = new ArrayList<Values>();
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
		Fields attributeNames = new Fields(((Fields) values.get(0)).toList());
		Values attributeValues = (Values) values.get(1);
		if(Arrays.equals(attributeNames.toList().toArray(), storedRelationSchema.toList().toArray())) {
			/**
			 * Store the new tuple
			 */
			tuples.add(attributeValues);
			Integer position = tuples.lastIndexOf(attributeValues);
			String joinAttributeValue = (String) attributeValues.get(storedRelationSchema.fieldIndex(storedJoinAttribute));
			if(tupleIndex.containsKey(joinAttributeValue)) {
				ArrayList<Integer> positionArray = tupleIndex.get(joinAttributeValue);
				positionArray.add(position);
				tupleIndex.put(joinAttributeValue, positionArray);
			}else {
				ArrayList<Integer> positionArray = new ArrayList<Integer>();
				positionArray.add(position);
				tupleIndex.put(joinAttributeValue, positionArray);
			}
		}else if(Arrays.equals(attributeNames.toList().toArray(), otherRelationSchema.toList().toArray()) && activeTasks != null && activeTasks.size() > 0) {
			/**
			 * Attempt to join with stored tuples
			 */
			String joinAttributeValue = (String) attributeValues.get(otherRelationSchema.fieldIndex(otherJoinAttribute));
			List<Values> joinResult = new ArrayList<Values>();
			if(tupleIndex.containsKey(joinAttributeValue)) {
				ArrayList<Integer> positionArray = tupleIndex.get(joinAttributeValue);
				if(positionArray != null && positionArray.size() > 0) {
					for(Integer index : positionArray) {
						Values tuple = tuples.get(index);
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
	}

	@Override
	public List<Values> getStateValues() {
		stateValues.clear();
		Values state = new Values();
		state.add(tupleIndex);
		state.add(tuples);
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
