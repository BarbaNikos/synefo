package gr.katsip.synefo.storm.operators.relational;

import gr.katsip.synefo.storm.operators.AbstractOperator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class JoinOperator<T extends Object> implements AbstractOperator, Serializable {

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

	public JoinOperator(Comparator<T> comparator, int window, String joinAttribute, 
			Fields leftFieldSchema, Fields rightFieldSchema) {
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
	}

	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
		leftRelation = new ArrayList<Values>();
		rightRelation = new ArrayList<Values>();
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
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
		return result;
	}

	@SuppressWarnings("unchecked")
	private Values equiJoin(Values leftTuple, Values rightTuple) {
		Values attributes = new Values();
		T val_1 = (T) leftTuple.get(leftFieldSchema.fieldIndex(joinAttribute));
		T val_2 = (T) rightTuple.get(rightFieldSchema.fieldIndex(joinAttribute));
		if(comparator.compare(val_1, val_2) == 0) {
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
		for(String field : this.leftFieldSchema.toList()) {
			outputSchema.add("l." + field);
		}
		for(String field : this.rightFieldSchema.toList()) {
			outputSchema.add("r." + field);
		}
		output_schema = new Fields(outputSchema);
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
		for(String field : this.leftFieldSchema.toList()) {
			outputSchema.add("l." + field);
		}
		for(String field : this.rightFieldSchema.toList()) {
			outputSchema.add("r." + field);
		}
		output_schema = new Fields(outputSchema);
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		/**
		 * The following does not really make sense because the left relation might have 
		 * a different schema compared to the right relation.
		 */
		//		List<String> schema = stateSchema.toList();
		//		schema.add("timestamp");
		//		this.stateSchema = new Fields(schema);
	}

}
