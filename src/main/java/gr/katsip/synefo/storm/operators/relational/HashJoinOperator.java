package gr.katsip.synefo.storm.operators.relational;

import gr.katsip.synefo.storm.operators.AbstractOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class HashJoinOperator<T extends Object> implements AbstractOperator, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4853379151524340252L;

	private Fields stateSchema;

	private List<Values> stateValues;

	private int window;

	private String field;

	private Fields output_schema;

	private Comparator<T> comparator;
	
	private HashMap<Object, ArrayList<Values>> hashState;
	
	private int hashSize;

	public HashJoinOperator(Comparator<T> _comparator, int _window, String _field) {
		window = _window;
		field = _field;
		comparator = _comparator;
		hashSize = 0;
	}

	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
		hashState = new HashMap<Object, ArrayList<Values>>();
		hashSize = 0;
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		List<Values> result = new ArrayList<Values>();
		if(hashState.containsKey(values.get(fields.fieldIndex(field)))) {
			Iterator<Values> itr = hashState.get(values.get(fields.fieldIndex(field))).iterator();
			while(itr.hasNext()) {
				Values stateTuple = (Values) itr.next();
				Values resultValues = equiJoin(stateTuple, values, fields, field);
				if(resultValues != null && resultValues.size() > 0) {
					result.add(resultValues);
				}
			}
		}
		if(stateValues.size() < window) {
			values.add(System.currentTimeMillis());
			if(hashState.containsKey(values.get(fields.fieldIndex(field)))) {
				ArrayList<Values> bucket = hashState.get(values.get(fields.fieldIndex(field)));
				bucket.add(values);
				hashState.put(values.get(fields.fieldIndex(field)), bucket);
			}else {
				ArrayList<Values> bucket = new ArrayList<Values>();
				bucket.add(values);
				hashState.put(values.get(fields.fieldIndex(field)), bucket);
			}
			hashSize += 1;
		}else {
			Object bucketIndex = -1;
			int recordIndex = -1;
			long earliestTime = System.currentTimeMillis();
			Iterator<Entry<Object, ArrayList<Values>>> bucketIterator = hashState.entrySet().iterator();
			while(bucketIterator.hasNext()) {
				Entry<Object, ArrayList<Values>> bucket = bucketIterator.next();
				ArrayList<Values> records = bucket.getValue();
				for(int i = 0; i < records.size(); i++) {
					if(earliestTime > (long) (records.get(i)).get((records.get(i)).size() - 1) ) {
						earliestTime = (long) (records.get(i)).get((records.get(i)).size() - 1);
						bucketIndex = bucket.getKey();
						recordIndex = i;
					}
				}
			}
			ArrayList<Values> bucket = hashState.get(bucketIndex);
			bucket.remove(recordIndex);
			hashState.put(bucketIndex, bucket);
			hashSize -= 1;
			values.add(System.currentTimeMillis());
			if(hashState.containsKey(values.get(fields.fieldIndex(field)))) {
				bucket = hashState.get(values.get(fields.fieldIndex(field)));
				bucket.add(values);
				hashState.put(values.get(fields.fieldIndex(field)), bucket);
			}else {
				bucket = new ArrayList<Values>();
				bucket.add(values);
				hashState.put(values.get(fields.fieldIndex(field)), bucket);
			}
			hashSize += 1;
		}
		return result;
	}
	
	@SuppressWarnings("unchecked")
	private Values equiJoin(Values stateTuple, Values values, Fields fields, String field) {
		Values attributes = new Values();
		T val_1 = (T) stateTuple.get(stateSchema.fieldIndex(field));
		T val_2 = (T) values.get(fields.fieldIndex(field));
		if(comparator.compare(val_1, val_2) == 0) {
			attributes.add(val_1);
			attributes.add(val_2);
		}
		return attributes;
	}

	@Override
	public List<Values> getStateValues() {
		stateValues.clear();
		Iterator<Entry<Object, ArrayList<Values>>> bucketIterator = hashState.entrySet().iterator();
		while(bucketIterator.hasNext()) {
			Entry<Object, ArrayList<Values>> bucket = bucketIterator.next();
			ArrayList<Values> records = bucket.getValue();
			for(int i = 0; i < records.size(); i++) {
				stateValues.add(records.get(i));
			}
		}
		return stateValues;
	}

	public Fields getStateSchema() {
		return stateSchema;
	}

	@Override
	public Fields getOutputSchema() {
		return output_schema;
	}

	@Override
	public void mergeState(Fields receivedStateSchema, List<Values> receivedStateValues) {
		/**
		 * Need to decide how state is combined
		 */
		if(receivedStateSchema.equals(stateSchema) == false) {
			return;
		}else {
			stateValues.addAll(receivedStateValues);
			/**
			 * Drop tuples from the state until it 
			 * reaches a size equal to the window parameter
			 */
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
			hashState.clear();
			for(Values stateVal : stateValues) {
				if(hashState.containsKey(stateVal.get(stateSchema.fieldIndex(field)))) {
					ArrayList<Values> bucket = hashState.get(stateVal.get(stateSchema.fieldIndex(field)));
					bucket.add(stateVal);
					hashState.put(stateVal.get(stateSchema.fieldIndex(field)), bucket);
				}else {
					ArrayList<Values> bucket = new ArrayList<Values>();
					bucket.add(stateVal);
					hashState.put(stateVal.get(stateSchema.fieldIndex(field)), bucket);
				}
				hashSize += 1;
			}
		}
	}

	@Override
	public void setOutputSchema(Fields _output_schema) {
		output_schema = new Fields(_output_schema.toList());
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		this.stateSchema = new Fields(stateSchema.toList());
	}

}
