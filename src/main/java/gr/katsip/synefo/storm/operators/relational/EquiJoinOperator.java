package gr.katsip.synefo.storm.operators.relational;

import gr.katsip.synefo.storm.operators.AbstractOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class EquiJoinOperator<T extends Object> implements AbstractOperator, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 36396552098460810L;
	
	private Fields stateSchema;
	
	private List<Values> stateValues;
	
	private int window;
	
	private String field;
	
	private Fields output_schema;
	
	private Comparator<T> comparator;
	
	public EquiJoinOperator(Comparator<T> _comparator, int _window, String _field) {
		window = _window;
		field = _field;
		comparator = _comparator;
	}

	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		List<Values> result = new ArrayList<Values>();
		Iterator<Values> itr = stateValues.iterator();
		while(itr.hasNext()) {
			Values stateTuple = (Values) itr.next();
			Values resultValues = equiJoin(stateTuple, values, fields, field);
			if(resultValues != null && resultValues.size() > 0) {
//				System.out.println("EQUI-JOINED: " + values);
				result.add(resultValues);
			}
		}
		if(stateValues.size() < window) {
			values.add(System.currentTimeMillis());
			stateValues.add(values);
		}else {
			long earliestTime = (long) stateValues.get(0).get(stateValues.get(0).size() - 1);
			int idx = 0;
			for(int i = 0; i < stateValues.size(); i++) {
				if(earliestTime > (long) (stateValues.get(i)).get((stateValues.get(i)).size() - 1) ) {
					earliestTime = (long) (stateValues.get(i)).get((stateValues.get(i)).size() - 1);
					idx = i;
				}
			}
			stateValues.remove(idx);
			values.add(System.currentTimeMillis());
			stateValues.add(values);
		}
		return result;
	}

	@Override
	public List<Values> getStateValues() {
		return stateValues;
	}
	
	public Fields getStateSchema() {
		return stateSchema;
	}
	
	@SuppressWarnings("unchecked")
	private Values equiJoin(Values stateTuple, Values values, Fields fields, String field) {
		Values attributes = new Values();
		T val_1 = (T) stateTuple.get(stateSchema.fieldIndex(field));
		T val_2 = (T) values.get(fields.fieldIndex(field));
		if(comparator.compare(val_1, val_2) == 0) {
			/**
			 * The time-stamp field (the last field of each state tuple) is not included in 
			 * the result produced
			 */
			for(int i = 0; i < stateTuple.size() - 1; i++) {
				attributes.add(stateTuple.get(stateSchema.fieldIndex(field)));
			}
			for(int i = 0; i < values.size(); i++) {
				attributes.add(values.get(fields.fieldIndex(field)));
			}
		}
		return attributes;
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
		}
	}

	@Override
	public void setOutputSchema(Fields _output_schema) {
		output_schema = _output_schema;
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		this.stateSchema = new Fields(stateSchema.toList());
	}

}
