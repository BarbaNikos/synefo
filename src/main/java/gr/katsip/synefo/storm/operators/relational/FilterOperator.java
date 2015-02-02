package gr.katsip.synefo.storm.operators.relational;

import gr.katsip.synefo.storm.operators.AbstractOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FilterOperator<T> implements AbstractOperator, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3151761129244857244L;

	private Comparator<T> comparator;

	private T value;

	private String field;

	private Fields stateSchema;
	
	private List<Values> stateValues;

	private Fields output_schema;

	public FilterOperator(Comparator<T> _comparator, String _field, T _value) {
		comparator = _comparator;
		value = _value;
		field = _field;
	}

	@Override
	public void init(Fields stateSchema, List<Values> stateValues) {
		this.stateSchema = stateSchema;
		this.stateValues = stateValues;
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		List<Values> returnTuples = new ArrayList<Values>();
		@SuppressWarnings("unchecked")
		T tValue = (T) values.get(fields.fieldIndex(field));
		if(comparator.compare(value, tValue) == 0) {
			returnTuples.add(new Values(values));
			return returnTuples;
		}
		return returnTuples;
	}

	@Override
	public List<Values> getStateValues() {
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
		//Nothing to do since no state is kept
	}

	@Override
	public void setOutputSchema(Fields _output_schema) {
		output_schema = _output_schema;
	}

}
