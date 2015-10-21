package gr.katsip.deprecated;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @deprecated
 * @param <T>
 */
public class FilterOperator<T> implements AbstractOperator, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3151761129244857244L;

	private Comparator<T> comparator;

	private T value;

	private String fieldName;

	private Fields stateSchema;
	
	private List<Values> stateValues;

	private Fields outputSchema;

	public FilterOperator(Comparator<T> comparator, String fieldName, T value) {
		this.comparator = comparator;
		this.value = value;
		this.fieldName = fieldName;
	}

	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		List<Values> returnTuples = new ArrayList<Values>();
		@SuppressWarnings("unchecked")
		T tValue = (T) values.get(fields.fieldIndex(fieldName));
		if(comparator.compare(value, tValue) == 0) {
			Values newValues = new Values(values.toArray());
			returnTuples.add(newValues);
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
		return outputSchema;
	}

	@Override
	public void mergeState(Fields receivedStateSchema, List<Values> receivedStateValues) {
		//Nothing to do since no state is kept
	}

	@Override
	public void setOutputSchema(Fields outputSchema) {
		this.outputSchema = new Fields(outputSchema.toList());
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		this.stateSchema = new Fields(stateSchema.toList());
	}

}
