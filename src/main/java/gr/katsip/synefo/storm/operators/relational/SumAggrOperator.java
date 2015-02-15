package gr.katsip.synefo.storm.operators.relational;

import java.util.ArrayList;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.AbstractOperator;

public class SumAggrOperator implements AbstractOperator {

	private List<Values> stateValues;
	
	private Fields stateSchema;

	private Integer sum;

	private Fields output_schema;
	
	private String summation_attribute;
	
	public SumAggrOperator(String _summation_attribute) {
		summation_attribute = _summation_attribute;
	}
	
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	public List<Values> execute(Fields fields, Values values) {
		sum += (Integer) values.get(fields.fieldIndex(summation_attribute));
		Values newValues = new Values();
		newValues.add(sum);
		List<Values> returnTuples = new ArrayList<Values>();
		returnTuples.add(newValues);
		return returnTuples;
	}
	
	public List<Values> getStateValues() {
		stateValues.clear();
		Values summation = new Values();
		summation.add(sum);
		stateValues.add(summation);
		return stateValues;
	}

	public Fields getStateSchema() {
		return stateSchema;
	}

	public Fields getOutputSchema() {
		return output_schema;
	}

	public void setOutputSchema(Fields _output_schema) {
		if(_output_schema.contains(summation_attribute))
			output_schema = new Fields(_output_schema.toList());
	}

	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		int sum = (int) receivedStateValues.get(0).get(receivedStateSchema.fieldIndex(summation_attribute));
		this.sum += sum;
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		this.stateSchema = new Fields(stateSchema.toList());
	}

}
