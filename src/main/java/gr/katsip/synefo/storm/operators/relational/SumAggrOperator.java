package gr.katsip.synefo.storm.operators.relational;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.AbstractOperator;

public class SumAggrOperator implements AbstractOperator, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -23091408494138253L;

	private List<Values> stateValues;
	
	private Fields stateSchema;

	private Integer sum;

	private Fields outputSchema;
	
	private String summationAttributeName;
	
	public SumAggrOperator(String summationAttribute) {
		this.summationAttributeName = summationAttribute;
	}
	
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	public List<Values> execute(Fields fields, Values values) {
		sum += (Integer) values.get(fields.fieldIndex(summationAttributeName));
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
		return outputSchema;
	}

	public void setOutputSchema(Fields outputSchema) {
		if(outputSchema.contains(summationAttributeName))
			this.outputSchema = new Fields(outputSchema.toList());
	}

	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		int sum = (int) receivedStateValues.get(0).get(
				receivedStateSchema.fieldIndex(summationAttributeName));
		this.sum += sum;
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		this.stateSchema = new Fields(stateSchema.toList());
	}

}
