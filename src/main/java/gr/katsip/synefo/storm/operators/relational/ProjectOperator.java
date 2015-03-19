package gr.katsip.synefo.storm.operators.relational;

import gr.katsip.synefo.storm.operators.AbstractOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ProjectOperator implements AbstractOperator, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9202155739771523785L;

	private List<Values> stateValues;
	
	private Fields stateSchema;

	private Fields output_schema;

	private Fields projected_attributes;

	public ProjectOperator(Fields _projected_attributes) {
		projected_attributes = new Fields(_projected_attributes.toList());
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		List<Values> returnTuples = new ArrayList<Values>();
		Iterator<String> itr = projected_attributes.iterator();
		Values projected_values = new Values();
		while(itr.hasNext()) {
			String field = itr.next();
			if(field.equals("timestamp")) {
				projected_values.add(new Long(System.currentTimeMillis()));
			}else {
				projected_values.add(values.get(fields.fieldIndex(field)));
			}
		}
		returnTuples.add(projected_values);
		return returnTuples;
	}

	@Override
	public Fields getOutputSchema() {
		return output_schema;
	}

	@Override
	public void setOutputSchema(Fields _output_schema) {
		output_schema = new Fields(_output_schema.toList());
	}

	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	@Override
	public List<Values> getStateValues() {
		return stateValues;
	}

	@Override
	public Fields getStateSchema() {
		return stateSchema;
	}

	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		//Nothing to be done since no state is kept
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		this.stateSchema = new Fields(stateSchema.toList());
	}

}
