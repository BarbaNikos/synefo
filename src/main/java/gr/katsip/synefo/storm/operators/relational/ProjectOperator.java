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

	private Fields outputSchema;

	private Fields projectedAttributes;

	public ProjectOperator(Fields projectedAttributes) {
		this.projectedAttributes = new Fields(projectedAttributes.toList());
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		List<Values> returnTuples = new ArrayList<Values>();
		Iterator<String> itr = projectedAttributes.iterator();
		Values projected_values = new Values();
		while(itr.hasNext()) {
			String field = itr.next();
			projected_values.add(values.get(fields.fieldIndex(field)));
		}
		returnTuples.add(projected_values);
		return returnTuples;
	}

	@Override
	public Fields getOutputSchema() {
		return outputSchema;
	}

	@Override
	public void setOutputSchema(Fields outputSchema) {
		this.outputSchema = new Fields(outputSchema.toList());
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
