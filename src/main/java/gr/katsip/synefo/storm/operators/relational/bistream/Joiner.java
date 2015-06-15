package gr.katsip.synefo.storm.operators.relational.bistream;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.AbstractOperator;

public class Joiner implements AbstractOperator {
	
	private Fields storedRelationSchema;
	
	private Fields joinRelationSchema;
	
	private HashMap<Object, List<Values>> equalityIndex;
	
	private TreeMap<Object, List<Values>> rangeIndex;
	
	private List<Values> stateValues;
	
	public Joiner(Fields storedRelationSchema, Fields joinRelationSchema) {
		this.storedRelationSchema = storedRelationSchema;
		this.joinRelationSchema = joinRelationSchema;
	}

	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		
	}

	@Override
	public void setOutputSchema(Fields output_schema) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Values> getStateValues() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getStateSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getOutputSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		// TODO Auto-generated method stub

	}

}
