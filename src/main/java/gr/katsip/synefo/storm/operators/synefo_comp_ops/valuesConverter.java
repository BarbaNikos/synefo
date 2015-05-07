package gr.katsip.synefo.storm.operators.synefo_comp_ops;

import gr.katsip.synefo.storm.operators.AbstractOperator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class valuesConverter implements Serializable, AbstractOperator {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6162934152185342643L;
	private  Fields stateSchema;
	private List<Values> stateValues;
	private Fields output_schema;
	public int ID;

	public valuesConverter(int ID) {
		this.ID=ID;
	}
	
	@Override
	public List<Values> execute(Fields fields, Values values) {
		ArrayList<Values> vals = new ArrayList<Values>();
		if(!values.get(0).toString().contains("SPS")) {
			Object[] tuples = values.get(0).toString().split(",");
			Values v = new Values(tuples);
			vals.add(v);
			return vals;
		}else {
			return vals;
		}
	}
	
	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
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
		output_schema = new Fields(_output_schema.toList());
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		this.stateSchema = new Fields(stateSchema.toList());
	}

}
