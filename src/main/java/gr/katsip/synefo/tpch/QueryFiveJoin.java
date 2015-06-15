package gr.katsip.synefo.tpch;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.AbstractOperator;

public class QueryFiveJoin implements AbstractOperator {
	
	private List<Values> stateValues;

	private HashMap<Integer, List<Values>> nations;
	
	private HashMap<Integer, List<Values>> regions;
	
	private HashMap<Integer, List<Values>> suppliers;
	
	private HashMap<Integer, List<Values>> customers;
	
	private HashMap<Integer, List<Values>> orders;
	
	private HashMap<Integer, List<Values>> lineItems;
	
	public QueryFiveJoin() {
		nations = new HashMap<Integer, List<Values>>();
		regions = new HashMap<Integer, List<Values>>();
		suppliers = new HashMap<Integer, List<Values>>();
		customers = new HashMap<Integer, List<Values>>();
		orders = new HashMap<Integer, List<Values>>();
		lineItems = new HashMap<Integer, List<Values>>();
	}
	
	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		//Do nothing
	}

	@Override
	public void setOutputSchema(Fields output_schema) {
		
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		if(Arrays.equals(Customer.schema, fields.toList().toArray())) {
			
		}else if(Arrays.equals(LineItem.schema, fields.toList().toArray())) {
			
		}else if(Arrays.equals(Nation.schema, fields.toList().toArray())) {
			
		}else if(Arrays.equals(Order.schema, fields.toList().toArray())) {
			
		}else if(Arrays.equals(Region.schema, fields.toList().toArray())) {
			
		}else if(Arrays.equals(Supplier.schema, fields.toList().toArray())) {
			
		}
		
		return null;
	}

	@Override
	public List<Values> getStateValues() {
		return null;
	}

	@Override
	public Fields getStateSchema() {
		return null;
	}

	@Override
	public Fields getOutputSchema() {
		return null;
	}

	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		
	}

}
