package gr.katsip.synefo.storm.operators;

import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public interface AbstractOperator {
	
	public void init(Fields stateSchema, List<Values> stateValues);
	
	public void setOutputSchema(Fields output_schema);

	public List<Values> execute(Fields fields, Values values);
	
	public List<Values> getStateValues();
	
	public Fields getStateSchema();
	
	public Fields getOutputSchema();
	
	public void mergeState(Fields receivedStateSchema, List<Values> receivedStateValues);
}
