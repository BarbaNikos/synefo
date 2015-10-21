package gr.katsip.deprecated;

import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @deprecated
 */
public interface AbstractOperator {
	
	public void init(List<Values> stateValues);
	
	public void setStateSchema(Fields stateSchema);
	
	public void setOutputSchema(Fields output_schema);

	public List<Values> execute(Fields fields, Values values);
	
	public List<Values> getStateValues();
	
	public Fields getStateSchema();
	
	public Fields getOutputSchema();
	
	public void mergeState(Fields receivedStateSchema, List<Values> receivedStateValues);
}
