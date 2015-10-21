package gr.katsip.deprecated;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @deprecated
 */
public class CountAggrOperator implements AbstractOperator, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6778746225568944495L;

	private List<Values> stateValues;
	
	private Fields stateSchema;
	
	private Integer count;
	
	private Fields outputSchema;
	
	public CountAggrOperator() {
		count = 0;
	}
	
	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		count += 1;
		Values newValues = new Values();
		newValues.add(count);
		List<Values> returnedTuples = new ArrayList<Values>();
		returnedTuples.add(newValues);
		return returnedTuples;
	}

	@Override
	public List<Values> getStateValues() {
		stateValues.clear();
		Values newCount = new Values();
		newCount.add(count);
		stateValues.add(newCount);
		return stateValues;
	}

	@Override
	public Fields getStateSchema() {
		return stateSchema;
	}

	@Override
	public Fields getOutputSchema() {
		return outputSchema;
	}
	
	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		Integer receivedCount = (Integer) (receivedStateValues.get(0)).get(0);
		count += receivedCount;
	}

	@Override
	public void setOutputSchema(Fields _output_schema) {
		outputSchema = new Fields(_output_schema.toList());
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		this.stateSchema = new Fields(stateSchema.toList());
	}

}
