package gr.katsip.synefo.storm.operators;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public interface AbstractTupleProducer {

	public Values nextTuple(int task_id);
	
	public void setSchema(Fields fields);
	
	public Fields getSchema();
	
}
