package gr.katsip.synefo.storm.producers;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public interface AbstractTupleProducer {

	public Values nextTuple();
	
	public void setSchema(Fields fields);
	
	public Fields getSchema();
	
}
