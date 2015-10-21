package gr.katsip.deprecated.deprecated;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @deprecated
 */
public interface AbstractTupleProducer {

	public Values nextTuple();
	
	public void setSchema(Fields fields);
	
	public Fields getSchema();
	
}
