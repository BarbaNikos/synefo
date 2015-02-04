package gr.katsip.synefo.storm.operators;

import java.io.Serializable;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SampleTupleProducer implements AbstractTupleProducer, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1964966398874669639L;

	private int word_idx;
	
	private Fields output_schema;
	
	private final String[] words = new String[] { "nathan", "mike", "jackson", "golda", "bertels", "nick", "romanos", "alexandros", "cory" };
	
//	private final String[] words = new String[] { "nathan" };
	
	public SampleTupleProducer() {
		word_idx = 0;
		String[] fields = { "name" };
		output_schema = new Fields(fields);
	}

	@Override
	public Values nextTuple(int task_id) {
		/**
		 * Sample execution
		 */
//		Utils.sleep(1000);
		String word = "";
		if(word_idx < words.length) {
			word = words[word_idx];
			word_idx += 1;
		}
		if(word_idx > (words.length - 1))
			word_idx = 0;
		System.out.println("SampleTupleProducer emits!!!");
		return new Values(word);
	}

	@Override
	public void setSchema(Fields fields) {
		output_schema = fields;
	}

	@Override
	public Fields getSchema() {
		return output_schema;
	}
}
