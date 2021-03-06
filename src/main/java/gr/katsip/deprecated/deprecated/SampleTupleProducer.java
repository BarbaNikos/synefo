package gr.katsip.deprecated.deprecated;

import java.io.Serializable;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @deprecated
 */
public class SampleTupleProducer implements AbstractTupleProducer, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1964966398874669639L;

	private int word_idx;
	
	private Fields output_schema;
	
	private final String[] words = new String[] { "nathan", "mike", "jackson", "golda", "bertels", "nick", "romanos", "alexandros", "cory" };
	
	private long num;
	
	public SampleTupleProducer() {
		word_idx = 0;
		String[] fields = { "name" };
		output_schema = new Fields(fields);
		num = 0;
	}

	@Override
	public Values nextTuple() {
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
//		System.out.println("SampleTupleProducer emits!!!");
		Values val = new Values();
		val.add(new Long(num));
		val.add(word);
		return val;
	}

	@Override
	public void setSchema(Fields fields) {
		output_schema = new Fields(fields.toList());
	}

	@Override
	public Fields getSchema() {
		return output_schema;
	}
}
