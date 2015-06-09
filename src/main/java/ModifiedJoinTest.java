import java.util.List;

import gr.katsip.synefo.storm.operators.crypstream.ModifiedJoinOperator;
import gr.katsip.synefo.storm.operators.relational.StringComparator;
import backtype.storm.tuple.Fields;


public class ModifiedJoinTest {

	public static void main(String[] args) {
		String[] selectionOutputSchema = { "tuple" };
		String[] vals_left = { "id" , "name" , "stairs_climed" , "steps" , "blood_pressure" , "location" };
		String[] vals_right = { "id" , "car_ID" , "car_owner" , "location" };
		ModifiedJoinOperator<String> joinOperator = new ModifiedJoinOperator<String>("join_bolt_1",new StringComparator(), 100, "location", 
				new Fields(vals_left), new Fields(vals_right), "127.0.0.1", 2181, 50);
		joinOperator.setOutputSchema(new Fields(selectionOutputSchema));
		
		List<String> schema = (new Fields(vals_left)).toList();
		Fields leftFieldSchema = new Fields(vals_left);
		schema.add("timestamp");
		Fields leftStateFieldSchema = new Fields(schema);
		schema = (new Fields(vals_right)).toList();
		Fields rightFieldSchema = new Fields(vals_right);
		schema.add("timestamp");
		Fields rightStateFieldSchema = new Fields(schema);
		System.out.println(leftStateFieldSchema.toList().toString());
		System.out.println(rightStateFieldSchema.toList().toString());
	}

}
