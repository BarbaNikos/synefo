import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.relational.JoinOperator;
import gr.katsip.synefo.storm.operators.relational.StringComparator;


public class JoinOpTest {

	public static void main(String[] args) {
		String[] leftSchema = { "num", "one", "two", "three", "four" };
		String[] rightSchema = { "num", "1", "2", "three", "5" };
		Fields lf = new Fields(leftSchema);
		Fields rf = new Fields(rightSchema);
		JoinOperator<String> joinOperator = 
				new JoinOperator<String>(new StringComparator(), 100, "three", new Fields(leftSchema), new Fields(rightSchema));
		List<Values> stateValues = new ArrayList<Values>();
		joinOperator.init(stateValues);
		System.out.println("State schema: " + joinOperator.getStateSchema());
		System.out.println("Output schema: " + joinOperator.getOutputSchema());
		
		Values leftTuple = new Values("1", "one", "two", "three", "four");
		List<Values> result = joinOperator.execute(lf, leftTuple);
		System.out.println("result: " + result.toString());
		result = joinOperator.getStateValues();
		System.out.println("state: " + stateValues.toString());
		Values rightTuple = new Values("2", "1", "2", "three", "5");
		result = joinOperator.execute(rf, rightTuple);
		System.out.println("result: " + result.toString());
		result = joinOperator.getStateValues();
		System.out.println("state: " + result.toString());
		joinOperator.mergeState(new Fields(), result);
		result = joinOperator.getStateValues();
		System.out.println("state: " + result.toString());
		String task = "ADD~bolt_1a:12@123.421.123.421";
		String[] tokens = task.split("[~:@]");
		for(String t : tokens) {
			System.out.println(t);
		}
		String punctTuple = "+EFO/ACTION:ADD/COMP:join_bolt_1:7/COMP_NUM:2/IP:136.142.184.19/";
		tokens = punctTuple.split("[/:]");
		String scaleAction = tokens[2];
		String component_name = tokens[4];
		String component_id = tokens[5];
		Integer comp_num = Integer.parseInt(tokens[7]);
		String ip = tokens[9];
		System.out.println("Action: " + scaleAction);
		System.out.println("Name: " + component_name);
		System.out.println("Id: " + component_id);
		System.out.println("Num: " + comp_num);
		System.out.println("IP: " + ip);
	}

}
