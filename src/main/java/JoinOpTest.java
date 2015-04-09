import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.relational.JoinOperator;
import gr.katsip.synefo.storm.operators.relational.StringComparator;


public class JoinOpTest {

	public static void main(String[] args) {
		String[] leftSchema = { "one", "two" };
		String[] rightSchema = {"three", "two" };
		Fields lf = new Fields(leftSchema);
		Fields rf = new Fields(rightSchema);
		JoinOperator joinOperator = 
				new JoinOperator(new StringComparator(), 100, "two", new Fields(leftSchema), new Fields(rightSchema));
		List<Values> stateValues = new ArrayList<Values>();
		joinOperator.init(stateValues);
		System.out.println("State schema: " + joinOperator.getStateSchema());
		System.out.println("Output schema: " + joinOperator.getOutputSchema());
		
//		Values leftTuple = new Values("1", "2");
//		List<Values> result = joinOperator.execute(lf, leftTuple);
//		System.out.println("result: " + result.toString());
//		result = joinOperator.getStateValues();
//		System.out.println("state: " + stateValues.toString());
		Values rightTuple = new Values("3", "2");
		List<Values> result = joinOperator.execute(rf, rightTuple);
		System.out.println("result: " + result.toString());
		result = joinOperator.getStateValues();
		System.out.println("state: " + result.toString());
		joinOperator.mergeState(new Fields(), result);
		result = joinOperator.getStateValues();
		System.out.println("state: " + result.toString());
	}

}
