import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.relational.JoinOperator;
import gr.katsip.synefo.storm.operators.relational.StringComparator;


public class JoinOpTest {

	public static void main(String[] args) {
		String[] leftSchema = { "one", "two", "three", "four", "five" };
		String[] rightSchema = { "one", "two", "three", "four", "five" };
//		String[] rightSchema = { "num", "1", "2", "three", "5" };
		Fields lf = new Fields(leftSchema);
		Fields rf = new Fields(rightSchema);
		JoinOperator<String> joinOperator = 
				new JoinOperator<String>(new StringComparator(), 100000000, "three", new Fields(leftSchema), new Fields(rightSchema));
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
		
//		Runtime runtime = Runtime.getRuntime();
//		System.out.println("Total memory: " + runtime.totalMemory());
//		System.out.println("Free memory: " + runtime.freeMemory());
		List<Values> state = new ArrayList<Values>();
		for(int i = 0; i < 10000000; ++i) {
			leftTuple = new Values("1", "one", "two", "three", "four");
			rightTuple = new Values("2", "1", "2", "three", "5");
//			joinOperator.execute(lf, leftTuple);
//			joinOperator.execute(rf, rightTuple);
			state.add(leftTuple);
			state.add(rightTuple);
		}
		Runtime runtime = Runtime.getRuntime();
		System.out.println("Total memory: " + runtime.totalMemory());
		System.out.println("Max memory: " + runtime.maxMemory());
		System.out.println("Free memory: " + runtime.freeMemory());
		double memory = (double) (runtime.maxMemory() - runtime.totalMemory()) / runtime.maxMemory();
		System.out.println("Used memory percentage: " + memory + " %");
//		for(int i = 0; i < 10000000; ++i) {
//			leftTuple = new Values("1", "one", "two", "three", "four");
//			rightTuple = new Values("2", "1", "2", "three", "5");
//			joinOperator.execute(lf, leftTuple);
//			joinOperator.execute(rf, rightTuple);
//			runtime = Runtime.getRuntime();
//			double memory = (runtime.totalMemory() - runtime.freeMemory());
//			if(memory > (double) 0.0)
//				System.out.println("Memory used: " + memory + "%");
//		}
	}

}
