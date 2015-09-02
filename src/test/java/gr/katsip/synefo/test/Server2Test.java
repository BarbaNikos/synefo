package gr.katsip.synefo.test;

import gr.katsip.synefo.server2.JoinOperator;
import gr.katsip.synefo.server2.ScaleFunction;
import gr.katsip.synefo.server2.SynefoMaster;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class Server2Test {
	
	public static void main(String[] args) {
		ConcurrentHashMap<String, ArrayList<String>> physicalTopology = new ConcurrentHashMap<String, ArrayList<String>>();
		ConcurrentHashMap<String, ArrayList<String>> activeTopology = new ConcurrentHashMap<String, ArrayList<String>>();
		ConcurrentHashMap<String, Integer> taskIdentifierIndex = new ConcurrentHashMap<String, Integer>();
		ConcurrentHashMap<String, String> taskAddressIndex = new ConcurrentHashMap<String, String>();
		ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation = new ConcurrentHashMap<Integer, JoinOperator>();
		
		ArrayList<String> tasks = new ArrayList<String>();
		tasks.add("dispatcher");
		physicalTopology.put("spout", tasks);
		taskIdentifierIndex.put("spout_9", new Integer(9));
		taskAddressIndex.put("spout_9:9", "1.1.1.1");
		
		tasks = new ArrayList<String>();
		tasks.add("joiner");
		physicalTopology.put("dispatcher", tasks);
		
		taskIdentifierIndex.put("dispatcher_7", new Integer(7));
		taskAddressIndex.put("dispatcher_7:7", "1.1.1.1");
		taskIdentifierIndex.put("dispatcher_8", new Integer(8));
		taskAddressIndex.put("dispatcher_8:8", "1.1.1.1");
		
		tasks = new ArrayList<String>();
		tasks.add("drain");
		physicalTopology.put("joiner", tasks);
		
		
		/**
		 * Relation R storage
		 */
		taskIdentifierIndex.put("joiner_1", new Integer(1));
		taskAddressIndex.put("joiner_1:1", "1.1.1.1");
		taskToJoinRelation.put(new Integer(1), new JoinOperator(1, JoinOperator.Step.JOIN, "R"));
		taskIdentifierIndex.put("joiner_2", new Integer(2));
		taskAddressIndex.put("joiner_2:2", "1.1.1.1");
		taskToJoinRelation.put(new Integer(2), new JoinOperator(2, JoinOperator.Step.JOIN, "R"));
		taskIdentifierIndex.put("joiner_3", new Integer(3));
		taskAddressIndex.put("joiner_3:3", "1.1.1.1");
		taskToJoinRelation.put(new Integer(3), new JoinOperator(3, JoinOperator.Step.JOIN, "R"));
		/**
		 * Relation S storage
		 */
		taskIdentifierIndex.put("joiner_4", new Integer(4));
		taskAddressIndex.put("joiner_4:4", "1.1.1.1");
		taskToJoinRelation.put(new Integer(4), new JoinOperator(4, JoinOperator.Step.JOIN, "S"));
		taskIdentifierIndex.put("joiner_5", new Integer(5));
		taskAddressIndex.put("joiner_5:5", "1.1.1.1");
		taskToJoinRelation.put(new Integer(5), new JoinOperator(5, JoinOperator.Step.JOIN, "S"));
		taskIdentifierIndex.put("joiner_6", new Integer(6));
		taskAddressIndex.put("joiner_6:6", "1.1.1.1");
		taskToJoinRelation.put(new Integer(6), new JoinOperator(6, JoinOperator.Step.JOIN, "S"));
		
		physicalTopology.put("drain", new ArrayList<String>());
		taskIdentifierIndex.put("drain_10", new Integer(10));
		taskAddressIndex.put("drain_10:10", "1.1.1.1");
		
		ConcurrentHashMap<String, ArrayList<String>> expandedPhysicalTopology = SynefoMaster.physicalTopologyTaskExpand(
				taskIdentifierIndex, physicalTopology);
//		System.out.println(expandedPhysicalTopology.toString());
		
		ConcurrentHashMap<String, ArrayList<String>> updatedTopology = SynefoMaster.updatePhysicalTopology(
				taskAddressIndex, taskIdentifierIndex, expandedPhysicalTopology);
//		System.out.println(updatedTopology.toString());
		
		System.out.println("++++ NOW to THE JOIN TEST ++++");
		activeTopology = SynefoMaster.getInitialActiveTopologyWithJoinOperators(
				updatedTopology,
				ScaleFunction.getInverseTopology(updatedTopology), taskToJoinRelation, false);
		System.out.println("Active topology with minimalFlag set to false: " + activeTopology.toString());
		activeTopology = SynefoMaster.getInitialActiveTopologyWithJoinOperators(
				updatedTopology,
				ScaleFunction.getInverseTopology(updatedTopology), taskToJoinRelation, true);
		System.out.println("Active topology with minimalFlag set to true: " + activeTopology.toString());
		ScaleFunction scaleFunction = new ScaleFunction(
				updatedTopology, activeTopology, taskToJoinRelation);
		String scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_4:4@1.1.1.1");
		assert scaleInCommand == null;
		System.out.println(scaleInCommand);
		String scaleOutCommand = scaleFunction.produceScaleOutCommand("dispatcher_7:7@1.1.1.1", "joiner_4:4@1.1.1.1");
		System.out.println(scaleOutCommand);
		scaleOutCommand = scaleFunction.produceScaleOutCommand("dispatcher_7:7@1.1.1.1", "joiner_4:4@1.1.1.1");
		System.out.println(scaleOutCommand);
		scaleOutCommand = scaleFunction.produceScaleOutCommand("dispatcher_7:7@1.1.1.1", "joiner_4:4@1.1.1.1");
		System.out.println(scaleOutCommand);
		
		scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_4:4@1.1.1.1");
		System.out.println(scaleInCommand);
		
		scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_4:4@1.1.1.1");
		System.out.println(scaleInCommand);
		
		scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_4:4@1.1.1.1");
		System.out.println(scaleInCommand);
		
		scaleInCommand = scaleFunction.produceScaleInCommand("spout_9:9@1.1.1.1", "dispatcher_7:7@1.1.1.1");
		System.out.println("Normal: " + scaleInCommand);
		
		scaleOutCommand = scaleFunction.produceScaleOutCommand("spout_9:9@1.1.1.1", "dispatcher_7:7@1.1.1.1");
		System.out.println(scaleOutCommand);
		
		scaleInCommand = scaleFunction.produceScaleInCommand("spout_9:9@1.1.1.1", "dispatcher_7:7@1.1.1.1");
		System.out.println("Normal: " + scaleInCommand);
	}

}
