package gr.katsip.synefo.junit.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import gr.katsip.synefo.server2.JoinOperator;
import gr.katsip.synefo.server2.ScaleFunction;
import gr.katsip.synefo.server2.SynefoCoordinatorThread;

import org.junit.Test;

public class ScaleFunctionServer2Test {

	@Test
	public void test() {
		ScaleFunction scaleFunction = new ScaleFunction(null, null, null);
		assertEquals(scaleFunction.physicalTopology, null);
		
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
		ConcurrentHashMap<String, ArrayList<String>> expandedPhysicalTopology = SynefoCoordinatorThread.physicalTopologyTaskExpand(
				taskIdentifierIndex, physicalTopology);
		ConcurrentHashMap<String, ArrayList<String>> updatedTopology = SynefoCoordinatorThread.updatePhysicalTopology(
				taskAddressIndex, taskIdentifierIndex, expandedPhysicalTopology);
		activeTopology = SynefoCoordinatorThread.getInitialActiveTopologyWithJoinOperators(
				updatedTopology, 
				ScaleFunction.getInverseTopology(updatedTopology), taskToJoinRelation);
		scaleFunction = new ScaleFunction(updatedTopology, activeTopology, taskToJoinRelation);
		String activeJoiner = "";
		Integer identifier = -1;
		String relation = "";
		Iterator<Entry<String, ArrayList<String>>> itr = activeTopology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			if(pair.getKey().contains("joiner")) {
				activeJoiner = pair.getKey();
				identifier = Integer.parseInt(activeJoiner.split("[:@]")[1]);
				if(identifier > 3)
					relation = "S";
				else
					relation = "R";
				break;
			}
		}
		/**
		 * Should return NULL value (only 1 node active)
		 */
		String scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", activeJoiner);
		assertEquals(scaleInCommand, "");
		/**
		 * The next 2 calls to produceScaleOutCommand should return non-null values
		 */
		String scaleOutCommand = scaleFunction.produceScaleOutCommand("dispatcher_7:7@1.1.1.1", activeJoiner);
		assertTrue(scaleOutCommand.length() > 0);
		scaleOutCommand = scaleFunction.produceScaleOutCommand("dispatcher_7:7@1.1.1.1", activeJoiner);
		assertNotEquals(scaleOutCommand, "");
		/**
		 * The next 1 call to produceScaleOutCommand should return null value (no-more in-active tasks available)
		 */
		scaleOutCommand = scaleFunction.produceScaleOutCommand("dispatcher_7:7@1.1.1.1", activeJoiner);
		assertEquals(scaleOutCommand, "");
		
		/**
		 * The next call should not return a null command
		 */
		scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", activeJoiner);
		assertNotEquals(scaleInCommand, "");
		
		if(relation.equals("S")) {
			/**
			 * The next call should not return a null command
			 */
			if(activeTopology.containsKey("joiner_4:4@1.1.1.1")) {
				scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_4:4@1.1.1.1");
				assertNotEquals(scaleInCommand, "");
			}else if(activeTopology.containsKey("joiner_5:5@1.1.1.1")) {
				scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_5:5@1.1.1.1");
				assertNotEquals(scaleInCommand, "");
			}else {
				scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_6:6@1.1.1.1");
				assertNotEquals(scaleInCommand, "");
			}
			/**
			 * The next call should return a null command
			 */
			if(activeTopology.containsKey("joiner_4:4@1.1.1.1")) {
				scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_4:4@1.1.1.1");
				assertEquals(scaleInCommand, "");
			}else if(activeTopology.containsKey("joiner_5:5@1.1.1.1")) {
				scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_5:5@1.1.1.1");
				assertEquals(scaleInCommand, "");
			}else {
				scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_6:6@1.1.1.1");
				assertEquals(scaleInCommand, "");
			}
		}else {
			/**
			 * The next call should not return a null command
			 */
			if(activeTopology.containsKey("joiner_1:1@1.1.1.1")) {
				scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_1:1@1.1.1.1");
				assertNotEquals(scaleInCommand, "");
			}else if(activeTopology.containsKey("joiner_2:2@1.1.1.1")) {
				scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_2:2@1.1.1.1");
				assertNotEquals(scaleInCommand, "");
			}else {
				scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_3:3@1.1.1.1");
				assertNotEquals(scaleInCommand, "");
			}
			/**
			 * The next call should return a null command
			 */
			if(activeTopology.containsKey("joiner_1:1@1.1.1.1")) {
				scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_1:1@1.1.1.1");
				assertEquals(scaleInCommand, "");
			}else if(activeTopology.containsKey("joiner_2:2@1.1.1.1")) {
				scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_2:2@1.1.1.1");
				assertEquals(scaleInCommand, "");
			}else {
				scaleInCommand = scaleFunction.produceScaleInCommand("dispatcher_7:7@1.1.1.1", "joiner_3:3@1.1.1.1");
				assertEquals(scaleInCommand, "");
			}
		}
		scaleInCommand = scaleFunction.produceScaleInCommand("spout_9:9@1.1.1.1", "dispatcher_7:7@1.1.1.1");
		assertEquals(scaleInCommand, "");
		
		scaleOutCommand = scaleFunction.produceScaleOutCommand("spout_9:9@1.1.1.1", "dispatcher_7:7@1.1.1.1");
		assertNotEquals(scaleOutCommand, "");
		
		scaleInCommand = scaleFunction.produceScaleInCommand("spout_9:9@1.1.1.1", "dispatcher_7:7@1.1.1.1");
		assertNotEquals(scaleInCommand, "");
	}

}
