package gr.katsip.synefo.junit.test;

import static org.junit.Assert.*;
import gr.katsip.synefo.server.SynefoCoordinatorThread;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

public class PhysicalTopologyExpansionTest {

	@Test
	public void test() {
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> taskList;
		taskList = new ArrayList<String>();
		taskList.add("project");
		topology.put("spout-1", new ArrayList<String>(taskList));
		taskList = new ArrayList<String>();
		taskList.add("join");
		topology.put("spout-2", new ArrayList<String>(taskList));
		taskList = new ArrayList<String>();
		taskList.add("join");
		topology.put("project", new ArrayList<String>(taskList));
		taskList = new ArrayList<String>();
		taskList.add("count-groupby");
		topology.put("join", new ArrayList<String>(taskList));
		taskList = new ArrayList<String>();
		taskList.add("drain");
		topology.put("count-groupby", taskList);
		topology.put("drain", new ArrayList<String>());
		
		HashMap<String, Integer> taskNameToIdMap = new HashMap<String, Integer>();
		
		taskNameToIdMap.put("count-groupby_16", 16);
		taskNameToIdMap.put("count-groupby_14", 14);
		taskNameToIdMap.put("spout-1_26", 26);
		taskNameToIdMap.put("project_23", 23);
		taskNameToIdMap.put("project_24", 24);
		taskNameToIdMap.put("join_21", 21);
		taskNameToIdMap.put("project_25", 25);
		taskNameToIdMap.put("count-groupby_13", 13);
		taskNameToIdMap.put("join_20", 20);
		taskNameToIdMap.put("join_18", 18);
		taskNameToIdMap.put("join_22", 22);
		taskNameToIdMap.put("join_19", 19);
		taskNameToIdMap.put("drain_17", 17);
		taskNameToIdMap.put("spout-2_27", 27);
		taskNameToIdMap.put("count-groupby_15", 15);
		
		HashMap<String, ArrayList<String>> physicalTopologyWithIds = SynefoCoordinatorThread.physicalTopologyTaskExpand(taskNameToIdMap, 
				topology);
		ArrayList<String> joinDownstreamTasks = physicalTopologyWithIds.get("join_19");
		assertEquals(joinDownstreamTasks.size(), 4);
		joinDownstreamTasks = physicalTopologyWithIds.get("join_18");
		assertEquals(joinDownstreamTasks.size(), 4);
	}

}
