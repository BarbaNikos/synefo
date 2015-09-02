package gr.katsip.synefo.junit.test;

import gr.katsip.synefo.server2.SynefoMaster;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Test;

public class ScaleFunctionServer2PhysicalTopTest {

	@Test
	public void test() {
		ConcurrentHashMap<String, ArrayList<String>> topology = new ConcurrentHashMap<String, ArrayList<String>>();
		ArrayList<String> taskList;
		taskList = new ArrayList<String>();
		taskList.add("joindispatch");
		topology.put("customer", taskList);
		topology.put("order", new ArrayList<String>(taskList));
		
		taskList = new ArrayList<String>();
		taskList.add("joindispatch2");
		topology.put("supplier", new ArrayList<String>(taskList));
		topology.put("lineitem", new ArrayList<String>(taskList));
		
		taskList = new ArrayList<String>();
		taskList.add("joinjoincust");
		taskList.add("joinjoinorder");
		topology.put("joindispatch", taskList);
		
		taskList = new ArrayList<String>();
		taskList.add("joinjoinline");
		taskList.add("joinjoinsup");
		topology.put("joindispatch2", taskList);
		
		taskList = new ArrayList<String>();
		taskList.add("joindispatch3");
		topology.put("joinjoincust", taskList);
		
		taskList = new ArrayList<String>();
		taskList.add("joindispatch3");
		topology.put("joinjoinorder", taskList);
		
		taskList = new ArrayList<String>();
		taskList.add("joindispatch3");
		topology.put("joinjoinline", new ArrayList<String>(taskList));
		topology.put("joinjoinsup", new ArrayList<String>(taskList));
		
		taskList = new ArrayList<String>();
		taskList.add("joinjoinoutputone");
		taskList.add("joinjoinoutputtwo");
		topology.put("joindispatch3", new ArrayList<String>(taskList));
		
		taskList = new ArrayList<String>();
		taskList.add("drain");
		topology.put("joinjoinoutputone", new ArrayList<String>(taskList));
		topology.put("joinjoinoutputtwo", new ArrayList<String>(taskList));
		
		topology.put("drain", new ArrayList<String>());
		System.out.println(topology.toString());
		
		ConcurrentHashMap<String, Integer> taskIdentifierIndex = new ConcurrentHashMap<String, Integer>();
		ConcurrentHashMap<String, String> taskAddressIndex = new ConcurrentHashMap<String, String>();
		taskIdentifierIndex.put("order_1", 1);
		taskAddressIndex.put("order_1:1", "1.1.1.1");
		taskIdentifierIndex.put("customer_2", 2);
		taskAddressIndex.put("customer_2:2", "1.1.1.1");
		taskIdentifierIndex.put("supplier_3", 3);
		taskAddressIndex.put("supplier_3:3", "1.1.1.1");
		taskIdentifierIndex.put("lineitem_4", 4);
		taskAddressIndex.put("lineitem_4:4", "1.1.1.1");
		
		taskIdentifierIndex.put("joindispatch_5", 5);
		taskAddressIndex.put("joindispatch_5:5", "1.1.1.1");
		
		taskIdentifierIndex.put("joindispatch2_6", 6);
		taskAddressIndex.put("joindispatch2_6:6", "1.1.1.1");
		taskIdentifierIndex.put("joinjoincust_7", 7);
		taskAddressIndex.put("joinjoincust_7:7", "1.1.1.1");
		taskIdentifierIndex.put("joinjoinorder_8", 8);
		taskAddressIndex.put("joinjoinorder_8:8", "1.1.1.1");
		taskIdentifierIndex.put("joinjoinline_9", 9);
		taskAddressIndex.put("joinjoinline_9:9", "1.1.1.1");
		taskIdentifierIndex.put("joinjoinsup_10", 10);
		taskAddressIndex.put("joinjoinsup_10:10", "1.1.1.1");
		taskIdentifierIndex.put("joindispatch3_11", 11);
		taskAddressIndex.put("joindispatch3_11:11", "1.1.1.1");
		taskIdentifierIndex.put("joinjoinoutputone_12", 12);
		taskAddressIndex.put("joinjoinoutputone_12:12", "1.1.1.1");
		taskIdentifierIndex.put("joinjoinoutputtwo_13", 13);
		taskAddressIndex.put("joinjoinoutputtwo_13:13", "1.1.1.1");
		taskIdentifierIndex.put("drain_14", 14);
		taskAddressIndex.put("drain_14:14", "1.1.1.1");
		
		ConcurrentHashMap<String, ArrayList<String>> expandedPhysicalTopology = SynefoMaster.physicalTopologyTaskExpand(
				taskIdentifierIndex, topology);
		
		System.out.println("expanded-phys-top: " + expandedPhysicalTopology.toString());
		
		topology.clear();
		topology.putAll(expandedPhysicalTopology);
		ConcurrentHashMap<String, ArrayList<String>> updatedPhysicalTopology = SynefoMaster.updatePhysicalTopology(
				taskAddressIndex, taskIdentifierIndex, topology);
		System.out.println("updated-phys-top: " + updatedPhysicalTopology);
		
		
	}

}
