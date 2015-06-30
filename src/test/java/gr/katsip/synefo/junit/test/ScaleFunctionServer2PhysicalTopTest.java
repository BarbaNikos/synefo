package gr.katsip.synefo.junit.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

public class ScaleFunctionServer2PhysicalTopTest {

	@Test
	public void test() {
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
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
	}

}
