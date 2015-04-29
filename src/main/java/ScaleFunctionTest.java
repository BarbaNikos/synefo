import gr.katsip.synefo.server.ScaleFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;


public class ScaleFunctionTest {

	public static void main(String[] args) {
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> taskList;
		taskList = new ArrayList<String>();
		taskList.add("project_bolt_1");
		taskList.add("project_bolt_2");
		taskList.add("project_bolt_3");
		topology.put("spout_1", new ArrayList<String>(taskList));
		taskList = new ArrayList<String>();
		taskList.add("join_bolt_1");
		taskList.add("join_bolt_2");
		taskList.add("join_bolt_3");
		topology.put("spout_2", new ArrayList<String>(taskList));
		taskList = new ArrayList<String>();
		taskList.add("join_bolt_1");
		taskList.add("join_bolt_2");
		taskList.add("join_bolt_3");
		topology.put("project_bolt_1", new ArrayList<String>(taskList));
		topology.put("project_bolt_2", new ArrayList<String>(taskList));
		topology.put("project_bolt_3", new ArrayList<String>(taskList));
		taskList = null;
		taskList = new ArrayList<String>();
		taskList.add("count_group_by_bolt_1");
		taskList.add("count_group_by_bolt_2");
		taskList.add("count_group_by_bolt_3");
		topology.put("join_bolt_1", new ArrayList<String>(taskList));
		topology.put("join_bolt_2", new ArrayList<String>(taskList));
		topology.put("join_bolt_3", new ArrayList<String>(taskList));
		taskList = null;
		taskList = new ArrayList<String>();
		taskList.add("drain_bolt");
		topology.put("count_group_by_bolt_1", taskList);
		topology.put("count_group_by_bolt_2", taskList);
		topology.put("count_group_by_bolt_3", taskList);
		
		HashMap<String, ArrayList<String>> activeTopology = ScaleFunction.getInitialActiveTopology(
				topology, ScaleFunction.getInverseTopology(topology));
		System.out.println("Physical topology: ");
		Iterator<Entry<String, ArrayList<String>>> itr = topology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			System.out.print(pair.getKey() + " -> {");
			for(String downTask : pair.getValue()) {
				System.out.print(downTask + " ");
			}
			System.out.println("}");
		}
		System.out.println("");
		System.out.println("Inverse topology: ");
		itr = ScaleFunction.getInverseTopology(topology).entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			System.out.print(pair.getKey() + " -> {");
			for(String downTask : pair.getValue()) {
				System.out.print(downTask + " ");
			}
			System.out.println("}");
		}
		System.out.println("");
		System.out.println("Topology layers: ");
		itr = ScaleFunction.produceTopologyLayers(topology, ScaleFunction
				.getInverseTopology(topology)).entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			System.out.print(pair.getKey() + " -> {");
			for(String downTask : pair.getValue()) {
				System.out.print(downTask + " ");
			}
			System.out.println("}");
		}
		System.out.println("");
		System.out.println("Initial active topology: ");
		itr = activeTopology.entrySet().iterator();
		String joinActiveBolt = "";
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> pair = itr.next();
			if(pair.getKey().contains("join"))
				joinActiveBolt = pair.getKey();
			System.out.print(pair.getKey() + "{");
			for(String downTask : pair.getValue()) {
				System.out.print(downTask + " ");
			}
			System.out.println("}");
		}
//		
//		ScaleFunction scaleFunction = new ScaleFunction(physicalTopology, activeTopology);
//		String scaleCommand = scaleFunction.produceScaleOutCommand("spout_1b", joinActiveBolt);
//		String addCommand = ScaleFunction.produceActivateCommand(scaleCommand);
//		System.out.println("scale-out command: " + scaleCommand + ", activate command: " + addCommand);
//		System.out.println("");
//		System.out.println("Initial active topology: ");
//		itr = activeTopology.entrySet().iterator();
//		while(itr.hasNext()) {
//			Entry<String, ArrayList<String>> pair = itr.next();
//			if(pair.getKey().contains("join"))
//				joinActiveBolt = pair.getKey();
//			System.out.print(pair.getKey() + "{");
//			for(String downTask : pair.getValue()) {
//				System.out.print(downTask + " ");
//			}
//			System.out.println("}");
//		}
//		
//		scaleCommand = scaleFunction.produceScaleInCommand("spout_2a", joinActiveBolt);
//		String removeCommand = ScaleFunction.produceDeactivateCommand(scaleCommand);
//		System.out.println("scale-out command: " + scaleCommand + ", activate command: " + removeCommand);
//		System.out.println("");
//		System.out.println("Initial active topology: ");
//		itr = activeTopology.entrySet().iterator();
//		while(itr.hasNext()) {
//			Entry<String, ArrayList<String>> pair = itr.next();
//			if(pair.getKey().contains("join"))
//				joinActiveBolt = pair.getKey();
//			System.out.print(pair.getKey() + "{");
//			for(String downTask : pair.getValue()) {
//				System.out.print(downTask + " ");
//			}
//			System.out.println("}");
//		}
//		scaleCommand = scaleFunction.produceScaleInCommand("spout_2a", joinActiveBolt);
//		removeCommand = ScaleFunction.produceDeactivateCommand(scaleCommand);
//		System.out.println("Active Topology: ");
//		itr = activeTopology.entrySet().iterator();
//		while(itr.hasNext()) {
//			Entry<String, ArrayList<String>> pair = itr.next();
//			if(pair.getKey().contains("join"))
//				joinActiveBolt = pair.getKey();
//			System.out.print(pair.getKey() + "{");
//			for(String downTask : pair.getValue()) {
//				System.out.print(downTask + " ");
//			}
//			System.out.println("}");
//		}
	}

}
