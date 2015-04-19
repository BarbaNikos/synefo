import gr.katsip.synefo.server.ScaleFunction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;


public class ScaleFunctionTest {

	public static void main(String[] args) {
		HashMap<String, ArrayList<String>> physicalTopology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp;
		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		_tmp.add("join_bolt_2");
		physicalTopology.put("spout_1a", new ArrayList<String>(_tmp));
		physicalTopology.put("spout_1b", new ArrayList<String>(_tmp));
		physicalTopology.put("spout_2a", new ArrayList<String>(_tmp));
		physicalTopology.put("spout_2b", new ArrayList<String>(_tmp));
		_tmp = new ArrayList<String>();
		_tmp.add("drain_bolt");
		physicalTopology.put("join_bolt_1", new ArrayList<String>(_tmp));
		physicalTopology.put("join_bolt_2", new ArrayList<String>(_tmp));
		physicalTopology.put("drain_bolt", new ArrayList<String>());
		
		HashMap<String, ArrayList<String>> activeTopology = ScaleFunction.getInitialActiveTopology(
				physicalTopology, ScaleFunction.getInverseTopology(physicalTopology));
		System.out.println("Physical topology: ");
		Iterator<Entry<String, ArrayList<String>>> itr = physicalTopology.entrySet().iterator();
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
		itr = ScaleFunction.getInverseTopology(physicalTopology).entrySet().iterator();
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
		itr = ScaleFunction.produceTopologyLayers(physicalTopology, ScaleFunction
				.getInverseTopology(physicalTopology)).entrySet().iterator();
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
		
		ScaleFunction scaleFunction = new ScaleFunction(physicalTopology, activeTopology);
		String scaleCommand = scaleFunction.produceScaleOutCommand("spout_1b", joinActiveBolt);
		String addCommand = scaleFunction.produceActivateCommand(scaleCommand);
		System.out.println("scale-out command: " + scaleCommand + ", activate command: " + addCommand);
		System.out.println("");
		System.out.println("Initial active topology: ");
		itr = activeTopology.entrySet().iterator();
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
		
		scaleCommand = scaleFunction.produceScaleInCommand("spout_2a", joinActiveBolt);
		String removeCommand = scaleFunction.produceDeactivateCommand(scaleCommand);
		System.out.println("scale-out command: " + scaleCommand + ", activate command: " + removeCommand);
		System.out.println("");
		System.out.println("Initial active topology: ");
		itr = activeTopology.entrySet().iterator();
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
		scaleCommand = scaleFunction.produceScaleInCommand("spout_2a", joinActiveBolt);
		removeCommand = scaleFunction.produceDeactivateCommand(scaleCommand);
		System.out.println("Active Topology: ");
		itr = activeTopology.entrySet().iterator();
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
	}

}
