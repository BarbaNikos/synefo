package gr.katsip.synefo.TopologyXMLParser;

//import gr.katsip.synefo.storm.api.SynEFOBolt;
import gr.katsip.synefo.storm.api.SynEFOSpout;
import gr.katsip.synefo.storm.lib.SynEFOMessage;
import gr.katsip.synefo.storm.lib.Topology;
import gr.katsip.synefo.storm.operators.SampleTupleProducer;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
//import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class SynEFOTopologyBuilder {
	
	private Topology _topology;
	
	private List<Component> components;
	
//	private List<ScaleOutEvent> events;
	
	private String synEFOhost;
	
	private String synEFOport;
	
	private TopologyBuilder builder;
	
    public void build(String topology_xml_file) {
    	_topology = new Topology();
    	TopologyParser parser = new TopologyParser();
    	parser.parseTopology(topology_xml_file);
    	components = parser.getComponents();
//    	events = parser.getScaleOutEvents();
    	synEFOhost = parser.getSynEFOhost();
    	synEFOport = parser.getSynEFOport();
    	Iterator<Component> itr = components.iterator();
    	builder = new TopologyBuilder();
    	System.out.println("Components:");
    	while(itr.hasNext()) {
    		Component comp = itr.next();
    		System.out.println(comp.toString());
    		if(comp.getType().equals("Spout")) {
    			builder.setSpout(comp.getName(), new SynEFOSpout(comp.getName(), synEFOhost, Integer.parseInt(synEFOport), 
    					new SampleTupleProducer()), comp.getExecutors());
    			_topology._topology.put(comp.getName(), new ArrayList<String>());
    		}else if(comp.getType().equals("Bolt")) {
    			//TODO: Need to fix the following
//    			BoltDeclarer declarer = 
//    					builder.setBolt(comp.getName(), new SynEFOBolt(comp.getName(), synEFOhost, Integer.parseInt(synEFOport), 
//    							new SampleOperator(), comp.getStat_report_timestamp()), comp.getExecutors());
    			_topology._topology.put(comp.getName(), new ArrayList<String>());
    			if(comp.getUpstreamTasks() != null && comp.getUpstreamTasks().size() > 0) {
    				for(String upstream_task : comp.getUpstreamTasks()) {
    					//TODO: Fix the following
//    					declarer.directGrouping(upstream_task);
    					if(_topology._topology.containsKey(upstream_task)) {
    						ArrayList<String> down_tasks = _topology._topology.get(upstream_task);
    						down_tasks.add(comp.getName());
    						_topology._topology.put(upstream_task, down_tasks);
    					}else {
    						ArrayList<String> down_tasks = new ArrayList<String>();
    						down_tasks.add(comp.getName());
    						_topology._topology.put(upstream_task, down_tasks);
    					}
    				}
    				
    			}
    		}
    	}
    	ArrayList<String> tmp = new ArrayList<String>();
    	tmp.add(Integer.toString(components.size()));
    	_topology._topology.put("TASK_NUM", tmp);
    	System.out.println("Topology parsed" + _topology.toString());
//    	Iterator<ScaleOutEvent> itr2 = events.iterator();
//    	System.out.println("Events:");
//    	while(itr2.hasNext()) {
//    		System.out.println(itr2.next().toString());
//    	}
    }
    
    public void submit() throws Exception {
    	System.out.println("About to connect to synEFO: " + synEFOhost + ":" + synEFOport);
		Socket synEFOSocket = new Socket(synEFOhost, Integer.parseInt(synEFOport));
		ObjectOutputStream _out = new ObjectOutputStream(synEFOSocket.getOutputStream());
		ObjectInputStream _in = new ObjectInputStream(synEFOSocket.getInputStream());
		SynEFOMessage msg = new SynEFOMessage();
		msg._values = new HashMap<String, String>();
		msg._values.put("TASK_TYPE", "TOPOLOGY");
		_out.writeObject(msg);
		_out.flush();
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		_out.writeObject(_topology);
		_out.flush();
		String _ack = null;
		try {
			_ack = (String) _in.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		if(_ack.equals("+EFO_ACK") == false) {
			System.err.println("+EFO returned different message other than +EFO_ACK");
			System.exit(1);
		}
		_in.close();
		_out.close();
		synEFOSocket.close();
		
		Config conf = new Config();
		conf.setDebug(true);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("scale-out-test", conf, builder.createTopology());
		Utils.sleep(100000);
		cluster.killTopology("scale-out-test");
		cluster.shutdown();
    }
    
}
