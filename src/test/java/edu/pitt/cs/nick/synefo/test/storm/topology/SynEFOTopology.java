package edu.pitt.cs.nick.synefo.test.storm.topology;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import gr.katsip.synefo.storm.api.SynEFOBolt;
import gr.katsip.synefo.storm.api.SynEFOSpout;
import gr.katsip.synefo.storm.lib.SynEFOMessage;
import gr.katsip.synefo.storm.operators.SampleTupleProducer;
import gr.katsip.synefo.storm.operators.relational.EquiJoinOperator;
import gr.katsip.synefo.storm.operators.relational.FilterOperator;
import gr.katsip.synefo.storm.operators.relational.StringComparator;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


public class SynEFOTopology {
	
	public static void main(String[] args) throws Exception {
		String synEFO_ip = "";
		Integer synEFO_port = -1;
		HashMap<String, ArrayList<String>> _topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp = new ArrayList<String>();
		if(args.length < 2) {
			System.err.println("Arguments: <synEFO_ip> <synEFO_port>");
			System.exit(1);
		}else {
			synEFO_ip = args[0];
			synEFO_port = Integer.parseInt(args[1]);
		}
		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		SampleTupleProducer tuple_producer = new SampleTupleProducer();
		builder.setSpout("spout_1", new SynEFOSpout("spout_1", synEFO_ip, synEFO_port, tuple_producer), 1);
		_tmp.add("filter_bolt_1");
		_tmp.add("filter_bolt_2");
		_topology.put("spout_1", new ArrayList<String>(_tmp));
		_tmp = null;
		_tmp = new ArrayList<String>();
		/**
		 * Stage 1
		 */
		FilterOperator<String> filter_op = new FilterOperator<String>(new StringComparator(), "name", "nathan");
		String[] filter_schema = { "name" };
		filter_op.setOutputSchema(new Fields(filter_schema));
		builder.setBolt("filter_bolt_1", new SynEFOBolt("filter_bolt_1", synEFO_ip, synEFO_port, filter_op), 1).directGrouping("spout_1");
		_tmp.add("join_bolt_3");
		_topology.put("filter_bolt_1", new ArrayList<String>(_tmp));
		FilterOperator<String> filter_op_1 = new FilterOperator<String>(new StringComparator(), "name", "nathan");
		String[] filter_schema_1 = { "name" };
		filter_op_1.setOutputSchema(new Fields(filter_schema_1));
		builder.setBolt("filter_bolt_2", new SynEFOBolt("filter_bolt_2", synEFO_ip, synEFO_port, filter_op_1), 1).directGrouping("spout_1");
		_topology.put("filter_bolt_2", new ArrayList<String>(_tmp));
		_tmp = null;
		/**
		 * Stage 2
		 */
		EquiJoinOperator<String> equi_join_op = new EquiJoinOperator<String>(new StringComparator(), 5, "name");
		String[] join_schema = { "name-a", "name-b" };
		equi_join_op.setOutputSchema(new Fields(join_schema));
		builder.setBolt("join_bolt_3", new SynEFOBolt("join_bolt_3", synEFO_ip, synEFO_port, equi_join_op), 1).directGrouping("filter_bolt_1")
				.directGrouping("filter_bolt_2");
		_topology.put("join_bolt_3", new ArrayList<String>());
		_tmp = null;
		_tmp = new ArrayList<String>();
		/**
		 * Notify SynEFO server about the 
		 * Topology
		 */
		System.out.println("About to connect to synEFO: " + synEFO_ip + ":" + synEFO_port);
		Socket synEFOSocket = new Socket(synEFO_ip, synEFO_port);
		ObjectOutputStream _out = new ObjectOutputStream(synEFOSocket.getOutputStream());
		ObjectInputStream _in = new ObjectInputStream(synEFOSocket.getInputStream());
		SynEFOMessage msg = new SynEFOMessage();
		msg._values = new HashMap<String, String>();
		msg._values.put("TASK_TYPE", "TOPOLOGY");
		_out.writeObject(msg);
		_out.flush();
		Thread.sleep(100);
		_out.writeObject(_topology);
		_out.flush();
		String _ack = null;
		_ack = (String) _in.readObject();
		if(_ack.equals("+EFO_ACK") == false) {
			System.err.println("+EFO returned different message other than +EFO_ACK");
			System.exit(1);
		}
		_in.close();
		_out.close();
		synEFOSocket.close();
		
		
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("scale-out-test", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("scale-out-test");
		cluster.shutdown();
	}
}
