package edu.pitt.cs.nick.synefo.test.storm.topology;
//package gr.katsip.synefo.storm.SynEFOStorm;
//
//import gr.katsip.synefo.storm.api.SynEFOBolt;
//import gr.katsip.synefo.storm.api.SynEFOSpout;
//import gr.katsip.synefo.storm.lib.SynEFOMessage;
//import gr.katsip.synefo.storm.lib.Topology;
//import gr.katsip.synefo.storm.operators.CombineOperator;
//import gr.katsip.synefo.storm.operators.GroupByOperator;
//import gr.katsip.synefo.storm.operators.SampleTupleProducer;
//import gr.katsip.synefo.storm.operators.SplitOperator;
//
//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
//import java.net.Socket;
//import java.net.UnknownHostException;
//import java.util.ArrayList;
//import java.util.HashMap;
//
//import backtype.storm.Config;
//import backtype.storm.LocalCluster;
//import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.utils.Utils;
//
//public class SampleWordCountTopology {
//
//	public static void main(String[] args) throws UnknownHostException, IOException, ClassNotFoundException {
//		String synEFO_ip = "";
//		Integer synEFO_port = -1;
//		Topology _topology = new Topology();
//		ArrayList<String> _tmp = new ArrayList<String>();
//		if(args.length < 2) {
//			System.err.println("Arguments: <synEFO_ip> <synEFO_port>");
//			System.exit(1);
//		}else {
//			synEFO_ip = args[0];
//			synEFO_port = Integer.parseInt(args[1]);
//		}
//		
//		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("spout_1", new SynEFOSpout("spout_1", synEFO_ip, synEFO_port, new SampleTupleProducer(), 10), 1);
//		_tmp.add("bolt_1");
//		_tmp.add("bolt_2");
//		_topology._topology.put("spout_1", new ArrayList<String>(_tmp));
//		_tmp = null;
//		_tmp = new ArrayList<String>();
//		/**
//		 * Stage 1: Word Splitting
//		 */
//		builder.setBolt("bolt_1", new SynEFOBolt("bolt_1", synEFO_ip, synEFO_port, new SplitOperator(" "), 10), 2).directGrouping("spout_1");
//		_tmp.add("bolt_3");
//		_tmp.add("bolt_4");
//		_topology._topology.put("bolt_1", new ArrayList<String>(_tmp));
//		builder.setBolt("bolt_2", new SynEFOBolt("bolt_2", synEFO_ip, synEFO_port, new SplitOperator(" "), 10), 2).directGrouping("spout_1");
//		_topology._topology.put("bolt_2", new ArrayList<String>(_tmp));
//		_tmp = null;
//		/**
//		 * Stage 2: Group-By Operator
//		 */
//		builder.setBolt("bolt_3", new SynEFOBolt("bolt_3", synEFO_ip, synEFO_port, new GroupByOperator(), 10), 2).directGrouping("bolt_1")
//				.directGrouping("bolt_2");
//		builder.setBolt("bolt_4", new SynEFOBolt("bolt_4", synEFO_ip, synEFO_port, new GroupByOperator(), 10), 2).directGrouping("bolt_1")
//		.directGrouping("bolt_2");
//		_tmp = null;
//		_tmp = new ArrayList<String>();
//		_tmp.add("bolt_5");
//		_tmp.add("bolt_5");
//		_topology._topology.put("bolt_3", new ArrayList<String>(_tmp));
//		_topology._topology.put("bolt_4", new ArrayList<String>(_tmp));
//		builder.setBolt("bolt_5", new SynEFOBolt("bolt_5", synEFO_ip, synEFO_port, new CombineOperator(), 10), 1).directGrouping("bolt_3")
//				.directGrouping("bolt_4");
//		_tmp = null;
//		_topology._topology.put("bolt_5", new ArrayList<String>());
//		_tmp = new ArrayList<String>();
//		_tmp.add(Integer.toString(6));
//		_topology._topology.put("TASK_NUM", _tmp);
//		/**
//		 * Notify SynEFO server about the Topology
//		 */
//		System.out.println("About to connect to synEFO: " + synEFO_ip + ":" + synEFO_port);
//		Socket synEFOSocket = new Socket(synEFO_ip, synEFO_port);
//		ObjectOutputStream _out = new ObjectOutputStream(synEFOSocket.getOutputStream());
//		ObjectInputStream _in = new ObjectInputStream(synEFOSocket.getInputStream());
//		SynEFOMessage msg = new SynEFOMessage();
//		msg._values = new HashMap<String, String>();
//		msg._values.put("TASK_TYPE", "TOPOLOGY");
//		_out.writeObject(msg);
//		_out.flush();
//		_out.writeObject(_topology);
//		_out.flush();
//		String _ack = null;
//		_ack = (String) _in.readObject();
//		if(_ack.equals("+EFO_ACK") == false) {
//			System.err.println("+EFO returned different message other than +EFO_ACK");
//			System.exit(1);
//		}
//		_in.close();
//		_out.close();
//		synEFOSocket.close();
//		
//		Config conf = new Config();
//		conf.setDebug(true);
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("word-count", conf, builder.createTopology());
//		Utils.sleep(100000);
//		cluster.killTopology("word-count");
//		cluster.shutdown();
//	}
//
//}
