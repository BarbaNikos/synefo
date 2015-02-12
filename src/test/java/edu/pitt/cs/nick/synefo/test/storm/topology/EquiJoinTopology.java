package edu.pitt.cs.nick.synefo.test.storm.topology;

import gr.katsip.synefo.storm.api.SynEFOBolt;
import gr.katsip.synefo.storm.api.SynEFOSpout;
import gr.katsip.synefo.storm.lib.SynEFOMessage;
import gr.katsip.synefo.storm.operators.relational.EquiJoinOperator;
import gr.katsip.synefo.storm.operators.relational.StringComparator;
import gr.katsip.synefo.storm.producers.SampleTupleProducer;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class EquiJoinTopology {
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
		_tmp.add("join_bolt");
		_topology.put("spout_1", new ArrayList<String>(_tmp));
		_tmp = null;
		_tmp = new ArrayList<String>();
		/**
		 * Stage 2
		 */
		EquiJoinOperator<String> equi_join_op = new EquiJoinOperator<String>(new StringComparator(), 5, "name");
		String[] join_schema = { "name-a", "name-b" };
		equi_join_op.setOutputSchema(new Fields(join_schema));
		builder.setBolt("join_bolt", new SynEFOBolt("join_bolt", synEFO_ip, synEFO_port, equi_join_op), 1).directGrouping("spout_1");
		_topology.put("join_bolt", new ArrayList<String>());
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
