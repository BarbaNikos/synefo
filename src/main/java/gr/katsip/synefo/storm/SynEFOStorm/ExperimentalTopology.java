package gr.katsip.synefo.storm.SynEFOStorm;

import gr.katsip.synefo.storm.api.SynEFOBolt;
import gr.katsip.synefo.storm.api.SynEFOSpout;
import gr.katsip.synefo.storm.lib.SynEFOMessage;
import gr.katsip.synefo.storm.operators.SampleTupleProducer;
import gr.katsip.synefo.storm.operators.relational.CountGroupByAggrOperator;
import gr.katsip.synefo.storm.operators.relational.EquiJoinOperator;
import gr.katsip.synefo.storm.operators.relational.FilterOperator;
import gr.katsip.synefo.storm.operators.relational.StringComparator;

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

public class ExperimentalTopology {
	public static void main(String[] args) throws Exception {
		String synEFO_ip = "";
		Integer synEFO_port = -1;
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp;
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
		String[] spoutSchema = { "name" };
		tuple_producer.setSchema(new Fields(spoutSchema));
		builder.setSpout("spout_1", new SynEFOSpout("spout_1", synEFO_ip, synEFO_port, tuple_producer), 1);
		_tmp = new ArrayList<String>();
		_tmp.add("select_bolt_1");
		_tmp.add("select_bolt_2");
		topology.put("spout_1", new ArrayList<String>(_tmp));
		/**
		 * Stage 1: Select operators
		 */
		FilterOperator<String> filterOperator = new FilterOperator<String>(new StringComparator(), "name", "nathan");
		String[] filterOutSchema = { "name" };
		filterOperator.setOutputSchema(new Fields(filterOutSchema));
		builder.setBolt("select_bolt_1", new SynEFOBolt("select_bolt_1", synEFO_ip, synEFO_port, filterOperator), 1).directGrouping("spout_1");
		filterOperator = new FilterOperator<String>(new StringComparator(), "name", "nathan");
		filterOperator.setOutputSchema(new Fields(filterOutSchema));
		builder.setBolt("select_bolt_2", new SynEFOBolt("select_bolt_2", synEFO_ip, synEFO_port, filterOperator), 1).directGrouping("spout_1");
		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		_tmp.add("join_bolt_2");
		topology.put("select_bolt_1", new ArrayList<String>(_tmp));
		topology.put("select_bolt_2", new ArrayList<String>(_tmp));
		_tmp = null;
		/**
		 * Stage 2: Join operators
		 */
		EquiJoinOperator<String> equi_join_op = new EquiJoinOperator<String>(new StringComparator(), 1000, "name");
		String[] join_schema = { "name-a", "name-b" };
		String[] state_schema = { "name", "time" };
		equi_join_op.setOutputSchema(new Fields(join_schema));
		equi_join_op.setStateSchema(new Fields(state_schema));
		builder.setBolt("join_bolt_1", new SynEFOBolt("join_bolt_1", synEFO_ip, synEFO_port, equi_join_op), 1).directGrouping("select_bolt_1").directGrouping("select_bolt_2");
		equi_join_op = new EquiJoinOperator<String>(new StringComparator(), 1000, "name");
		equi_join_op.setOutputSchema(new Fields(join_schema));
		equi_join_op.setStateSchema(new Fields(state_schema));
		builder.setBolt("join_bolt_2", new SynEFOBolt("join_bolt_2", synEFO_ip, synEFO_port, equi_join_op), 1).directGrouping("select_bolt_1").directGrouping("select_bolt_2");
		_tmp = new ArrayList<String>();
		_tmp.add("count_group_by_bolt_1");
		topology.put("join_bolt_1", new ArrayList<String>(_tmp));
		topology.put("join_bolt_2", new ArrayList<String>(_tmp));
		_tmp = null;
		/**
		 * Stage 3: Aggregate operator
		 */
		CountGroupByAggrOperator countGroupByAggrOperator = new CountGroupByAggrOperator(1000, join_schema);
		String[] countGroupBySchema = { "key", "count" };
		String[] countGroupByStateSchema = { "key", "count", "time" };
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count_group_by_bolt_1", new SynEFOBolt("count_group_by_bolt_1", synEFO_ip, synEFO_port, countGroupByAggrOperator), 1)
		.directGrouping("join_bolt_1").directGrouping("join_bolt_2");
		topology.put("count_group_by_bolt_1", new ArrayList<String>());
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
		_out.writeObject(topology);
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
		Utils.sleep(100000);
		cluster.killTopology("scale-out-test");
		cluster.shutdown();
	}
}
