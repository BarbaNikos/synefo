package gr.katsip.synefo.storm.topology;

import gr.katsip.synefo.storm.api.SynefoBolt;
import gr.katsip.synefo.storm.api.SynefoSpout;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.operators.relational.CountGroupByAggrOperator;
import gr.katsip.synefo.storm.operators.relational.EquiJoinOperator;
import gr.katsip.synefo.storm.operators.relational.ProjectOperator;
import gr.katsip.synefo.storm.operators.relational.StringComparator;
import gr.katsip.synefo.storm.producers.StreamgenTupleProducer;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import backtype.storm.Config;
//import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class DistributedTopology {

	public static void main(String[] args) throws Exception {
		String synefoIP = "";
		Integer synefoPort = -1;
		String streamIP = "";
		Integer streamPort = -1;
		String zooIP = "";
		Integer zooPort = -1;
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp;
		if(args.length < 6) {
			System.err.println("Arguments: <synefo-IP> <synefo-port> <stream-IP> <stream-port> <zoo-IP> <zoo-port>");
			System.exit(1);
		}else {
			synefoIP = args[0];
			synefoPort = Integer.parseInt(args[1]);
			streamIP = args[2];
			streamPort = Integer.parseInt(args[3]);
			zooIP = args[4];
			zooPort = Integer.parseInt(args[5]);
		}
		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		StreamgenTupleProducer tupleProducer = new StreamgenTupleProducer(streamIP, streamPort);
//		String[] spoutSchema = { "num", "timestamp", "one", "two", "three", "four" };
		String[] spoutSchema = { "num", "one", "two", "three", "four" };
		tupleProducer.setSchema(new Fields(spoutSchema));
		builder.setSpout("spout_1", 
				new SynefoSpout("spout_1", synefoIP, synefoPort, tupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);
		_tmp = new ArrayList<String>();
		_tmp.add("project_bolt_1");
		_tmp.add("project_bolt_2");
		_tmp.add("project_bolt_3");
		topology.put("spout_1", new ArrayList<String>(_tmp));
		/**
		 * Stage 1: Project operators
		 */
//		String[] projectOutSchema = { "num", "timestamp", "one", "two", "three", "four" };
		String[] projectOutSchema = { "num", "one", "two", "three", "four" };
		ProjectOperator projectOperator = new ProjectOperator(new Fields(projectOutSchema));
		projectOperator.setOutputSchema(new Fields(projectOutSchema));
		builder.setBolt("project_bolt_1", 
				new SynefoBolt("project_bolt_1", synefoIP, synefoPort, projectOperator, zooIP, zooPort), 1)
				.setNumTasks(1)
				.directGrouping("spout_1");
		projectOperator = new ProjectOperator(new Fields(projectOutSchema));
		projectOperator.setOutputSchema(new Fields(projectOutSchema));
		builder.setBolt("project_bolt_2", 
				new SynefoBolt("project_bolt_2", synefoIP, synefoPort, projectOperator, zooIP, zooPort), 1)
				.setNumTasks(1)
				.directGrouping("spout_1");
		projectOperator = new ProjectOperator(new Fields(projectOutSchema));
		projectOperator.setOutputSchema(new Fields(projectOutSchema));
		builder.setBolt("project_bolt_3", 
				new SynefoBolt("project_bolt_3", synefoIP, synefoPort, projectOperator, zooIP, zooPort), 1)
				.setNumTasks(1)
				.directGrouping("spout_1");
		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		_tmp.add("join_bolt_2");
		_tmp.add("join_bolt_3");
		topology.put("project_bolt_1", new ArrayList<String>(_tmp));
		topology.put("project_bolt_2", new ArrayList<String>(_tmp));
		topology.put("project_bolt_3", new ArrayList<String>(_tmp));
		_tmp = null;
		/**
		 * Stage 2: Join operators
		 */
		EquiJoinOperator<String> equi_join_op = new EquiJoinOperator<String>(new StringComparator(), 100, "three");
//		String[] join_schema = { "timestamp", "three-a", "three-b" };
		String[] join_schema = { "three-a", "three-b" };
//		String[] state_schema = { "num", "timestamp", "one", "two", "three", "four", "time" };
		String[] state_schema = { "num", "one", "two", "three", "four", "time" };
		equi_join_op.setOutputSchema(new Fields(join_schema));
		equi_join_op.setStateSchema(new Fields(state_schema));
		builder.setBolt("join_bolt_1", 
				new SynefoBolt("join_bolt_1", synefoIP, synefoPort, equi_join_op, zooIP, zooPort), 1)
				.setNumTasks(1)
				.directGrouping("project_bolt_1")
				.directGrouping("project_bolt_2")
				.directGrouping("project_bolt_3");
		equi_join_op = new EquiJoinOperator<String>(new StringComparator(), 100, "three");
		equi_join_op.setOutputSchema(new Fields(join_schema));
		equi_join_op.setStateSchema(new Fields(state_schema));
		builder.setBolt("join_bolt_2", 
				new SynefoBolt("join_bolt_2", synefoIP, synefoPort, equi_join_op, zooIP, zooPort), 1)
				.setNumTasks(1)
				.directGrouping("project_bolt_1")
				.directGrouping("project_bolt_2")
				.directGrouping("project_bolt_3");
		equi_join_op = new EquiJoinOperator<String>(new StringComparator(), 100, "three");
		equi_join_op.setOutputSchema(new Fields(join_schema));
		equi_join_op.setStateSchema(new Fields(state_schema));
		builder.setBolt("join_bolt_3", 
				new SynefoBolt("join_bolt_3", synefoIP, synefoPort, equi_join_op, zooIP, zooPort), 1)
				.setNumTasks(1)
				.directGrouping("project_bolt_1")
				.directGrouping("project_bolt_2")
				.directGrouping("project_bolt_3");
		_tmp = new ArrayList<String>();
		_tmp.add("count_group_by_bolt_1");
		_tmp.add("count_group_by_bolt_2");
		topology.put("join_bolt_1", new ArrayList<String>(_tmp));
		topology.put("join_bolt_2", new ArrayList<String>(_tmp));
		topology.put("join_bolt_3", new ArrayList<String>(_tmp));
		_tmp = null;
		/**
		 * Stage 3: Aggregate operator
		 */
		CountGroupByAggrOperator countGroupByAggrOperator = new CountGroupByAggrOperator(100, join_schema);
		String[] countGroupBySchema = { "key", "count" };
		String[] countGroupByStateSchema = { "key", "count", "time" };
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count_group_by_bolt_1", 
				new SynefoBolt("count_group_by_bolt_1", synefoIP, synefoPort, countGroupByAggrOperator, zooIP, zooPort), 1)
				.setNumTasks(1)
				.directGrouping("join_bolt_1")
				.directGrouping("join_bolt_2")
				.directGrouping("join_bolt_3");
		countGroupByAggrOperator = new CountGroupByAggrOperator(100, join_schema);
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count_group_by_bolt_2", 
				new SynefoBolt("count_group_by_bolt_2", synefoIP, synefoPort, countGroupByAggrOperator, zooIP, zooPort), 1)
				.setNumTasks(1)
				.directGrouping("join_bolt_1")
				.directGrouping("join_bolt_2")
				.directGrouping("join_bolt_3");
		topology.put("count_group_by_bolt_1", new ArrayList<String>());
		topology.put("count_group_by_bolt_2", new ArrayList<String>());
		/**
		 * Notify SynEFO server about the 
		 * Topology
		 */
		System.out.println("About to connect to synefo: " + synefoIP + ":" + synefoPort);
		Socket synEFOSocket = new Socket(synefoIP, synefoPort);
		ObjectOutputStream _out = new ObjectOutputStream(synEFOSocket.getOutputStream());
		ObjectInputStream _in = new ObjectInputStream(synEFOSocket.getInputStream());
		SynefoMessage msg = new SynefoMessage();
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


//		conf.setDebug(true);
		conf.setDebug(false);
		conf.setNumWorkers(9);
		StormSubmitter.submitTopology("dist-top", conf, builder.createTopology());
	}
}
