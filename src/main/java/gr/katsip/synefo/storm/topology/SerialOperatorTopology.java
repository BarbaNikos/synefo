package gr.katsip.synefo.storm.topology;

import gr.katsip.synefo.storm.api.OperatorBolt;
import gr.katsip.synefo.storm.api.OperatorSpout;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.operators.relational.CountGroupByAggrOperator;
import gr.katsip.synefo.storm.operators.relational.JoinOperator;
import gr.katsip.synefo.storm.operators.relational.ProjectOperator;
import gr.katsip.synefo.storm.operators.relational.StringComparator;
import gr.katsip.synefo.storm.producers.StreamgenTupleProducer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class SerialOperatorTopology {

	public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException, ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
		String synefoIP = "";
		Integer synefoPort = -1;
		String streamIP = "";
		Integer streamPort = -1;
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp;
		if(args.length < 4) {
			System.err.println("Arguments: <synefo-IP> <synefo-port> <stream-IP> <stream-port>");
			System.exit(1);
		}else {
			synefoIP = args[0];
			synefoPort = Integer.parseInt(args[1]);
			streamIP = args[2];
			streamPort = Integer.parseInt(args[3]);
		}
		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		StreamgenTupleProducer tupleProducer = new StreamgenTupleProducer(streamIP, streamPort);
		String[] spoutSchema = { "one", "two", "three", "four" };
		tupleProducer.setSchema(new Fields(spoutSchema));
		builder.setSpout("spout_1", 
				new OperatorSpout("spout_1", synefoIP, synefoPort, tupleProducer), 1)
				.setNumTasks(1);
		_tmp = new ArrayList<String>();
		_tmp.add("project_bolt_1");
		topology.put("spout_1", new ArrayList<String>(_tmp));
		/**
		 * Stage 1: Project Operators
		 */
		String[] projectOutSchema = { "one", "two", "three", "four" };
		ProjectOperator projectOperator = new ProjectOperator(new Fields(projectOutSchema));
		projectOperator.setOutputSchema(new Fields(projectOutSchema));
		builder.setBolt("project_bolt_1", 
				new OperatorBolt("project_bolt_1", synefoIP, synefoPort, projectOperator), 1)
				.setNumTasks(1)
				.directGrouping("spout_1");
		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		topology.put("project_bolt_1", new ArrayList<String>(_tmp));
		_tmp = null;
		/**
		 * Stage 2: Join operators
		 */
		JoinOperator<String> joinOperator = new JoinOperator<String>(new StringComparator(), 100, "three", 
				new Fields(projectOutSchema), new Fields(projectOutSchema));
		builder.setBolt("join_bolt_1", 
				new OperatorBolt("join_bolt_1", synefoIP, synefoPort, joinOperator), 1)
				.setNumTasks(1)
				.directGrouping("project_bolt_1");
		_tmp = new ArrayList<String>();
		_tmp.add("count_group_by_bolt_1");
		topology.put("join_bolt_1", new ArrayList<String>(_tmp));
		_tmp = null;
		/**
		 * Stage 3: Aggregate operator
		 */
		CountGroupByAggrOperator countGroupByAggrOperator = new CountGroupByAggrOperator(100, 
				joinOperator.getOutputSchema().toList().toArray(
						new String[joinOperator.getOutputSchema().toList().size()]));
		String[] countGroupBySchema = { "key", "count" };
		String[] countGroupByStateSchema = { "key", "count", "time" };
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count_group_by_bolt_1", 
				new OperatorBolt("count_group_by_bolt_1", synefoIP, synefoPort, countGroupByAggrOperator), 1)
				.setNumTasks(1)
				.directGrouping("join_bolt_1");
		topology.put("count_group_by_bolt_1", new ArrayList<String>());
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

		conf.setDebug(false);
		conf.setNumWorkers(4);
		StormSubmitter.submitTopology("operator-serial-top", conf, builder.createTopology());
	}

}