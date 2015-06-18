package gr.katsip.synefo.storm.topology;

import gr.katsip.synefo.storm.api.SynefoBolt;
import gr.katsip.synefo.storm.api.SynefoSpout;
import gr.katsip.synefo.storm.lib.SynefoMessage;
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

public class MinimalTopology {

	public static void main(String[] args) throws UnknownHostException, IOException, 
	InterruptedException, ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
		String synefoIP = "";
		Integer synefoPort = 5555;
		String[] streamIPs = null;
		String zooIP = "";
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp;
		if(args.length < 3) {
			System.err.println("Arguments: <synefo-IP> <stream-IP> <zoo-ip1:port1,zoo-ip2:port2,...,zoo-ipN:portN>");
			System.exit(1);
		}else {
			synefoIP = args[0];
			streamIPs = args[1].split(",");
			zooIP = args[2];
		}
		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		/**
		 * Stage 0: Data Sources
		 */
		String[] spoutSchema = { "num", "one", "two", "three", "four" };
		String[] spoutTwoSchema = { "num", "1", "2", "three", "5" };
		StreamgenTupleProducer tupleProducer = new StreamgenTupleProducer(streamIPs[0]);
		tupleProducer.setSchema(new Fields(spoutSchema));
		builder.setSpout("spout_1a", 
				new SynefoSpout("spout_1a", synefoIP, synefoPort, tupleProducer, zooIP), 1)
				.setNumTasks(1);
		tupleProducer = new StreamgenTupleProducer(streamIPs[1]);
		tupleProducer.setSchema(new Fields(spoutSchema));
		builder.setSpout("spout_1b", 
				new SynefoSpout("spout_1b", synefoIP, synefoPort, tupleProducer, zooIP), 1)
				.setNumTasks(1);
		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		_tmp.add("join_bolt_2");
		topology.put("spout_1a", new ArrayList<String>(_tmp));
		topology.put("spout_1b", new ArrayList<String>(_tmp));
		/**
		 * Stage 1: Join operators
		 */
		JoinOperator<String> joinOperator = new JoinOperator<String>(new StringComparator(), 500, "three", 
				new Fields(spoutSchema), new Fields(spoutTwoSchema));
		builder.setBolt("join_bolt_1", 
				new SynefoBolt("join_bolt_1", synefoIP, synefoPort, 
						joinOperator, zooIP, true), 1)
						.setNumTasks(1)
						.directGrouping("spout_1a")
						.directGrouping("spout_1b");
		joinOperator = new JoinOperator<String>(new StringComparator(), 500, "three", 
				new Fields(spoutSchema), new Fields(spoutTwoSchema));
		builder.setBolt("join_bolt_2", 
				new SynefoBolt("join_bolt_2", synefoIP, synefoPort, 
						joinOperator, zooIP, true), 1)
						.setNumTasks(1)
						.directGrouping("spout_1a")
						.directGrouping("spout_1b");
		_tmp = new ArrayList<String>();
		_tmp.add("drain_bolt");
		topology.put("join_bolt_1", new ArrayList<String>(_tmp));
		topology.put("join_bolt_2", new ArrayList<String>(_tmp));
		/**
		 * Stage 2: Drain Operator (project operator)
		 */
		ProjectOperator projectOperator = new ProjectOperator(new Fields(
				joinOperator.getOutputSchema().toList().toArray(new String[joinOperator.getOutputSchema().size()])));
		projectOperator.setOutputSchema(new Fields(
				joinOperator.getOutputSchema().toList().toArray(new String[joinOperator.getOutputSchema().size()])));
		builder.setBolt("drain_bolt", 
				new SynefoBolt("drain_bolt", synefoIP, synefoPort, 
						projectOperator, zooIP, false), 1)
						.setNumTasks(1)
						.directGrouping("join_bolt_1")
						.directGrouping("join_bolt_2");
		topology.put("drain_bolt", new ArrayList<String>());
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
		conf.setNumWorkers(5);
		StormSubmitter.submitTopology("minimal-top", conf, builder.createTopology());
	}

}
