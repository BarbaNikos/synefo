package gr.katsip.synefo.storm.topology;

import gr.katsip.synefo.storm.api.SynEFOBolt;
import gr.katsip.synefo.storm.api.SynEFOSpout;
import gr.katsip.synefo.storm.lib.SynEFOMessage;
import gr.katsip.synefo.storm.operators.relational.CountGroupByAggrOperator;
import gr.katsip.synefo.storm.operators.relational.EquiJoinOperator;
import gr.katsip.synefo.storm.operators.relational.FilterOperator;
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
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class ExperimentalTopology {

	public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException, 
	ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
		String synefoIP = "";
		Integer synefoPort = -1;
		String streamIP = "";
		Integer streamPort = -1;
		String zooIP = "";
		Integer zooPort = -1;
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp;
		Integer numOfWorkers = -1;
		if(args.length < 6) {
			System.err.println("Arguments: <synefo-IP> <synefo-port> <stream-IP> <stream-port> <zoo-IP> <zoo-port> <opt:num-of-workers>");
			System.exit(1);
		}else {
			synefoIP = args[0];
			synefoPort = Integer.parseInt(args[1]);
			streamIP = args[2];
			streamPort = Integer.parseInt(args[3]);
			zooIP = args[4];
			zooPort = Integer.parseInt(args[5]);
			if(args.length > 6) {
				numOfWorkers = Integer.parseInt(args[6]);
			}
		}
		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		StreamgenTupleProducer tupleProducer = new StreamgenTupleProducer(streamIP, streamPort);
		String[] spoutSchema = { "one", "two", "three", "four" };
		tupleProducer.setSchema(new Fields(spoutSchema));
		builder.setSpout("spout_1", new SynEFOSpout("spout_1", synefoIP, synefoPort, tupleProducer, zooIP, zooPort), 1);
		_tmp = new ArrayList<String>();
		_tmp.add("select_bolt_1");
		topology.put("spout_1", new ArrayList<String>(_tmp));
		
		FilterOperator<String> filterOperator = new FilterOperator<String>(new StringComparator(), "one", "HPibkcVIld");
		String[] filterOutSchema = { "one", "two", "three", "four" };
		filterOperator.setOutputSchema(new Fields(filterOutSchema));
		builder.setBolt("select_bolt_1", 
				new SynEFOBolt("select_bolt_1", synefoIP, synefoPort, filterOperator, zooIP, zooPort), 1)
				.directGrouping("spout_1");
		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		topology.put("select_bolt_1", new ArrayList<String>(_tmp));
		_tmp = null;
		/**
		 * Stage 2: Join operators
		 */
		EquiJoinOperator<String> equi_join_op = new EquiJoinOperator<String>(new StringComparator(), 1000, "two");
		String[] join_schema = { "two-a", "two-b" };
		String[] state_schema = { "two", "time" };
		equi_join_op.setOutputSchema(new Fields(join_schema));
		equi_join_op.setStateSchema(new Fields(state_schema));
		builder.setBolt("join_bolt_1", 
				new SynEFOBolt("join_bolt_1", synefoIP, synefoPort, equi_join_op, zooIP, zooPort), 1)
				.directGrouping("select_bolt_1");
		_tmp = new ArrayList<String>();
		_tmp.add("count_group_by_bolt_1");
		topology.put("join_bolt_1", new ArrayList<String>(_tmp));
		_tmp = null;
		/**
		 * Stage 3: Aggregate operator
		 */
		CountGroupByAggrOperator countGroupByAggrOperator = new CountGroupByAggrOperator(1000, join_schema);
		String[] countGroupBySchema = { "key", "count" };
		String[] countGroupByStateSchema = { "key", "count", "time" };
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count_group_by_bolt_1", 
				new SynEFOBolt("count_group_by_bolt_1", synefoIP, synefoPort, countGroupByAggrOperator, zooIP, zooPort), 1)
				.directGrouping("join_bolt_1");
		topology.put("count_group_by_bolt_1", new ArrayList<String>());
		/**
		 * Notify SynEFO server about the 
		 * Topology
		 */
		System.out.println("About to connect to synEFO: " + synefoIP + ":" + synefoPort);
		Socket synEFOSocket = new Socket(synefoIP, synefoPort);
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

		conf.setDebug(true);
		if(numOfWorkers != -1) {
			conf.setNumWorkers(4);
			StormSubmitter.submitTopology("experimental-top", conf, builder.createTopology());
		} else {        
			conf.setMaxTaskParallelism(5);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("experimental-top", conf, builder.createTopology());
			Thread.sleep(100000);
			cluster.shutdown();
		}
	}

}
