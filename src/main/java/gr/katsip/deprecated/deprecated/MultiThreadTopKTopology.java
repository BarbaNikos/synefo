package gr.katsip.deprecated.deprecated;

import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.deprecated.CountGroupByAggrOperator;
import gr.katsip.deprecated.JoinOperator;
import gr.katsip.deprecated.ProjectOperator;
import gr.katsip.deprecated.StringComparator;

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

/**
 * @deprecated
 */
public class MultiThreadTopKTopology {

	public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException, 
	ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
		String synefoIP = "";
		Integer synefoPort = 5555;
		String[] streamIPs = null;
		String zooIP = "";
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> taskList;
		Integer taskNumber = 0;
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
		 * Stage 0: Two input streams (spout_1, spout_2)
		 */
		StreamgenTupleProducer tupleProducer = new StreamgenTupleProducer(streamIPs[0]);
		String[] spoutSchemaOne = { "one", "two", "three", "four", "five" };
		tupleProducer.setSchema(new Fields(spoutSchemaOne));
		builder.setSpout("spout-1", 
				new SynefoSpout("spout-1", synefoIP, synefoPort, tupleProducer, zooIP), 1);
		taskNumber += 1;
		taskList = new ArrayList<String>();
		taskList.add("project");
		topology.put("spout-1", new ArrayList<String>(taskList));

		tupleProducer = new StreamgenTupleProducer(streamIPs[1]);
		String[] spoutSchemaTwo = { "1", "2", "three", "4", "5" };
		tupleProducer.setSchema(new Fields(spoutSchemaTwo));
		builder.setSpout("spout-2", 
				new SynefoSpout("spout-2", synefoIP, synefoPort, tupleProducer, zooIP), 1);
		taskNumber += 1;
		taskList = new ArrayList<String>();
		taskList.add("join");
		topology.put("spout-2", new ArrayList<String>(taskList));
		/**
		 * Stage 1: Project operator after spout_1
		 */
		ProjectOperator projectOperator = new ProjectOperator(new Fields(spoutSchemaOne));
		projectOperator.setOutputSchema(new Fields(spoutSchemaOne));
		builder.setBolt("project", 
				new SynefoBolt("project", synefoIP, synefoPort, projectOperator, zooIP, true), 3)
				.directGrouping("spout-1");
		taskNumber += 3;
		taskList = new ArrayList<String>();
		taskList.add("join");
		topology.put("project", new ArrayList<String>(taskList));
		taskList = null;
		/**
		 * Stage 2: Join operators after project and spout_2
		 */
		JoinOperator<String> joinOperator = new JoinOperator<String>(new StringComparator(), 100000, "three", 
				new Fields(spoutSchemaOne), new Fields(spoutSchemaTwo));
		builder.setBolt("join", 
				new SynefoBolt("join", synefoIP, synefoPort, 
						joinOperator, zooIP, true), 16)
						.directGrouping("project")
						.directGrouping("spout-2");
		taskNumber += 16;
		taskList = new ArrayList<String>();
		taskList.add("count-groupby");
		topology.put("join", new ArrayList<String>(taskList));
		taskList = null;
		/**
		 * Stage 3: Count-Group-By operators after join
		 */
		String[] groupByAttributes = new String[joinOperator.getOutputSchema().toList().size()];
		groupByAttributes = joinOperator.getOutputSchema().toList().toArray(groupByAttributes);
		CountGroupByAggrOperator countGroupByAggrOperator = new CountGroupByAggrOperator(500, groupByAttributes);
		String[] countGroupBySchema = { "key", "count" };
		String[] countGroupByStateSchema = { "key", "count", "time" };
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count-groupby", 
				new SynefoBolt("count-groupby", synefoIP, synefoPort, 
						countGroupByAggrOperator, zooIP, true), 4)
						.directGrouping("join");
		taskNumber += 4;
		taskList = new ArrayList<String>();
		taskList.add("drain");
		topology.put("count-groupby", taskList);
		/**
		 * Stage 4: Drain Operator (project operator)
		 */
		projectOperator = new ProjectOperator(new Fields(countGroupBySchema));
		projectOperator.setOutputSchema(new Fields(countGroupBySchema));
		builder.setBolt("drain", 
				new SynefoBolt("drain", synefoIP, synefoPort, projectOperator, zooIP, true), 1)
				.directGrouping("count-groupby");
		taskNumber += 1;
		topology.put("drain", new ArrayList<String>());
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
		msg._values.put("TASK_NUM", Integer.toString(taskNumber));
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
		conf.setNumWorkers(16);
		conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

		StormSubmitter.submitTopology("multicore-top-k-top", conf, builder.createTopology());
	}

}
