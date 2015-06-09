package gr.katsip.synefo.storm.topology;

import gr.katsip.synefo.storm.api.SynefoBolt;
import gr.katsip.synefo.storm.api.SynefoSpout;
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

/**
 * The following topology simulates a SQL query as the following
 * SELECT A.three, B.three, COUNT(*) 
 * FROM STREAM A, STREAM B
 * WHERE A.three = B.three
 * GROUP BY A.three, B.three
 * 
 * @author katsip
 *
 */
public class TopKTopology {

	public static void main(String[] args) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException, AlreadyAliveException, InvalidTopologyException {
		String synefoIP = "";
		Integer synefoPort = 5555;
		String[] streamIPs = null;
		String zooIP = "";
		Integer zooPort = -1;
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> taskList;
		if(args.length < 4) {
			System.err.println("Arguments: <synefo-IP> <stream-IP> <zoo-IP> <zoo-port>");
			System.exit(1);
		}else {
			synefoIP = args[0];
			streamIPs = args[1].split(",");
			zooIP = args[2];
			zooPort = Integer.parseInt(args[3]);
		}
		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		/**
		 * Stage 0: Two input streams (spout_1, spout_2)
		 */
		StreamgenTupleProducer tupleProducer = new StreamgenTupleProducer(streamIPs[0]);
		String[] spoutSchemaOne = { "one", "two", "three", "four", "five" };
		tupleProducer.setSchema(new Fields(spoutSchemaOne));
		builder.setSpout("spout_1", 
				new SynefoSpout("spout_1", synefoIP, synefoPort, tupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);
		taskList = new ArrayList<String>();
		taskList.add("project_bolt_1");
		taskList.add("project_bolt_2");
		taskList.add("project_bolt_3");
		topology.put("spout_1", new ArrayList<String>(taskList));
		
		tupleProducer = new StreamgenTupleProducer(streamIPs[1]);
		String[] spoutSchemaTwo = { "1", "2", "three", "4", "5" };
		tupleProducer.setSchema(new Fields(spoutSchemaTwo));
		builder.setSpout("spout_2", 
				new SynefoSpout("spout_2", synefoIP, synefoPort, tupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);
		taskList = new ArrayList<String>();
		taskList.add("join_bolt_1");
		taskList.add("join_bolt_2");
		taskList.add("join_bolt_3");
		topology.put("spout_2", new ArrayList<String>(taskList));
		
		/**
		 * Stage 1: Project operators spout_1 => {project_1, 2, 3} / spout_3 => {project_4, 5, 6}
		 */
		ProjectOperator projectOperator = new ProjectOperator(new Fields(spoutSchemaOne));
		projectOperator.setOutputSchema(new Fields(spoutSchemaOne));
		builder.setBolt("project_bolt_1", 
				new SynefoBolt("project_bolt_1", synefoIP, synefoPort, projectOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("spout_1");
		projectOperator = new ProjectOperator(new Fields(spoutSchemaOne));
		projectOperator.setOutputSchema(new Fields(spoutSchemaOne));
		builder.setBolt("project_bolt_2", 
				new SynefoBolt("project_bolt_2", synefoIP, synefoPort, projectOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("spout_1");
		projectOperator = new ProjectOperator(new Fields(spoutSchemaOne));
		projectOperator.setOutputSchema(new Fields(spoutSchemaOne));
		builder.setBolt("project_bolt_3", 
				new SynefoBolt("project_bolt_3", synefoIP, synefoPort, projectOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("spout_1");
		taskList = new ArrayList<String>();
		taskList.add("join_bolt_1");
		taskList.add("join_bolt_2");
		taskList.add("join_bolt_3");
		topology.put("project_bolt_1", new ArrayList<String>(taskList));
		topology.put("project_bolt_2", new ArrayList<String>(taskList));
		topology.put("project_bolt_3", new ArrayList<String>(taskList));
		taskList = null;
		
		/**
		 * Stage 2: Join operators between project_bolt_1:3 (spout_1) and spout_2
		 */
		JoinOperator<String> joinOperator = new JoinOperator<String>(new StringComparator(), 500, "three", 
				new Fields(spoutSchemaOne), new Fields(spoutSchemaTwo));
		builder.setBolt("join_bolt_1", 
				new SynefoBolt("join_bolt_1", synefoIP, synefoPort, 
						joinOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("project_bolt_1")
				.directGrouping("project_bolt_2")
				.directGrouping("project_bolt_3")
				.directGrouping("spout_2");
		joinOperator = new JoinOperator<String>(new StringComparator(), 500, "three", 
				new Fields(spoutSchemaOne), new Fields(spoutSchemaTwo));
		builder.setBolt("join_bolt_2", 
				new SynefoBolt("join_bolt_2", synefoIP, synefoPort, 
						joinOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("project_bolt_1")
				.directGrouping("project_bolt_2")
				.directGrouping("project_bolt_3")
				.directGrouping("spout_2");
		joinOperator = new JoinOperator<String>(new StringComparator(), 500, "three", 
				new Fields(spoutSchemaOne), new Fields(spoutSchemaTwo));
		builder.setBolt("join_bolt_3", 
				new SynefoBolt("join_bolt_3", synefoIP, synefoPort, 
						joinOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("project_bolt_1")
				.directGrouping("project_bolt_2")
				.directGrouping("project_bolt_3")
				.directGrouping("spout_2");
		taskList = new ArrayList<String>();
		taskList.add("count_group_by_bolt_1");
		taskList.add("count_group_by_bolt_2");
		taskList.add("count_group_by_bolt_3");
		topology.put("join_bolt_1", new ArrayList<String>(taskList));
		topology.put("join_bolt_2", new ArrayList<String>(taskList));
		topology.put("join_bolt_3", new ArrayList<String>(taskList));
		taskList = null;
		
		/**
		 * Stage 3: Count-Group-By operators between join_bolt_1:3
		 */
		String[] groupByAttributes = new String[joinOperator.getOutputSchema().toList().size()];
		groupByAttributes = joinOperator.getOutputSchema().toList().toArray(groupByAttributes);
		
		CountGroupByAggrOperator countGroupByAggrOperator = new CountGroupByAggrOperator(500, groupByAttributes);
		String[] countGroupBySchema = { "key", "count" };
		String[] countGroupByStateSchema = { "key", "count", "time" };
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count_group_by_bolt_1", 
				new SynefoBolt("count_group_by_bolt_1", synefoIP, synefoPort, 
						countGroupByAggrOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("join_bolt_1")
				.directGrouping("join_bolt_2")
				.directGrouping("join_bolt_3");
		countGroupByAggrOperator = new CountGroupByAggrOperator(500, groupByAttributes);
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count_group_by_bolt_2", 
				new SynefoBolt("count_group_by_bolt_2", synefoIP, synefoPort, 
						countGroupByAggrOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("join_bolt_1")
				.directGrouping("join_bolt_2")
				.directGrouping("join_bolt_3");
		countGroupByAggrOperator = new CountGroupByAggrOperator(500, groupByAttributes);
		countGroupByAggrOperator.setOutputSchema(new Fields(countGroupBySchema));
		countGroupByAggrOperator.setStateSchema(new Fields(countGroupByStateSchema));
		builder.setBolt("count_group_by_bolt_3", 
				new SynefoBolt("count_group_by_bolt_3", synefoIP, synefoPort, 
						countGroupByAggrOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("join_bolt_1")
				.directGrouping("join_bolt_2")
				.directGrouping("join_bolt_3");
		taskList = new ArrayList<String>();
		taskList.add("drain_bolt");
		topology.put("count_group_by_bolt_1", taskList);
		topology.put("count_group_by_bolt_2", taskList);
		topology.put("count_group_by_bolt_3", taskList);
		
		/**
		 * Stage 4: Drain Operator (project operator)
		 */
		projectOperator = new ProjectOperator(new Fields(countGroupBySchema));
		projectOperator.setOutputSchema(new Fields(countGroupBySchema));
		builder.setBolt("drain_bolt", 
				new SynefoBolt("drain_bolt", synefoIP, synefoPort, projectOperator, zooIP, zooPort, true), 1)
				.setNumTasks(1)
				.directGrouping("count_group_by_bolt_1")
				.directGrouping("count_group_by_bolt_2")
				.directGrouping("count_group_by_bolt_3");
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
		conf.setNumWorkers(12);
		conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		
		StormSubmitter.submitTopology("top-k-top", conf, builder.createTopology());
	}

}
