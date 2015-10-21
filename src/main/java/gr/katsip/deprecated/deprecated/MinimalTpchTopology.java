package gr.katsip.deprecated.deprecated;

import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.deprecated.ProjectOperator;
import gr.katsip.deprecated.JoinDispatcher;
import gr.katsip.deprecated.JoinJoiner;
import gr.katsip.tpch.LineItem;
import gr.katsip.tpch.Order;
import gr.katsip.deprecated.TpchTupleProducer;
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
public class MinimalTpchTopology {

	public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException, ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
		String synefoIP = "";
		Integer synefoPort = 5555;
		String[] streamIPs = null;
		String zooIP = "";
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> taskList;
		Integer taskNumber = 0;
		if(args.length < 3) {
			System.err.println("Arguments: <synefo-IP> <stream-IP1:port1,stream-IP2:port2,...,streamIP4:port4> <zoo-ip1:port1,zoo-ip2:port2,...,zoo-ipN:portN>");
			System.exit(1);
		}else {
			synefoIP = args[0];
			streamIPs = args[1].split(",");
			zooIP = args[2];
		}
		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		/**
		 * Stage 0: 2 input streams (order, lineitem)
		 */
		String[] dataSchema = { "attributes", "values" };
		TpchTupleProducer orderProducer = new TpchTupleProducer(streamIPs[0], Order.schema, Order.query5Schema);
		orderProducer.setSchema(new Fields(dataSchema));
		TpchTupleProducer lineitemProducer = new TpchTupleProducer(streamIPs[1], LineItem.schema, LineItem.query5Schema);
		lineitemProducer.setSchema(new Fields(dataSchema));
		builder.setSpout("order", 
				new SynefoSpout("order", synefoIP, synefoPort, orderProducer, zooIP), 1);
		taskNumber += 1;
		builder.setSpout("lineitem",
				new SynefoSpout("lineitem", synefoIP, synefoPort, lineitemProducer, zooIP), 1);
		taskNumber += 1;
		taskList = new ArrayList<String>();
		taskList.add("joindispatch");
		topology.put("order", new ArrayList<String>(taskList));
		topology.put("lineitem", new ArrayList<String>(taskList));
		
		/**
		 * Stage 1a: join dispatchers
		 */
		JoinDispatcher dispatcher = new JoinDispatcher("order", new Fields(Order.query5Schema), "lineitem", 
				new Fields(LineItem.query5Schema), new Fields(dataSchema));
		builder.setBolt("joindispatch", new SynefoJoinBolt("joindispatch", synefoIP, synefoPort, 
				dispatcher, zooIP, false), 1)
				.directGrouping("order")
				.directGrouping("lineitem");
		taskNumber += 1;
		taskList = new ArrayList<String>();
		taskList.add("joinjoinorder");
		taskList.add("joinjoinline");
		topology.put("joindispatch", taskList);
		
		/**
		 * Stage 1b : join joiners
		 */
		JoinJoiner joiner = new JoinJoiner("order", new Fields(Order.query5Schema), "lineitem", 
				new Fields(LineItem.query5Schema), "O_ORDERKEY", "L_ORDERKEY", 120000, 1000);
		joiner.setOutputSchema(new Fields(dataSchema));
		builder.setBolt("joinjoinorder", new SynefoJoinBolt("joinjoinorder", synefoIP, synefoPort, 
				joiner, zooIP, false), 1)
				.directGrouping("joindispatch");
		taskNumber += 1;
		taskList = new ArrayList<String>();
		taskList.add("drain");
		topology.put("joinjoinorder", taskList);
		joiner = new JoinJoiner("lineitem", new Fields(LineItem.query5Schema), "order", 
				new Fields(Order.query5Schema), "L_ORDERKEY", "O_ORDERKEY", 120000, 1000);
		joiner.setOutputSchema(new Fields(dataSchema));
		builder.setBolt("joinjoinline", new SynefoJoinBolt("joinjoinline", synefoIP, synefoPort, 
				joiner, zooIP, false), 1)
				.directGrouping("joindispatch");
		taskNumber += 1;
		taskList = new ArrayList<String>();
		taskList.add("drain");
		topology.put("joinjoinline", taskList);
		
		/**
		 * Stage 2: drain
		 */
		ProjectOperator projectOperator = new ProjectOperator(new Fields(dataSchema));
		projectOperator.setOutputSchema(new Fields(dataSchema));
		builder.setBolt("drain", 
				new SynefoBolt("drain", synefoIP, synefoPort, projectOperator, zooIP, true), 1)
				.directGrouping("joinjoinorder")
				.directGrouping("joinjoinline");
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

//		conf.setDebug(true);
		conf.setNumWorkers(4);
		conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, 
				"-Xmx4096m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled -Djava.net.preferIPv4Stack=true");
		conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

		//		LocalCluster cluster = new LocalCluster();
		//		cluster.submitTopology("tpch-q5-top", conf, builder.createTopology());
		//		Thread.sleep(100000);
		//		cluster.shutdown();
		StormSubmitter.submitTopology("min-tpch-top", conf, builder.createTopology());
	}

}
