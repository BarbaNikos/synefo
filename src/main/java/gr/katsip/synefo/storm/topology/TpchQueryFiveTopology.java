package gr.katsip.synefo.storm.topology;

import gr.katsip.synefo.storm.api.SynefoBolt;
import gr.katsip.synefo.storm.api.SynefoJoinBolt;
import gr.katsip.synefo.storm.api.SynefoSpout;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.operators.relational.ProjectOperator;
import gr.katsip.synefo.storm.operators.relational.elastic.JoinDispatcher;
import gr.katsip.synefo.storm.operators.relational.elastic.JoinJoiner;
import gr.katsip.synefo.tpch.Customer;
import gr.katsip.synefo.tpch.Order;
import gr.katsip.synefo.tpch.TpchTupleProducer;
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

public class TpchQueryFiveTopology {

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
		 * Stage 0: 6 input streams (customer, lineitem, nation, orders, region, supplier)
		 */
		String[] dataSchema = { "attributes", "values" };
		TpchTupleProducer customerProducer = new TpchTupleProducer(streamIPs[0], Customer.schema, Customer.query5schema);
		customerProducer.setSchema(new Fields(dataSchema));
		TpchTupleProducer orderProducer = new TpchTupleProducer(streamIPs[1], Order.schema, Order.query5Schema);
		orderProducer.setSchema(new Fields(dataSchema));
		builder.setSpout("customer", 
				new SynefoSpout("customer", synefoIP, synefoPort, customerProducer, zooIP), 1);
		taskNumber += 1;
		builder.setSpout("order", 
				new SynefoSpout("order", synefoIP, synefoPort, orderProducer, zooIP), 1);
		taskNumber += 1;
		taskList = new ArrayList<String>();
		taskList.add("joindispatch");
		topology.put("customer", taskList);
		topology.put("order", new ArrayList<String>(taskList));
		
		/**
		 * Stage 1a: join dispatchers
		 */
		JoinDispatcher dispatcher = new JoinDispatcher("customer", new Fields(Customer.query5schema), "order", 
				new Fields(Order.query5Schema), new Fields(dataSchema));
		builder.setBolt("joindispatch", new SynefoJoinBolt("joindispatch", synefoIP, synefoPort, 
			dispatcher, zooIP, true), 3)
			.directGrouping("customer")
			.directGrouping("order");
		taskNumber += 3;
		taskList = new ArrayList<String>();
		taskList.add("joinjoincust");
		taskList.add("joinjoinorder");
		topology.put("joindispatch", taskList);
		/**
		 * Stage 1b : join joiners
		 */
		JoinJoiner joiner = new JoinJoiner("customer", new Fields(Customer.query5schema), "order", 
				new Fields(Order.query5Schema), "C_CUSTKEY", "O_CUSTKEY", 120000, 1000);
		joiner.setOutputSchema(new Fields(dataSchema));
		builder.setBolt("joinjoincust", new SynefoJoinBolt("joinjoincust", synefoIP, synefoPort, 
				joiner, zooIP, false), 3)
		.directGrouping("joindispatch");
		taskNumber += 3;
		taskList = new ArrayList<String>();
		taskList.add("drain");
		topology.put("joinjoincust", taskList);
		joiner = new JoinJoiner("order", new Fields(Order.query5Schema), "customer", 
				new Fields(Customer.query5schema), "O_CUSTKEY", "C_CUSTKEY", 120000, 1000);
		joiner.setOutputSchema(new Fields(dataSchema));
		builder.setBolt("joinjoinorder", new SynefoJoinBolt("joinjoinorder", synefoIP, synefoPort, 
				joiner, zooIP, false), 3)
		.directGrouping("joindispatch");
		taskNumber += 3;
		taskList = new ArrayList<String>();
		taskList.add("drain");
		topology.put("joinjoinorder", taskList);
		/**
		 * Stage 2: drain
		 */
		ProjectOperator projectOperator = new ProjectOperator(new Fields(dataSchema));
		projectOperator.setOutputSchema(new Fields(dataSchema));
		builder.setBolt("drain", 
				new SynefoBolt("drain", synefoIP, synefoPort, projectOperator, zooIP, true), 1)
				.directGrouping("joinjoincust").directGrouping("joinjoinorder");
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
		conf.setNumWorkers(8);
		conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("tpch-q5-top", conf, builder.createTopology());
//		Thread.sleep(100000);
//		cluster.shutdown();
		StormSubmitter.submitTopology("tpch-q5-top", conf, builder.createTopology());
	}
}
