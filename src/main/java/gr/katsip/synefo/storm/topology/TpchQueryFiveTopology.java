package gr.katsip.synefo.storm.topology;

import gr.katsip.synefo.storm.api.SynefoSpout;
import gr.katsip.synefo.storm.lib.SynefoMessage;
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

	public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException, ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
		String synefoIP = "";
		Integer synefoPort = 5555;
		String[] streamIPs = null;
		String zooIP = "";
		Integer zooPort = -1;
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> taskList;
		Integer taskNumber = 0;
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
		 * Stage 0: 6 input streams (customer, lineitem, nation, orders, region, supplier)
		 */
		String[] dataSchema = { "attributes", "values" };
		TpchTupleProducer customerProducer = new TpchTupleProducer(streamIPs[0], Customer.schema);
		customerProducer.setSchema(new Fields(dataSchema));
		TpchTupleProducer orderProducer = new TpchTupleProducer(streamIPs[1], Order.schema);
		orderProducer.setSchema(new Fields(dataSchema));
		builder.setSpout("customer", 
				new SynefoSpout("customer", synefoIP, synefoPort, customerProducer, zooIP, zooPort), 1)
				.setNumTasks(1);
		taskNumber += 1;
		builder.setSpout("order", 
				new SynefoSpout("order", synefoIP, synefoPort, customerProducer, zooIP, zooPort), 1)
				.setNumTasks(1);
		taskNumber += 1;
		topology.put("customer", new ArrayList<String>());
		topology.put("order", new ArrayList<String>());
		
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

		StormSubmitter.submitTopology("tpch-q5-top", conf, builder.createTopology());
	}
}
