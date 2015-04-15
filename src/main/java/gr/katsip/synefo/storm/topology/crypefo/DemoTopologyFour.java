package gr.katsip.synefo.storm.topology.crypefo;

import gr.katsip.synefo.storm.api.SynefoBolt;
import gr.katsip.synefo.storm.api.SynefoSpout;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.operators.relational.FilterOperator;
import gr.katsip.synefo.storm.operators.relational.JoinOperator;
import gr.katsip.synefo.storm.operators.relational.ProjectOperator;
import gr.katsip.synefo.storm.operators.relational.StringComparator;
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

public class DemoTopologyFour {

	public static void main(String[] args) throws UnknownHostException, IOException, 
	InterruptedException, ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
		String synefoIP = "";
		Integer synefoPort = 5555;
		String[] streamIPs = null;
		String zooIP = "";
		Integer zooPort = -1;
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp;
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
		 * Stage 0: Data Sources Spouts
		 */
		//Need to change the following to your punctuation schema
		String[] punctuationSpoutSchema = { "num", "one", "two", "three", "four" };

		CrypefoPunctuationTupleProducer punctuationTupleProducer = new CrypefoPunctuationTupleProducer(streamIPs[0]);
		punctuationTupleProducer.setSchema(new Fields(punctuationSpoutSchema));
		builder.setSpout("spout_punctuation_tuples_1", 
				new SynefoSpout("spout_punctuation_tuples_1", synefoIP, synefoPort, punctuationTupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);

		//Need to change the following to your data schema
		String[] dataSpoutSchema = { "num", "1", "2", "three", "5" };
		CrypefoDataTupleProducer dataTupleProducer = new CrypefoDataTupleProducer(streamIPs[0]);
		punctuationTupleProducer.setSchema(new Fields(dataSpoutSchema));
		builder.setSpout("spout_data_tuples_1", 
				new SynefoSpout("spout_data_tuples_1", synefoIP, synefoPort, dataTupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);

		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("spout_punctuation_tuples_1", new ArrayList<String>(_tmp));
		_tmp = new ArrayList<String>();
		_tmp.add("select_bolt_1");
		topology.put("spout_data_tuples_1", new ArrayList<String>(_tmp));

		//Need to change the following to your punctuation schema
		String[] punctuationSpoutTwoSchema = { "num", "one", "two", "three", "four" };

		punctuationTupleProducer = new CrypefoPunctuationTupleProducer(streamIPs[0]);
		punctuationTupleProducer.setSchema(new Fields(punctuationSpoutTwoSchema));
		builder.setSpout("spout_punctuation_tuples_2", 
				new SynefoSpout("spout_punctuation_tuples_2", synefoIP, synefoPort, punctuationTupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);

		//Need to change the following to your data schema
		String[] dataSpoutTwoSchema = { "num", "1", "2", "three", "5" };
		dataTupleProducer = new CrypefoDataTupleProducer(streamIPs[0]);
		punctuationTupleProducer.setSchema(new Fields(dataSpoutTwoSchema));
		builder.setSpout("spout_data_tuples_2", 
				new SynefoSpout("spout_data_tuples_2", synefoIP, synefoPort, dataTupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);

		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("spout_punctuation_tuples_2", new ArrayList<String>(_tmp));
		_tmp = new ArrayList<String>();
		_tmp.add("select_bolt_2");
		topology.put("spout_data_tuples_2", new ArrayList<String>(_tmp));

		/**
		 * Stage 1: Select operator
		 */
		String[] selectionOutputSchema = dataSpoutSchema; // These two have to be the same since it is just a selection
		FilterOperator<String> filterOperator = new FilterOperator<String>(new StringComparator(), 
				"selection-field-name", "selection-field-value");
		filterOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("select_bolt_1", 
				new SynefoBolt("select_bolt_1", synefoIP, synefoPort, filterOperator, 
						zooIP, zooPort, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_tuples_1");
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("select_bolt_1", new ArrayList<String>(_tmp));
		
		String[] selectionTwoOutputSchema = dataSpoutTwoSchema; // These two have to be the same since it is just a selection
		filterOperator = new FilterOperator<String>(new StringComparator(), 
				"selection-field-name", "selection-field-value");
		filterOperator.setOutputSchema(new Fields(selectionTwoOutputSchema));
		builder.setBolt("select_bolt_2", 
				new SynefoBolt("select_bolt_2", synefoIP, synefoPort, filterOperator, 
						zooIP, zooPort, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_tuples_2");
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("select_bolt_2", new ArrayList<String>(_tmp));
		
		/**
		 * Stage 2: Join Bolt
		 */
		JoinOperator<String> joinOperator = new JoinOperator<String>(new StringComparator(), 1000, "join-field-name", 
				new Fields(selectionOutputSchema), new Fields(selectionTwoOutputSchema));
		builder.setBolt("join_bolt", 
				new SynefoBolt("join_bolt", synefoIP, synefoPort, 
						joinOperator, zooIP, zooPort, true), 1)
						.setNumTasks(1)
						.directGrouping("select_bolt_1")
						.directGrouping("select_bolt_2");
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("join_bolt", new ArrayList<String>(_tmp));

		/**
		 * Stage 3: Client Bolt (project operator)
		 */
		ProjectOperator projectOperator = new ProjectOperator(new Fields(
				joinOperator.getOutputSchema().toList().toArray(new String[joinOperator.getOutputSchema().size()])));
		projectOperator.setOutputSchema(new Fields(
				joinOperator.getOutputSchema().toList().toArray(new String[joinOperator.getOutputSchema().size()])));
		builder.setBolt("client_bolt", 
				new SynefoBolt("client_bolt", synefoIP, synefoPort, 
						projectOperator, zooIP, zooPort, false), 1)
						.setNumTasks(1)
						.directGrouping("join_bolt")
						.directGrouping("spout_punctuation_tuples_1")
						.directGrouping("spout_punctuation_tuples_2");
		topology.put("client_bolt", new ArrayList<String>());
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
		conf.setNumWorkers(8);
		StormSubmitter.submitTopology("crypefo-top-4", conf, builder.createTopology());
	}
}
