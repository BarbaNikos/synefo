package gr.katsip.synefo.storm.topology.crypefo;

import gr.katsip.synefo.storm.api.SynefoBolt;
import gr.katsip.synefo.storm.api.SynefoSpout;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.operators.relational.ProjectOperator;
import gr.katsip.synefo.storm.operators.synefo_comp_ops.Count;
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

public class DemoTopologyOne {


	
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
		/*try {
			Runtime.getRuntime().exec("/home/cpabe-0.11/cpabe-setup");
		} catch (IOException e) {
			System.out.println("Error in Central Authority. CPABE command error, could not execute commands.");
			e.printStackTrace();
		}
		String masterKey="";
		String publicKey="";
		try {
			FileInputStream fstream = new FileInputStream("pub_key");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null)   {
				publicKey=strLine;
			}
		} catch (FileNotFoundException e) {
			System.out.println("Error in Central Authority. File: priv_key_1 not found.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error in Central Authority. Reading File: priv_key_1 error, good luck.");
			e.printStackTrace();
		}
		try {
			FileInputStream fstream = new FileInputStream("master_key");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null)   {
				masterKey=strLine;
			}
		} catch (FileNotFoundException e) {
			System.out.println("Error in Central Authority. File: priv_key_1 not found.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error in Central Authority. Reading File: priv_key_1 error, good luck.");
			e.printStackTrace();
		}*/
		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		/**
		 * Stage 0: Data Sources Spouts
		 */
		//Need to change the following to your punctuation schema
		String[] punctuationSpoutSchema = { "punct" };
		
		CrypefoPunctuationTupleProducer punctuationTupleProducer = new CrypefoPunctuationTupleProducer(streamIPs[0]);
		punctuationTupleProducer.setSchema(new Fields(punctuationSpoutSchema));
		builder.setSpout("spout_punctuation_tuples", 
				new SynefoSpout("spout_punctuation_tuples", synefoIP, synefoPort, punctuationTupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);
		
		//Need to change the following to your data schema
		String[] dataSpoutSchema = { "tuple" };
		CrypefoDataTupleProducer dataTupleProducer = new CrypefoDataTupleProducer(streamIPs[0]);
		dataTupleProducer.setSchema(new Fields(dataSpoutSchema));
		builder.setSpout("spout_data_tuples", 
				new SynefoSpout("spout_data_tuples", synefoIP, synefoPort, dataTupleProducer, zooIP, zooPort), 1)
				.setNumTasks(1);
		
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("spout_punctuation_tuples", new ArrayList<String>(_tmp));
		_tmp = new ArrayList<String>();
		_tmp.add("count_bolt");
		topology.put("spout_data_tuples", new ArrayList<String>(_tmp));
		
		/**
		 * Stage 1: Count operator
		 */
		String[] countOutputSchema = dataSpoutSchema; // These two have to be the same since it is just a selection
		//ArrayList<Integer> retSet = new ArrayList<Integer>(); //for select
		//int buff, int attr, String pred, int typ, String client, int statBuffer
		int buffer = 100;
		String pred = "50";
		int attribute = 2;
		Count count = new Count(buffer, attribute, pred, 1, "0",1000, zooIP, zooPort);
		//FilterOperator<String> filterOperator = new FilterOperator<String>(new StringComparator(), 
		//		"communist_level", "50");
		count.setOutputSchema(new Fields(countOutputSchema));
		//filterOperator.setOutputSchema(new Fields(selectionOutputSchema));
		builder.setBolt("count_bolt", 
				new SynefoBolt("count_bolt", synefoIP, synefoPort, count, 
						zooIP, zooPort, false), 1)
						.setNumTasks(1)
						.directGrouping("spout_data_tuples");
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("count_bolt", new ArrayList<String>(_tmp));
		
		/**
		 * Stage 2: Client Bolt (project operator)
		 */
		ProjectOperator projectOperator = new ProjectOperator(new Fields(
				count.getOutputSchema().toList().toArray(new String[count.getOutputSchema().size()])));
		projectOperator.setOutputSchema(new Fields(
				count.getOutputSchema().toList().toArray(new String[count.getOutputSchema().size()])));
		builder.setBolt("client_bolt", 
				new SynefoBolt("client_bolt", synefoIP, synefoPort, 
						projectOperator, zooIP, zooPort, false), 1)
						.setNumTasks(1)
						.directGrouping("count_bolt")
						.directGrouping("spout_punctuation_tuples");
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
		conf.setNumWorkers(4);
		StormSubmitter.submitTopology("crypefo-top-1", conf, builder.createTopology());
	}
	

}
