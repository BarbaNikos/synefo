import java.util.ArrayList;
import java.util.HashMap;

import gr.katsip.cestorm.db.CEStormDatabaseManager;
import gr.katsip.synefo.storm.operators.crypstream.DataCollector;


public class StatCollectorClient {

	public static void main(String[] args) {
		
		String dbServerIp = null;
		String dbServerUser = null;
		String dbServerPass = null;
		HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp;
		
		CEStormDatabaseManager ceDb = new CEStormDatabaseManager(dbServerIp, 
				dbServerUser, dbServerPass);
		Integer queryId = ceDb.insertQuery(1, 
				"SELECT SUM(steps) FROM D1 WHERE lon=x AND lat=y");
		
		
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("spout_punctuation_tuples", new ArrayList<String>(_tmp));
		ceDb.insertOperator("spout_punctuation_tuples", "n/a", queryId, 0, 1, "SPOUT");
		
		
		_tmp = new ArrayList<String>();
		_tmp.add("select_bolt_1");
		_tmp.add("select_bolt_2");
		topology.put("spout_data_tuples", new ArrayList<String>(_tmp));	
		ceDb.insertOperator("spout_data_tuples", "n/a", queryId, 0, 2, "SPOUT");
		
		_tmp = new ArrayList<String>();
		_tmp.add("sum_operator_1");
		_tmp.add("sum_operator_2");
		topology.put("select_bolt_1", new ArrayList<String>(_tmp));
		ceDb.insertOperator("select_bolt_1", "n/a", queryId, 1, 1, "BOLT");
		
		_tmp = new ArrayList<String>();
		_tmp.add("sum_operator_1");
		_tmp.add("sum_operator_2");
		topology.put("select_bolt_2", new ArrayList<String>(_tmp));
		ceDb.insertOperator("select_bolt_2", "n/a", queryId, 1, 2, "BOLT");
		
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("sum_operator_1", new ArrayList<String>(_tmp));		
		ceDb.insertOperator("sum_operator_1", "n/a", queryId, 3, 1, "BOLT");
	
		_tmp = new ArrayList<String>();
		_tmp.add("client_bolt");
		topology.put("sum_operator_2", new ArrayList<String>(_tmp));
		ceDb.insertOperator("sum_operator_2", "n/a", queryId, 3, 2, "BOLT");
		
		
		topology.put("client_bolt", new ArrayList<String>());
		ceDb.insertOperator("client_bolt", "n/a", queryId, 4, 1, "BOLT");
		ceDb.insertOperatorAdjacencyList(queryId, topology);
		
		ceDb.destroy();
	}
//		DataCollector collector = new DataCollector("localhost", 2181, 1000, "select_bolt_1");
//		DataCollector collector2 = new DataCollector("localhost", 2181, 1000, "select_bolt_2");
//		DataCollector collector3 = new DataCollector("localhost", 2181, 1000, "select_bolt_3");
//		DataCollector collector4 = new DataCollector("localhost", 2181, 1000, "select_bolt_4");
//		
//		for(int i = 0; i < 100000; ++i) {
////			try {
////				Thread.sleep(5);
////			} catch (InterruptedException e) {
////				e.printStackTrace();
////			}
//			if(i % 4 == 0)
//				collector.addToBuffer("select_bolt_1,1," + i + "," + i + "," + i);
//			else if(i % 4 == 1)
//				collector2.addToBuffer("select_bolt_2,1," + i + "," + i + "," + i);
//			else if(i % 4 == 2)
//				collector3.addToBuffer("select_bolt_3,1," + i + "," + i + "," + i);
//			else
//				collector4.addToBuffer("select_bolt_4,1," + i + "," + i + "," + i);
//		}
//	}

}
