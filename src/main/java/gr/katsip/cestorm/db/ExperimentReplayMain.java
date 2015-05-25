package gr.katsip.cestorm.db;

import gr.katsip.cestorm.db.ExperimentReplayer.CrypefoEvent;
import gr.katsip.cestorm.db.ExperimentReplayer.Operator;
import gr.katsip.cestorm.db.ExperimentReplayer.OperatorAdjacencyListEntry;
import gr.katsip.cestorm.db.ExperimentReplayer.Query;
import gr.katsip.cestorm.db.ExperimentReplayer.ScaleEvent;
import gr.katsip.cestorm.db.ExperimentReplayer.Statistic;
import gr.katsip.cestorm.db.ExperimentReplayer.TopologyOperatorEntry;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;

public class ExperimentReplayMain {

	public static void main(String[] args) throws IOException {
		String dbServerIp = null;
		String dbServerUser = null;
		String dbServerPass = null;
		Integer queryId = -1;
		if(args.length < 2) {
			System.err.println("Arguments: <query-id> <db-info-file>");
			System.exit(1);
		}else {
			queryId = Integer.parseInt(args[0]);
			System.out.println("Database Configuration file provided. Parsing connection information...");
			try(BufferedReader br = new BufferedReader(new FileReader(new File(args[1])))) {
				for(String line; (line = br.readLine()) != null;) {
					String[] lineTokens = line.split(":");
					if(line.contains("db-server-ip:"))
						dbServerIp = "jdbc:mysql://" + lineTokens[1] + "/";
					else if(line.contains("db-schema-name:")) 
						dbServerIp = dbServerIp + lineTokens[1];
					else if(line.contains("db-user:"))
						dbServerUser = lineTokens[1];
					else if(line.contains("db-password:"))
						dbServerPass = lineTokens[1];
					else {
						System.err.println("Invalid db-info file provided. Please use proper formatted file. Format: ");
						System.err.println("db-server-ip:\"proper-ip-here\"");
						System.err.println("db-schema-name:\"proper-schema-name-here\"");
						System.err.println("db-user:\"proper-username-here\"");
						System.err.println("db-password:\"proper-user-password-here\"");
						System.exit(1);
					}
				}
			}
		}
		ExperimentReplayer replayer = new ExperimentReplayer(dbServerIp, dbServerUser, dbServerPass, queryId);
		replayer.retrieveExperimentData();
		
		Query query = replayer.getQuery();
		ArrayList<Operator> operators = replayer.getOperators();
		ArrayList<OperatorAdjacencyListEntry> opAdjacencyList = replayer.getOperatorAdjacencyList();
		ArrayList<TopologyOperatorEntry> topologyOperatorInitial = replayer.getInitialTopologyOperatorList();
		
		System.out.println("Query (ID: " + query.id + ") of client-" + query.clientId + " is: \"" + query.query + "\"");
		System.out.println("Query (ID: " + query.id + ") has operators: ");
		for(Operator op : operators) {
			System.out.println("id:" + op.id + ", name: " + op.name + ", IP: " + op.ipAddress + 
					", query-id: " + op.queryId + ", (x,y): (" + op.x + "," + op.y + "), type: " + op.type);
		}
		System.out.println("Query (ID: " + query.id + ") has operator adjacency list: ");
		for(OperatorAdjacencyListEntry opAdjEntry : opAdjacencyList) {
			System.out.println("query-id: " + opAdjEntry.queryId + ", parent: " + opAdjEntry.parentId + ", child: " + opAdjEntry.childId);
		}
		System.out.println("Query (ID: " + query.id + ") has initial active topology: ");
		for(TopologyOperatorEntry topologyOperatorEntry : topologyOperatorInitial) {
			System.out.println("operator_id: " + topologyOperatorEntry.operatorId + ", status: " + topologyOperatorEntry.status + ", start_time: " + topologyOperatorEntry.startTime);
		}
		TreeMap<Long, ArrayList<CrypefoEvent>> timeEvents = replayer.getTimeEvents();
		Iterator<Long> itr = timeEvents.keySet().iterator();
		while(itr.hasNext()) {
			Long timestamp = itr.next();
			System.out.println("Timestamp: " + timestamp);
			ArrayList<CrypefoEvent> events = timeEvents.get(timestamp);
			for(CrypefoEvent event : events) {
				if(event instanceof ExperimentReplayer.ScaleEvent) {
					ScaleEvent scaleEvent = (ScaleEvent) event;
					System.out.println("\t" + scaleEvent.toString());
				}else {
					Statistic statistic = (Statistic) event;
					System.out.println("\t" + statistic.toString());
				}
			}
		}
		replayer.destroyConnection();
	}

}
