package gr.katsip.cestorm.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ExperimentReplayer {
	
	private static final String retrieveQueryInformation = "SELECT * FROM query WHERE query_id = ?";
	
	private static final String retrieveOperators = "SELECT * FROM operator WHERE query_id = ?";
	
	private static final String retrieveOperatorAdjacencyList = "SELECT * FROM operator_adjacency_list WHERE query_id = ?";
	
	private static final String retrieveInitialActiveTopology = "SELECT * from topology_operator WHERE start_time = (SELECT min(start_time) FROM topology_operator WHERE end_time IS NOT NULL AND operator_id IN (SELECT id FROM operator WHERE query_id = ?))";

	private static final String retrieveTopologyOperators = "SELECT * FROM topology_operator WHERE query_id = ?";
	
	private static final String retrieveSceleEvents = "SELECT * FROM scale_event";
	
	private static final String retrieveStatistics = "SELECT * FROM statistic";
	
	private String url = null;

	private String user = null;

	private String password = null;

	private Connection connection = null;
	
	public class Query {
		
		public int id;
		
		public int clientId;
		
		public String query;
		
	}
	
	public class Operator {
		
		public Integer id;
		
		public String name;
		
		public String ipAddress;
		
		public Integer queryId;
		
		public Integer x;
		
		public Integer y;
		
		public String type;
		
	}
	
	public class OperatorAdjacencyListEntry {
		
		public Integer queryId;
		
		public Integer parentId;
		
		public Integer childId;
		
	}
	
	public class TopologyOperatorEntry {
		
		public Integer operatorId;
		
		public String status;
		
		public Long startTime;
		
		public Long endTime;
		
	}
	
	public class ScaleEvent {
		
		public Integer id;
		
		public Integer operatorId;
		
		public String action;
		
		public Long timestamp;
		
	}
	
	public class Statistic {
		
		public Integer statisticId;
		
		public Integer operatorId;
		
		public Long timestamp;
		
		public Float cpu;
		
		public Float memory;
		
		public Integer latency;
		
		public Integer throughput;
		
		public Float selectivity;
		
		public Integer plain;
		
		public Integer det;
		
		public Integer rnd;
		
		public Integer ope;
		
		public Integer hom;
		
	}
	
	private enum Type {
		SCALE_EVENT,
		TOPOLOGY_OPERATOR,
		STAT
	}
	
	private class CrypefoEvent {
		public Type eventType;
		
		public List<Object> fields;
		
		public CrypefoEvent(Type eventType, List<Object> fields) {
			this.eventType = eventType;
			this.fields = new ArrayList<Object>(fields);
		}
	}
	
	private Integer queryId = -1;
	
	private Query query = null;
	
	private ArrayList<Operator> operators = null;
	
	public Query getQuery() {
		return query;
	}

	public ArrayList<Operator> getOperators() {
		return operators;
	}

	public ArrayList<OperatorAdjacencyListEntry> getOperatorAdjacencyList() {
		return operatorAdjacencyList;
	}

	private ArrayList<OperatorAdjacencyListEntry> operatorAdjacencyList = null;
	
	private ArrayList<TopologyOperatorEntry> initialTopologyOperatorList = null;
	
	public ArrayList<TopologyOperatorEntry> getInitialTopologyOperatorList() {
		return initialTopologyOperatorList;
	}

	public ExperimentReplayer(String url, String user, String password, int queryId) {
		this.url = url;
		this.user = user;
		this.password = password;
		try {
			connection = DriverManager.getConnection(this.url, this.user, this.password);
		} catch (SQLException e) {
			System.err.println("ExperimentReplayer failed to connect to MySQL DBMS: " + e.getMessage());
			e.printStackTrace();
		}
		this.queryId = queryId;
	}
	
	public void retrieveExperimentData() {
		/**
		 * Initially retrieve query information
		 */
		try {
			connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			connection.setAutoCommit(false);
			PreparedStatement prepStatement = connection.prepareStatement(retrieveQueryInformation);
			prepStatement.setInt(1, queryId);
			ResultSet result = prepStatement.executeQuery();
			while(result.next()) {
				if(result.wasNull() == false) {
					query = new Query();
					query.id = result.getInt("query_id");
					query.clientId = result.getInt("client_id");
					query.query = result.getString("query");
				}
			}
			result.close();
		} catch(SQLException e) {
			System.err.println("ExperimentReplayer encountered error when retrieving query information: " + e.getMessage());
			e.printStackTrace();
			try {
				connection.rollback();
				connection.setAutoCommit(true);
				return;
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		/**
		 * Moving on to retrieving operator information
		 */
		try {
			connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			connection.setAutoCommit(false);
			PreparedStatement prepStatement = connection.prepareStatement(retrieveOperators);
			prepStatement.setInt(1, queryId);
			ResultSet result = prepStatement.executeQuery();
			operators = new ArrayList<Operator>();
			while(result.next()) {
				if(result.wasNull() == false) {
					Operator op = new Operator();
					op.id = result.getInt("id");
					op.name = result.getString("name");
					op.ipAddress = result.getString("ip_address");
					op.queryId = result.getInt("query_id");
					op.x = result.getInt("x_coord");
					op.y = result.getInt("y_coord");
					op.type = result.getString("type");
					operators.add(op);
				}
			}
			result.close();
		} catch(SQLException e) {
			System.err.println("ExperimentReplayer encountered error when retrieving operators: " + e.getMessage());
			e.printStackTrace();
			try {
				connection.rollback();
				connection.setAutoCommit(true);
				return;
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		/**
		 * Retrieving operator adjacency list
		 */
		try {
			connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			connection.setAutoCommit(false);
			PreparedStatement prepStatement = connection.prepareStatement(retrieveOperatorAdjacencyList);
			prepStatement.setInt(1, queryId);
			ResultSet result = prepStatement.executeQuery();
			operatorAdjacencyList = new ArrayList<OperatorAdjacencyListEntry>();
			while(result.next()) {
				if(result.wasNull() == false) {
					OperatorAdjacencyListEntry entry = new OperatorAdjacencyListEntry();
					entry.queryId = result.getInt("query_id");
					entry.parentId = result.getInt("parent_id");
					entry.childId = result.getInt("child_id");
					operatorAdjacencyList.add(entry);
				}
			}
			result.close();
		} catch(SQLException e) {
			System.err.println("ExperimentReplayer encountered error when retrieving operators' adjacency list: " + e.getMessage());
			e.printStackTrace();
			try {
				connection.rollback();
				connection.setAutoCommit(true);
				return;
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		/**
		 * Retrieve initial active topology
		 */
		try {
			connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			connection.setAutoCommit(false);
			PreparedStatement prepStatement = connection.prepareStatement(retrieveInitialActiveTopology);
			prepStatement.setInt(1, queryId);
			ResultSet result = prepStatement.executeQuery();
			initialTopologyOperatorList = new ArrayList<TopologyOperatorEntry>();
			while(result.next()) {
				if(result.wasNull() == false) {
					TopologyOperatorEntry topologyOperator = new TopologyOperatorEntry();
					topologyOperator.operatorId = result.getInt("operator_id");
					topologyOperator.status = result.getString("status");
					topologyOperator.startTime = result.getLong("start_time");
					topologyOperator.endTime = result.getLong("end_time");
					initialTopologyOperatorList.add(topologyOperator);
				}
			}
			result.close();
		} catch(SQLException e) {
			System.err.println("ExperimentReplayer encountered error when retrieving initial active topology: " + e.getMessage());
			e.printStackTrace();
			try {
				connection.rollback();
				connection.setAutoCommit(true);
				return;
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	public void destroyConnection() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
