package gr.katsip.cestorm.db;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;


public class CEStormDatabaseManager {

	private String url = null;

	private String user = null;

	private String password = null;

	private Connection connection = null;

	private static final String retrieveLatestActiveTopologyTimestamp = "SELECT MAX(start_time) FROM topology_operator WHERE end_time IS NULL";

	private static final String retrieveOperatorIdentifiers = "SELECT id FROM operator WHERE query_id = ?";
	
	private static final String retrieveOperatorNames = "SELECT name FROM operator";
	
	private static final String retrieveOperatorNamesIdentifiers = "SELECT id, name FROM operator WHERE query_id = ?";

	private static final String insertOperatorActivityStatus = "INSERT INTO topology_operator (operator_id, status, start_time, end_time) VALUES(?,?,?,?)";

	private static final String updateActiveTopologyEndTime = "UPDATE topology_operator SET end_time = ? where start_time = ?";
	
	private static final String retrieveOperatorId = "SELECT id FROM operator WHERE name = ?";

	private static final String insertScaleEvent = "INSERT INTO scale_event (operator_id, action, timestamp) VALUES (?,?,?)";
	
	private static final String insertQuery = "INSERT INTO query (client_id, query) VALUES(?,?)";
	
	private static final String retrieveQueryIdentifier = "SELECT query_id FROM query WHERE client_id = ? AND query = ?";
	
	private static final String insertOperator = "INSERT INTO operator (name, ip_address, query_id, x_coord, y_coord, type) VALUES (?,?,?,?,?,?)";
	
	private static final String insertOperatorAdjacencyRecord = "INSERT INTO operator_adjacency_list (query_id, parent_id, child_id) values(?,?,?)";

	CEStormDatabaseManager(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
		try {
			connection = DriverManager.getConnection(this.url, this.user, this.password);
		} catch (SQLException e) {
			System.err.println("CEStormDatabaseManager failed to connect to MySQL DBMS: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public void destroy() {
		try {
			connection.close();
		} catch (SQLException e) {
			System.err.println("CEStormDatabaseManager failed to disconnect from MySQL DBMS: " + 
					e.getMessage());
			e.printStackTrace();
		}
	}
	
	public Integer insertQuery(Integer clientId, String query) {
		Integer queryIdentifier = -1;
		try {
			connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			connection.setAutoCommit(false);
			PreparedStatement prepStatement = connection.prepareStatement(insertQuery);
			prepStatement.setInt(1, clientId);
			prepStatement.setString(2, query);
			prepStatement.executeUpdate();
			prepStatement = connection.prepareStatement(retrieveQueryIdentifier);
			prepStatement.setInt(1, clientId);
			prepStatement.setString(2, query);
			connection.commit();
			connection.setAutoCommit(true);
			ResultSet result = prepStatement.executeQuery();
			while(result.next()) {
				if(result.wasNull() == false)
					queryIdentifier = result.getInt(1);
			}
			result.close();
		} catch(SQLException e) {
			System.err.println("CEStormDatabaseManager encountered error when inserting query: " + e.getMessage());
			e.printStackTrace();
			try {
				connection.rollback();
				connection.setAutoCommit(true);
				return -1;
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		return queryIdentifier;
	}
	
	public void insertOperator(String name, String ip_address, Integer queryId, Integer x, Integer y, String type) {
		try {
			connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			connection.setAutoCommit(false);
			PreparedStatement prepStatement = connection.prepareStatement(insertOperator);
			prepStatement.setString(1, name);
			prepStatement.setString(2, ip_address);
			prepStatement.setInt(3, queryId);
			prepStatement.setInt(4, x);
			prepStatement.setInt(5, y);
			prepStatement.setString(6, type);
			prepStatement.executeUpdate();
			connection.commit();
			connection.setAutoCommit(true);
		} catch(SQLException e) {
			System.err.println("CEStormDatabaseManager encountered error when inserting operator: " + e.getMessage());
			e.printStackTrace();
			try {
				connection.rollback();
				connection.setAutoCommit(true);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	public void insertOperatorAdjacencyList(Integer queryId, HashMap<String, List<String>> topology) {
		HashMap<String, Integer> nameToIdMap = getOperatorNameToIdentifiersMap(queryId);
		HashMap<Integer, List<Integer>> numericalTopology = new HashMap<Integer, List<Integer>>();
		Iterator<Entry<String, List<String>>> itr = topology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, List<String>> pair = itr.next();
			String operatorName = pair.getKey();
			List<String> childrenNames = pair.getValue();
			List<Integer> childrenIdentifiers = new ArrayList<Integer>();
			for(String child : childrenNames) {
				childrenIdentifiers.add(nameToIdMap.get(child));
			}
			numericalTopology.put(nameToIdMap.get(operatorName), childrenIdentifiers);
		}
		try {
			connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			connection.setAutoCommit(false);
			PreparedStatement prepStatement = connection.prepareStatement(insertOperatorAdjacencyRecord);
			Iterator<Entry<Integer, List<Integer>>> numericItr = numericalTopology.entrySet().iterator();
			while(numericItr.hasNext()) {
				Entry<Integer, List<Integer>> pair = numericItr.next();
				Integer parentId = pair.getKey();
				List<Integer> children = pair.getValue();
				for(Integer child : children) {
					prepStatement.setInt(1, queryId);
					prepStatement.setInt(2, parentId);
					prepStatement.setInt(3, child);
					prepStatement.addBatch();
				}
			}
			prepStatement.executeBatch();
			connection.commit();
			connection.setAutoCommit(true);
		} catch(SQLException e) {
			System.err.println("CEStormDatabaseManager encountered error when inserting topology information: " + e.getMessage());
			e.printStackTrace();
			try {
				connection.rollback();
				connection.setAutoCommit(true);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	public HashMap<String, Integer> getOperatorNameToIdentifiersMap(Integer queryId) {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		try {
			PreparedStatement prepStatement = connection.prepareStatement(retrieveOperatorNamesIdentifiers);
			prepStatement.setInt(1, queryId);
			ResultSet result = prepStatement.executeQuery();
			while(result.next()) {
				if(result.wasNull() == false) {
					map.put(result.getString(2), result.getInt(1));
				}
			}
			result.close();
		} catch(SQLException e) {
			e.printStackTrace();
		}
		return map;
	}

	public List<Integer> getOperatorIdentifiers(Integer queryId) {
		/**
		 * Get operator ids first
		 */
		ArrayList<Integer> operators = null;
		try {
			PreparedStatement prepStatement = connection.prepareStatement(retrieveOperatorIdentifiers);
			prepStatement.setInt(1, queryId);
			operators = new ArrayList<Integer>();
			ResultSet result = prepStatement.executeQuery();
			result.setFetchSize(5);
			while(result.next()) {
				if(!result.wasNull()) {
					operators.add(result.getInt(1));
				}
			}
			result.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return operators;
	}
	
	public List<String> getOperatorNames() {
		ArrayList<String> operators = null;
		try {
			PreparedStatement prepStatement = connection.prepareStatement(retrieveOperatorNames);
			operators = new ArrayList<String>();
			ResultSet result = prepStatement.executeQuery();
			result.setFetchSize(5);
			while(result.next()) {
				if(!result.wasNull()) {
					operators.add(result.getString(1));
				}
			}
			result.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return operators;
	}

	public void updateActiveTopology(final List<Integer> operatorIdentifiers, final HashMap<Integer, String> operatorActivityState) {
		try {
			connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			connection.setAutoCommit(false);
			PreparedStatement prepStatement = connection.prepareStatement(retrieveLatestActiveTopologyTimestamp);
			ResultSet result = prepStatement.executeQuery();
			result.setFetchSize(1);
			Long previousStartTime = -1L;
			while(result.next()) {
				if(!result.wasNull()) {
					previousStartTime = result.getLong(1);
				}
			}
			result.close();
			/**
			 * Insert new values
			 */
			Long currentTimestamp = System.currentTimeMillis();
			prepStatement = connection.prepareStatement(insertOperatorActivityStatus);
			for(Integer operator : operatorIdentifiers) {
				prepStatement.setInt(1, operator);
				prepStatement.setString(2, operatorActivityState.get(operator));
				prepStatement.setLong(3, currentTimestamp);
				prepStatement.setNull(4, java.sql.Types.BIGINT);
				prepStatement.addBatch();
			}
			prepStatement.executeBatch();
			/**
			 * Update previous values with endTime equal to currentTimestamp
			 */
			if(previousStartTime != -1L) {
				prepStatement = connection.prepareStatement(updateActiveTopologyEndTime);
				prepStatement.setLong(1, currentTimestamp);
				prepStatement.setLong(2, previousStartTime);
				prepStatement.executeUpdate();
			}
			connection.commit();
			connection.setAutoCommit(true);
		} catch(SQLException e) {
			System.err.println("CEStormDatabaseManager encountered error when updating topology: " + e.getMessage());
			e.printStackTrace();
			try {
				System.err.println("CEStormDatabaseManager encountered error when rolling-back transcation: " + e.getMessage());
				connection.rollback();
				connection.setAutoCommit(true);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}

	public void insertScaleEvent(String operatorName, String action) {
		Long timestamp = System.currentTimeMillis();
		try{
			connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			connection.setAutoCommit(false);
			PreparedStatement prepStatement = connection.prepareStatement(retrieveOperatorId);
			prepStatement.setString(1, operatorName);
			Integer operatorId = -1;
			ResultSet result = prepStatement.executeQuery();
			while(result.next()) {
				if(result.wasNull() == false)
					operatorId = result.getInt(1);
			}
			result.close();
			if(operatorId == -1) {
				System.err.println("CEStormDatabaseManager no operator id found for given operator name");
				connection.rollback();
				connection.setAutoCommit(true);
				return;
			}
			prepStatement = connection.prepareStatement(insertScaleEvent);
			prepStatement.setInt(1, operatorId);
			prepStatement.setString(2, action);
			prepStatement.setLong(3, timestamp);
			prepStatement.executeUpdate();
			connection.commit();
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			System.err.println("CEStormDatabaseManager encountered error when inserting scale event: " + e.getMessage());
			try {
				connection.rollback();
				connection.setAutoCommit(true);
			} catch (SQLException e1) {
				System.err.println("CEStormDatabaseManager encountered error when rolling-back insert scale-event-transcation: " + e.getMessage());
				e1.printStackTrace();
			}
		}
	}

}