package gr.katsip.cestorm.db;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class CEStormDatabaseManager {

	private String url = null;

	private String user = null;

	private String password = null;

	private Connection connection = null;

	private static final String retrieveLatestActiveTopologyTimestamp = "SELECT MAX(start_time) FROM topology_operator WHERE end_time IS NULL";

	private static final String retrieveOperatorIdentifiers = "SELECT id FROM operator";
	
	private static final String retrieveOperatorNames = "SELECT name FROM operator";

	private static final String insertOperatorActivityStatus = "INSERT INTO topology_operator (operator_id, status, start_time, end_time) VALUES(?,?,?,?)";

	private static final String updateActiveTopologyEndTime = "UPDATE topology_operator SET end_time = ? where start_time = ?";
	
	private static final String retrieveOperatorId = "SELECT id FROM operator WHERE name = ?";

	private static final String insertScaleEvent = "INSERT INTO scale_event (operator_id, action, timestamp) VALUES (?,?,?)";

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

	public List<Integer> getOperatorIdentifiers() {
		/**
		 * Get operator ids first
		 */
		ArrayList<Integer> operators = null;
		try {
			PreparedStatement prepStatement = connection.prepareStatement(retrieveOperatorIdentifiers);
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
			} catch (SQLException e1) {
				System.err.println("CEStormDatabaseManager encountered error when rolling-back insert scale-event-transcation: " + e.getMessage());
				e1.printStackTrace();
			}
		}
	}

}
