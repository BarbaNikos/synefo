package gr.katsip.deprecated.crypstream;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class initDataSender {
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"; 
	static final String DB_URL = "jdbc:mysql://db10.cs.pitt.edu/stormxl";

	static  String USER = "cory.thoma";
	static final String PASS = "19feb15c";
	static Connection conn = null;
	static Statement stmt = null;

	public initDataSender(){
		connectToDB();
	}

	public void connectToDB(){
		try{
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL,USER,PASS);
		}catch(SQLException se){
			System.out.println("Error connecting to DB: "+se);
			System.exit(1);
			se.printStackTrace();
		}catch(Exception e){
			System.out.println("Error connecting to DB: "+e);
			System.exit(1);
			e.printStackTrace();
		}
	}
	/**
	 * Send initial information to the database:
	 * 		Data Provider Policy
	 * 		Data Consumer Attributes
	 */
	public void providerInfoSender(int numDataProviders, String[] policies, String[] dpNames, String[] attributes){
		try {
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			stmt = conn.createStatement();
			int rs = stmt.executeUpdate("TRUNCATE TABLE data_provider");
			if(rs == 0) {
				System.err.println("initDataSender.providerInfoSender(): truncate data_provider table failed.");
			}
			 rs = stmt.executeUpdate("TRUNCATE TABLE policy");
			if(rs == 0) {
				System.err.println("initDataSender.providerInfoSender(): truncate data_provider table failed.");
			}
			 rs = stmt.executeUpdate("TRUNCATE TABLE attribute");
			if(rs == 0) {
				System.err.println("initDataSender.providerInfoSender(): truncate data_provider table failed.");
			}
			for(int i=0;i<numDataProviders;i++){
				String sql = "INSERT INTO data_provider (id, name) VALUES ("+i+",\""+dpNames[i]+"\");";			
				stmt = conn.createStatement();
				System.out.println(sql);
				rs = stmt.executeUpdate(sql);
				if(rs == 0) {
					System.err.println("initDataSender.providerInfoSender(): Initial data not loaded.");
					conn.rollback();
					conn.setAutoCommit(true);
					return;
				}
			}
			conn.commit();
			conn.setAutoCommit(true);
		} catch(SQLException e) {
			try {
				conn.rollback();
				conn.setAutoCommit(true);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		for(int i=0;i<policies.length;i++){
			String[] current= policies[i].split(":");
			String dpId = current[0];
			String policy = current[1]+","+current[2];
			String sql = "INSERT INTO policy (provider_id, policy) VALUES ("+dpId+",\""+policy+"\");";
			try {
				stmt = conn.createStatement();
				System.out.println(sql);
				int rs = stmt.executeUpdate(sql);
				if(rs == 0) {
					System.err.println("initDataSender.providerInfoSender(): Initial policies not loaded.");
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		for (int i=0;i<attributes.length;i++){
			String sql = "INSERT INTO attribute (id,name) VALUES ("+i+",\""+attributes[i]+"\");";
			try {
				stmt = conn.createStatement();
				System.out.println(sql);
				int rs = stmt.executeUpdate(sql);
				if(rs == 0) {
					System.err.println("initDataSender.providerInfoSender(): Initial attributes not loaded.");
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}		
	}
}
