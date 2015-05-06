package gr.katsip.synefo.storm.operators.synefo_comp_ops;

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
		for(int i=0;i<numDataProviders;i++){
			String sql = "INSERT INTO data_provider VALUES ("+i+","+dpNames[i]+");";			
			try {
				stmt = conn.createStatement();
				int rs = stmt.executeUpdate(sql);
				if(rs == 0) {
					System.err.println("initDataSender.providerInfoSender(): Initial data not loaded.");
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		for(int i=0;i<policies.length;i++){
			String[] current= policies[i].split(",");
			String dpId = current[0];
			String policy = current[1];
			String sql = "INSERT INTO policy VALUES ("+dpId+","+policy+");";
			try {
				stmt = conn.createStatement();
				int rs = stmt.executeUpdate(sql);
				if(rs == 0) {
					System.err.println("initDataSender.providerInfoSender(): Initial policies not loaded.");
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		for (int i=0;i<attributes.length;i++){
			String sql = "INSERT INTO attribute VALUES ("+i+","+attributes[i]+");";
			try {
				stmt = conn.createStatement();
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
