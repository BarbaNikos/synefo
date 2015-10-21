package gr.katsip.tpch;

public class Supplier {

	/**
	 * S_SUPPKEY BIGINT (or INT)
	 * S_NAME VARCHAR(25)
	 * S_ADDRESS VARCHAR(40) 
	 * S_NATIONKEY INT
	 * S_PHONE VARCHAR(15)
	 * S_ACCTBAL DECIMAL
	 * S_COMMENT VARCHAR(101)
	 */
	
	public static final String[] schema = {
		"S_SUPPKEY",
		"S_NAME", 
		"S_ADDRESS", 
		"S_NATIONKEY", 
		"S_PHONE", 
		"S_ACCTBAL", 
		"S_COMMENT"
	};
	
	public static final String[] query5Schema = {
		"S_SUPPKEY",
		"S_NATIONKEY"
	};
}
