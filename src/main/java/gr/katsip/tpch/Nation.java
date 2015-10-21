package gr.katsip.tpch;

public class Nation {

	/**
	 * N_NATIONKEY INT
	 * N_NAME VARCHAR(25)
	 * N_REGIONKEY INT
	 * N_COMMENT VARCHAR(152)
	 */
	
	public static final String[] schema = {
		"N_NATIONKEY", 
		"N_NAME", 
		"N_REGIONKEY", 
		"N_COMMENT"
	};
	
	public static final String[] query5schema = {
		"N_NATIONKEY", 
		"N_REGIONKEY"
	};
}
