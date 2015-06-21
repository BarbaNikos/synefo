package gr.katsip.synefo.tpch;

public class Region {

	/**
	 * R_REGIONKEY INT
	 * R_NAME VARCHAR(25)
	 * R_COMMENT VARCHAR(152)
	 */
	
	public static final String[] schema = {
		"R_REGIONKEY", 
		"R_NAME", 
		"R_COMMENT"
	};
	
	public static final String[] query5Schema = {
		"R_REGIONKEY", 
		"R_NAME"
	};
}
