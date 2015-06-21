package gr.katsip.synefo.tpch;

public class LineItem {

	/**
	 * L_ORDERKEY BIGINT
	 * L_PARTKEY BIGINT
	 * L_SUPPKEY BIGINT
	 * L_LINENUMBER INT
	 * L_QUANTITY DECIMAL
	 * L_EXTENDEDPRICE DECIMAL
	 * L_DISCOUNT DECIMAL
	 * L_TAX DECIMAL
	 * L_RETURNFLAG VARCHAR(1)
	 * L_LINESTATUS VARCHAR(1)
	 * L_SHIPDATE DATE
	 * L_COMMITDATE DATE
	 * L_RECEIPTDATE DATE
	 * L_SHIPINSTRUCT VARCHAR(25)
	 * L_SHIPMODE VARCHAR(10)
	 * L_COMMENT VARCHAR(44)
	 */
	
	public static final String[] schema = {
		"L_ORDERKEY", 
		"L_PARTKEY", 
		"L_SUPPKEY", 
		"L_LINENUMBER", 
		"L_QUANTITY", 
		"L_EXTENDEDPRICE", 
		"L_DISCOUNT", 
		"L_TAX", 
		"L_RETURNFLAG", 
		"L_LINESTATUS", 
		"L_SHIPDATE", 
		"L_COMMITDATE", 
		"L_RECEIPTDATE", 
		"L_SHIPINSTRUCT", 
		"L_SHIPMODE", 
		"L_COMMENT"
	};
	
	public static final String[] query5Schema = {
		"L_ORDERKEY", 
		"L_SUPPKEY", 
		"L_EXTENDEDPRICE", 
		"L_DISCOUNT"
	};
}
