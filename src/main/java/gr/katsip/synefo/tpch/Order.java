package gr.katsip.synefo.tpch;

public class Order {

	/**
	 * O_ORDERKEY BIGINT
	 * O_CUSTKEY BIGINT
	 * O_ORDERSTATUS VARCHAR(1)
	 * O_TOTALPRICE DECIMAL
	 * O_ORDERDATE DATE
	 * O_ORDERPRIORITY VARCHAR(15)
	 * O_CLERK VARCHAR(15)
	 * O_SHIPPRIORITY INT
	 * O_COMMENT VARCHAR(79)
	 */
	
	public static final String[] schema = {
		"O_ORDERKEY", 
		"O_CUSTKEY", 
		"O_ORDERSTATUS", 
		"O_TOTALPRICE", 
		"O_ORDERDATE", 
		"O_ORDERPRIORITY", 
		"O_CLERK", 
		"O_SHIPPRIORITY", 
		"O_COMMENT"
	};
}

