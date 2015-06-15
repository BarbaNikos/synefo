package gr.katsip.synefo.server2;

public class JoinOperator {

	private int identifier;
	
	public enum Step {
		DISPATCH, 
		JOIN
	}
	
	private Step step;
	
	private String relation;
	
	public JoinOperator() {
		
	}
	
	public JoinOperator(int identifier, Step step, String relation) {
		this.identifier = identifier;
		this.step = step;
		this.relation = relation;
	}

	public int getIdentifier() {
		return identifier;
	}

	public void setIdentifier(int identifier) {
		this.identifier = identifier;
	}

	public Step getStep() {
		return step;
	}

	public void setStep(Step step) {
		this.step = step;
	}

	public String getRelation() {
		return relation;
	}

	public void setRelation(String relation) {
		this.relation = relation;
	}

	@Override
	public String toString() {
		return "JoinOperator [identifier=" + identifier + ", step=" + step
				+ ", relation=" + relation + "]";
	}
	
}
