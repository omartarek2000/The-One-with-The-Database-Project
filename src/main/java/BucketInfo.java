public class BucketInfo {
	
	private Tuple tuple;
	private String key;
	private Cell cell;
	
	public BucketInfo(Tuple t, String k, Cell c) {
		
		this.tuple=t;
		this.key=k;
		this.cell = c;
		
	}

	public Tuple getTuple() {
		return tuple;
	}

	public void setTuple(Tuple tuple) {
		this.tuple = tuple;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
	
}