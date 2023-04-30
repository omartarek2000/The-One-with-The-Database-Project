import java.io.Serializable;
import java.util.Vector;

public class Tuple implements Serializable {
	
	private Vector<String> tuple;
	
	public Tuple() {
		this.setTuple(null);
		tuple = new Vector<String>();
		
		
	}

	public Vector<String> getTuple() {
		return tuple;
	}

	public void setTuple(Vector<String> tuple) {
		this.tuple = tuple;
	}
	
	public String getPK() {
		return this.tuple.get(0);
	}
	
	
	
	
	

}
