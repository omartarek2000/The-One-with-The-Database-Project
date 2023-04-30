import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Vector;

public class Bucket {
	
	private Bucket overflow;
	private int noOfOverflows;
	
	

	private Vector<BucketInfo> bucket;
	
	private Vector<Cell> cellsInTheBuckets;
	


	public Bucket() {
		setBucket(new Vector<BucketInfo>(this.readconfig()));
		cellsInTheBuckets = new Vector<Cell>();
		overflow = null;
		noOfOverflows=0;
		
	}
	
	public static int readconfig() {
		Properties prop = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		InputStream is = null;
		try {
		    is = new FileInputStream(fileName);
		} catch (FileNotFoundException ex) {
		  
		}
		try {
		    prop.load(is);
		} catch (IOException ex) {
		}
		return Integer.parseInt((prop.getProperty("MaximumKeysCountinIndexBucket")));
		
	}
	
	public int getNoOfOverflows() {
		return noOfOverflows;
	}

	public void setNoOfOverflows(int noOfOverflows) {
		this.noOfOverflows = noOfOverflows;
	}
	public Bucket getOverflow() {
		return overflow;
	}

	public void setOverflow(Bucket overflow) {
		this.overflow = overflow;
	}

	public Vector<BucketInfo> getBucket() {
		return bucket;
	}

	public void setBucket(Vector<BucketInfo> bucket) {
		this.bucket = bucket;
	}
	
	public Vector<Cell> getCellsInTheBuckets() {
		return cellsInTheBuckets;
	}

	public void setCellsInTheBuckets(Vector<Cell> cellsInTheBuckets) {
		this.cellsInTheBuckets = cellsInTheBuckets;
	}

	

}
