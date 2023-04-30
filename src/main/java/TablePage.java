import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Properties;
import java.util.Vector ;



public class TablePage implements Serializable{
	
	private Vector<Tuple> page;
	private String minPK;
	private TablePage overflowPage;
	
	
	public TablePage() throws IOException {
		setPage(new Vector<Tuple>(this.readconfig()));
/*		setMinPK(this.getPage().firstElement().getPK());
		setMaxPK(this.getPage().lastElement().getPK());*/
		
		
		minPK="";
		overflowPage= null;
		
	}
	

	
	public  int findInsertingIndex(String pk, String dataType) throws ParseException {
		
		
//		if(pk.compareTo(page.get(0).getPK())<0) {
//			return 0;
//		}1 2 3 4 5 6
		for(int i =0 ;i<page.size()+1;i++) {
            Boolean biggerThanLastPk = i == 0 ||  page.get(i-1).getPK().compareTo(pk)<=0;
            Boolean lessThanNextPk = i == page.size() ||  page.get(i).getPK().compareTo(pk)>=0;
           
            if (biggerThanLastPk && lessThanNextPk)
            	return i;
		}
		
		System.out.println("YOU  SHOULD NOT BE HERE");
		return -1;

		
	}
	public static TablePage deserialize(String path) throws IOException, ClassNotFoundException {
		// Reading the object from a file
        FileInputStream file = new FileInputStream(path);
        ObjectInputStream in = new ObjectInputStream(file);
          
        // Method for deserialization of object
        TablePage obj = (TablePage)in.readObject();
          
        in.close();
        file.close();
        
        obj.updateminmaxPK();
        return obj;
	}
	
	
	
	public void serialize(String table, int index) throws IOException {
		
//			File file = new File("src/main/resources/data/"+table+"/"+index+".ser");

//			if(!file.exists()){
//				 file.createNewFile();
//			} 
			try {
				  FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/"+table+"/"+index+".ser");
			         ObjectOutputStream out = new ObjectOutputStream(fileOut);
			         out.writeObject(this);
			         out.close();
			         fileOut.close();
			         //System.out.printf("Serialized data is saved in page.ser");
			      } catch (IOException i) {
			         i.printStackTrace();
			      }
	       
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
		return Integer.parseInt((prop.getProperty("MaximumRowsCountinPage")));
		
	}
	


		public Vector<Tuple> getPage() {
			return page;
		}

		public void setPage(Vector<Tuple> page) {
			this.page = page;
		}
		
		
		public int getfirstrow(TablePage x) {
			return x.getPage().size() -1 ;
		}
		
		
		
		public String updateminmaxPK() {
			
			minPK= this.getPage().get(0).getPK();
//			System.out.println(minPK);
//			maxPK= this.getPage().lastElement().getPK();
			return minPK;
		
		}
		public String getMinPK() {
			return minPK;
		}
		public void setMinPK(String minPK) {
			this.minPK = minPK;
		}
		public TablePage getOverflowPage() {
			
			return overflowPage;
		}

		public void setOverflowPage(TablePage overflowPage) {
			this.overflowPage = overflowPage;
		}
//		public TablePage isPresent( String PK ) {
//				if((this.getMinPK().compareTo(PK) < 0) && 
//				(this.getMaxPK().compareTo(PK) > 0) ) {
//					return this;
//				}
//				return null;	
//		}
		
		
			    }
	

