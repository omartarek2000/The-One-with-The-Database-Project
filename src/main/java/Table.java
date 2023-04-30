import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable{
	
	
	private GridIndex gridIndex;
	//(String clusteringKey, Hashtable<String, String> colNameType,
	//Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax)
	private String tableName ;
	//	private String clusteringKey;
//	private Hashtable<String, String> colNameType;
//	private Hashtable<String, String> colNameMin;
//	private Hashtable<String, String> colNameMax;
	private Vector<TablePage> fulltable;
//	private ArrayList<String> header;
//	private Vector<Tuple> sortingvector;
	private ArrayList<Integer> pageSorting;
	
	
	
	public Table() {
		fulltable = new Vector<TablePage>();
//		header = new ArrayList<String>();
//		sortingvector = new Vector<Tuple>();
		pageSorting = new ArrayList<Integer>();
		gridIndex = null;
		
	}

	public  Table deserialize(String tableName) throws ClassNotFoundException, IOException {
		
		File x = new File("src/main/resources/data/"+tableName);
		if(!x.exists()) return this;
		
		for(int i = 0;;i++)
		{
			String path = "src/main/resources/data/"+tableName+"/"+i+".ser";
			x = new File(path);
			if(x.exists())
				fulltable.add(TablePage.deserialize(path));
			else
				break;
		}
		
		deserializePageSorting(tableName);
//		deserializeGridIndex(tableName);
;		
		return this;
	}
	
	public  void deserialize(int  i) throws ClassNotFoundException, IOException {

		File x = new File("src/main/resources/data/"+this.tableName);
		if(!x.exists()) return;
		
		
			String path = "src/main/resources/data/"+tableName+"/"+i+".ser";
			x = new File(path);
			if(x.exists())
				fulltable.add(TablePage.deserialize(path));
			
			
	}
	@SuppressWarnings("unchecked")
	public  void deserializePageSorting(String tableName) throws ClassNotFoundException, IOException {

		File x = new File("src/main/resources/data/"+tableName);
		if(!x.exists()) return;
		
			String path = "src/main/resources/data/"+tableName+"/"+"pageSorting.ser";
			x = new File(path);
			if(x.exists()) {
				 FileInputStream file = new FileInputStream(path);
	        ObjectInputStream in = new ObjectInputStream(file);
	          
	        // Method for deserialization of object
	        pageSorting = (ArrayList<Integer>) in.readObject();
	          
	        in.close();
	        file.close();
			}
			
			
	}
	
	@SuppressWarnings("unchecked")
	public  void deserializeGridIndex(String tableName) throws ClassNotFoundException, IOException {

		File x = new File("src/main/resources/data/"+tableName);
		if(!x.exists()) return;
		
			String path = "src/main/resources/data/"+tableName+"/"+"GridIndex.ser";
			x = new File(path);
			if(x.exists()) {
				 FileInputStream file = new FileInputStream(path);
	        ObjectInputStream in = new ObjectInputStream(file);
	          
	        // Method for deserialization of object
	        gridIndex = (GridIndex) in.readObject();
	          
	        in.close();
	        file.close();
			}
			
			
	}
	
	public void serialize() throws IOException {
		File dir = new File("src/main/resources/data/"+tableName+"/");
		if(!dir.exists())
			if(!dir.mkdirs())
				throw new IOException("Failed to create directory");
			
		for(int i = 0; i < fulltable.size();i++)
		{
			fulltable.get(i).serialize(tableName, i);	
		}
	}
	
	public void serialize(int p) throws IOException {
		File dir = new File("src/main/resources/data/"+tableName+"/");
		if(!dir.exists())
			if(!dir.mkdirs())
				throw new IOException("Failed to create directory");
			
//		for(int i = 0; i < fulltable.size();i++)
//		{
			fulltable.get(p).serialize(tableName, p);	
//		}
	}
	
	public void serializePageSorting() throws IOException {
		File dir = new File("src/main/resources/data/"+tableName+"/");
		if(!dir.exists())
			if(!dir.mkdirs())
				throw new IOException("Failed to create directory");
		try {
			  FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/"+tableName+"/"+"pageSorting.ser");
		         ObjectOutputStream out = new ObjectOutputStream(fileOut);
		         out.writeObject(pageSorting);
		         out.close();
		         fileOut.close();
		         //System.out.printf("Serialized data is saved in page.ser");
		      } catch (IOException i) {
		         i.printStackTrace();
		      }
	}
	
	public void serializeGridIndex() throws IOException {
//		File dir = new File("src/main/resources/data/"+tableName+"/");
//		if(!dir.exists())
//			if(!dir.mkdirs())
//				throw new IOException("Failed to create directory");
//		try {
//			  FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/"+tableName+"/"+"GridIndex.ser");
//		         ObjectOutputStream out = new ObjectOutputStream(fileOut);
//		         out.writeObject(gridIndex);
//		         out.close();
//		         fileOut.close();
//		         //System.out.printf("Serialized data is saved in page.ser");
//		      } catch (IOException i) {
//		         i.printStackTrace();
//		      }
		gridIndex.serialize(tableName);
		
	}
	
	public int nextPageIndex(int x) {
		if(pageSorting.size()-1>=pageSorting.indexOf(x)+1)
		return pageSorting.get(pageSorting.indexOf(x)+1);
		else
			return -1;
		
	}
		public void updateminmaxPK() {
			fulltable.get(pageSorting.get(0)).setMinPK("");
//			System.out.println(fulltable.size());
//			if(fulltable.size()==1)
//				fulltable.get(pageSorting.get(0)).setMaxPK(fulltable.get(pageSorting.get(0)).getPage().lastElement().getPK());
			
		for (int i = 1;i<fulltable.size();i++) {
//			if(i==1) {
//				fulltable.get(pageSorting.get(0)).setMaxPK(fulltable.get(pageSorting.get(i)).updateminmaxPK());
//			}
//				
			fulltable.get(pageSorting.get(i)).updateminmaxPK();
			
//			if(fulltable.size()>i+1)
//				fulltable.get(pageSorting.get(i)).setMaxPK(fulltable.get(pageSorting.get(i+1)).updateminmaxPK());
//			else 
//				fulltable.get(pageSorting.get(i)).setMaxPK(fulltable.get(pageSorting.get(i)).getPage().lastElement().getPK());
		}
	
		}
	 
		public String getTablename() {
			return tableName;
		}

		public void setTablename(String tablename) {
			this.tableName = tablename;
		}

		public GridIndex getGridIndex() {
			return gridIndex;
		}

		public void setGridIndex(GridIndex gridIndex) {
			this.gridIndex = gridIndex;
		}
//		public String getClusteringKey() {
//			return clusteringKey;
//		}
//
//		public void setClusteringKey(String clusteringKey) {
//			this.clusteringKey = clusteringKey;
//		}
//
//		public Hashtable<String, String> getColNameType() {
//			return colNameType;
//		}
//
//		public void setColNameType(Hashtable<String, String> colNameType) {
//			this.colNameType = colNameType;
//		}
//
//		public Hashtable<String, String> getColNameMin() {
//			return colNameMin;
//		}
//
//		public void setColNameMin(Hashtable<String, String> colNameMin) {
//			this.colNameMin = colNameMin;
//		}
//
//		public Hashtable<String, String> getColNameMax() {
//			return colNameMax;
//		}
//
//		public void setColNameMax(Hashtable<String, String> colNameMax) {
//			this.colNameMax = colNameMax;
//		}

		public Vector<TablePage> getFulltable() {
			return fulltable;
		}

		public void setFulltable(Vector<TablePage> fulltable) {
			this.fulltable = fulltable;
		}
		
		public int getfirstpage() {
			return this.getPageSorting().get(pageSorting.size()-1); 
		}

		public ArrayList<Integer> getPageSorting() {
			return pageSorting;
		}

		public void setPageSorting(ArrayList<Integer> pageSorting) {
			this.pageSorting = pageSorting;
		}

//		public ArrayList<String> getHeader() {
//			return header;
//		}
//
//		public void setHeader(ArrayList<String> header) {
//			this.header = header;
//		}
		
		
//		
//		public void sort() throws IOException {
//			
//			for( int i = 0; i< this.getFulltable().size(); i++) {
//				for( int j = 0; j< this.getFulltable().get(i).getPage().size(); j++) {
//					sortingvector.add(this.getFulltable().get(i).getPage().get(j));
//				}	
//			}
//			
//			for( int i = 0; i< this.getFulltable().size(); i++) {
//				this.getFulltable().get(i).getPage().clear();
//			}
//			
//			Collections.sort(sortingvector, new TupleComparator());
//			
//			
////			for(int i = 0; i< this.getFulltable().size() ; i++) {
////				
////				for(int j = 0 ;  j< this.getFulltable().get(i).getPage().size() && (sortingvector.isEmpty() == false) ; j++) {
//				while(sortingvector.size()!=0)
//					if(this.getFulltable().get(getfirstpage()).getPage().size()>TablePage.readconfig()) {
//						fulltable.add(new TablePage());
//					}
//					this.getFulltable().get(getfirstpage()).getPage().add(sortingvector.get(0));
//					sortingvector.remove(0);
//					
//					
////				}
////				
////			}
//
//			
//		}
}
//		class TupleComparator implements Comparator<Tuple>{
//			
//			@Override
//			public int compare(Tuple o1, Tuple o2) {
//				return o1.getTuple().get(0).compareTo(o2.getTuple().get(0));
//			}
//		}

		
		
		
		
		
		
		
		
		
		
		
	
		 
		
			    
