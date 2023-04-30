import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DBApp implements DBAppInterface, Serializable{
	
	public ArrayList<Table> alltables;
	
	

	@Override
	public void init() {
		
		alltables = new ArrayList<Table>();
		try {
		String line = "";  		
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));  
		while ((line = br.readLine()) != null)   //returns a Boolean value  
		{  
			String[] row = line.split(",");
			if(row.length!=0) {
			String tablename = row[0];
			Boolean tmp= true;
			for(int i = 0; i < alltables.size(); i++) {
				if(alltables.get(i).getTablename().compareTo(tablename)==0) {
					tmp =false;
				}	
			}
			if(tmp) {
				Table res = new Table();
				res.setTablename(tablename);

			alltables.add(res.deserialize(tablename));
						}
			}
			
		}  
	
		}catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	
		
	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException, IOException {
		// TODO Auto-generated method stub
		
		String line = "";  		
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));  
		while ((line = br.readLine()) != null)   //returns a Boolean value  
		{  
			String[] row = line.split(",");
			if(row.length!=0) {
			
				if(row[0].equals(tableName)) {
					return;
				}
						}
		
			
		} 
		
		
		
		Table x = new Table();
		x.setTablename(tableName);
//		x.setClusteringKey(clusteringKey);
//		x.setColNameType(colNameType);
//		x.setColNameMin(colNameMin);
//		x.setColNameMax(colNameMax);
//		
		alltables.add(x);
		
		
		
		
		// get keys() from Hashtable and iterate


		    FileWriter writer = new FileWriter("src/main/resources/metadata.csv", true);

		    writer.append(tableName);
		    writer.append(',');
		    writer.append(clusteringKey);
		    writer.append(',');
		    writer.append(colNameType.get(clusteringKey));
		    writer.append(',');
		    writer.append("true");
		    writer.append(',');
		    writer.append("false");
		    writer.append(',');
		    writer.append(colNameMin.get(clusteringKey));
		    writer.append(',');
		    writer.append(colNameMax.get(clusteringKey));
		    
		    writer.append('\n');

		

		 Enumeration<String> enumeration = colNameType.keys();
		 while(enumeration.hasMoreElements()) {
			  String key =enumeration.nextElement();
        	
		if(key.equals(clusteringKey)==false) { 
			 
		  
		  writer.append(tableName);
		  writer.append(',');
		  writer.append(key);
		  writer.append(',');
		  writer.append(colNameType.get(key));
		  writer.append(',');
		  writer.append("false");
		  writer.append(',');
		  writer.append("false");
		  writer.append(',');
		  writer.append(colNameMin.get(key));
		  writer.append(',');
		  writer.append(colNameMax.get(key));
		  
		  writer.append('\n');
		  
		  }
        
        }
        writer.flush();
		  writer.close();

//  			  x.serialize();
  		  
            		
            }
            
	
	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException, IOException, ParseException {
		// TODO Auto-generated method stub
		
		Table x = null;
		Tuple tmp = new Tuple();
		for(int i = 0; i < alltables.size(); i++) {
			if(alltables.get(i).getTablename().equals(tableName)) {
				x = alltables.get(i);
			}	
		}
		if(x==null) {
			throw new DBAppException("Table not Found");
		}
		
		int dimensionSize = columnNames.length;
		
		if((!x.getFulltable().isEmpty() && x.getFulltable().get(0).getPage().get(0).getTuple().size()<dimensionSize)) {
			
			throw new DBAppException();
		}
		
		
		
		
//		//create grid index
//		GridIndex grid = new GridIndex(columnNames.length);
//		
//		int indexInTable =0;
//		String line = "";  		
//		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));  
//		while ((line = br.readLine()) != null)   //returns a Boolean value  
//		{  
//		
//		String[] row = line.split(",");
//		if(row[0].equals(tableName)) {
//			
//			for (int i=0;i<columnNames.length;i++) {
//			if(row[1].equals(columnNames[i])) {
//				
//				//change index column for this row to true in the csv file
//				
//				DimensionInfo dimension = new DimensionInfo(columnNames[i]);
//				dimension.setRange(createRange(row[2], row[5], row[6]));
//				dimension.setIndexInTable(indexInTable);
//				dimension.setDataType(row[2]);
//				grid.getDimensionsInfo().add(dimension);
//				
//			}
//			}
//		
//			indexInTable++;
//		}
//		
//		}
		
		
		
		
		//start
		//create grid index
		GridIndex grid = new GridIndex(columnNames.length);
		int indexInTable =0;
		Boolean edited = false;
		String line = "";  
		String newfile="";
		File oldfile = new File ("src/main/resources/metadata.csv");
		BufferedReader br = new BufferedReader(new FileReader(oldfile));

//		File newfile = new File ("src/main/resources/metadata1.csv");
//		FileWriter writer = new FileWriter(newfile, false);


		while ((line = br.readLine()) != null)   //returns a Boolean value  
		{
			edited = false;

		String[] row = line.split(",");
		if(row.length>0) {
		if(row[0].equals(tableName)) {

		for (int i=0;i<columnNames.length;i++) {
		if(row[1].equals(columnNames[i])) {

		//change index column for this row to true in the csv file
		for(int j = 0; j< row.length ; j++) {
		if (j==4) {
		newfile = newfile+"true";
		newfile = newfile+",";

		}
		else {
			newfile = newfile+row[j];
			newfile = newfile+",";
		}
		}
		newfile = newfile+"\n";
		edited = true;



		DimensionInfo dimension = new DimensionInfo(columnNames[i]);
		dimension.setRange(createRange(row[2], row[5], row[6]));
		dimension.setIndexInTable(indexInTable);
		dimension.setDataType(row[2]);
		grid.getDimensionsInfo().add(dimension);

		}
		}
		if(!edited) {
		for(int i = 0; i< row.length ; i++) {
			newfile = newfile+row[i];
			newfile = newfile+",";
		}
		newfile = newfile+"\n";
		}

		indexInTable++;
		}
		else {
		for(int i = 0; i< row.length ; i++) {
			newfile = newfile+row[i];
			newfile = newfile+",";
		}
		newfile = newfile+"\n";

		}

		}
		}
//		writer.flush();
//		writer.close();
		br.close();
		
//		System.out.println(newfile);
		FileWriter writer = new FileWriter(oldfile, false);
		writer.append(newfile);
		writer.flush();
		writer.close();
//		oldfile.
//		oldfile.delete();
		
//		File metadata = new File ("src/main/resources/metadata.csv");
//		newfile.renameTo(metadata);
//		File delete = new File ("src/main/resources/tmp.csv");
		
//		newfile.renameTo(delete);
//		newfile.delete();
//		delete.delete();
		
		
		//end
		
		x.setGridIndex(grid);
		
		
		
		
		if(x.getFulltable().size()!=0) {
			
			for (int i =0; i<x.getFulltable().size();i++) {
				for(int j =0;j<x.getFulltable().get(i).getPage().size();j++) {
					Tuple t = x.getFulltable().get(i).getPage().get(j);
					int[] ins = new int[x.getGridIndex().getDimensionsInfo().size()];
					
					for(int k=0 ;k<x.getGridIndex().getDimensionsInfo().size();k++) {
						
						BigDecimal m  = turnIntoBigDecimal(x.getGridIndex().getDimensionsInfo().get(k).getDataType(), t.getTuple().get(x.getGridIndex().getDimensionsInfo().get(k).getIndexInTable()));
						
						for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(k).getRange().length;q++) {
							if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(k).getRange()[10])==0) {
								ins[k]=9;
								break;
							}
							
							if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(k).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(k).getRange()[q+1])<0) {
								ins[k]=	q;
								break;
								}
						}
						
						
					}
					
					// law feeh bucket gowa el cell 7ot el tuple fel bucket we law el bucket full e2semha
					if(x.getGridIndex().getByIndex(ins).getBucket()==null) {
						
						Bucket o = new Bucket();
						BucketInfo oi = new BucketInfo(t, t.getTuple().get(x.getGridIndex().getDimensionsInfo().get(0).getIndexInTable()), x.getGridIndex().getByIndex(ins));
						o.getBucket().add(oi);
						o.getCellsInTheBuckets().add(x.getGridIndex().getByIndex(ins));
						x.getGridIndex().getByIndex(ins).setBucket(o);
						
						
					}else {
						BucketInfo oi = new BucketInfo(t, t.getTuple().get(x.getGridIndex().getDimensionsInfo().get(0).getIndexInTable()), x.getGridIndex().getByIndex(ins));
						
						if(x.getGridIndex().getByIndex(ins).getBucket().getBucket().size()>=Bucket.readconfig()) {
							if(x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows()==0) {
								Bucket o = new Bucket();
								x.getGridIndex().getByIndex(ins).getBucket().setOverflow(o);
								x.getGridIndex().getByIndex(ins).getBucket().getOverflow().getBucket().add(oi);
								x.getGridIndex().getByIndex(ins).getBucket().setNoOfOverflows(x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows()+1);
								
							}else {
								Bucket tmp1 = x.getGridIndex().getByIndex(ins).getBucket();
								for(int i1=0; i1<x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows();i1++) {
									if(tmp1.getOverflow().getBucket().size()>=Bucket.readconfig()) {
										if(tmp1.getOverflow()!=null) {
										tmp1 = tmp1.getOverflow();
										}else {
											Bucket o = new Bucket();
											tmp1.setOverflow(o);
											o.getBucket().add(oi);
											x.getGridIndex().getByIndex(ins).getBucket().setNoOfOverflows(x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows()+1);
											
										}
									}else {
										tmp1.getOverflow().getBucket().add(oi);
									}
								}
							}
							
							
						}else {
							
						x.getGridIndex().getByIndex(ins).getBucket().getBucket().add(oi);
						}
						
					}
					//law mafeesh e3mel wahda we hotaha fel cell ely el index beta3ha fel ins array
					
				}
			}
		}
		
//		x.serializeGridIndex();
		
	}
	
	private static BigDecimal turnIntoBigDecimal(String dataType, String x) throws ParseException {
		
		switch(dataType) {
		 case "java.lang.String" :
		      // Statements
			  
			  byte[] string = x.getBytes();
			  
			  BigInteger stringi = new BigInteger(string);
			  
			  BigDecimal stringd = new BigDecimal(stringi);
  
			   return stringd;

			   
		   
		   case "java.lang.Double" :
		      // Statements
			   
			   Double d = Double.parseDouble(x);
			   
			   BigDecimal dd = new BigDecimal(d);
			   		  
			   return dd;
			
		      
		   case "java.util.Date" :
			      // Statements
			   Date da = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy").parse(x);

			   long minepoch = da.getTime();
			   
			   BigDecimal mindad = new BigDecimal(minepoch);

			   

			   return mindad;
			   
			  
			      
		   case "java.lang.Integer" :
			      // Statements
			   
			   int mini = Integer.parseInt(x);

			   
			   BigDecimal minid = new BigDecimal(mini);
   
			   return minid;
			 
		   default:
			   System.out.println("YOU SHOULD NOT GET HERE");
			   return null;
		}
		
	}
	
	
	private BigDecimal[] createRange(String dataType, String min, String max) throws ParseException {
		BigDecimal[] res = new BigDecimal[11];
		BigDecimal d = new BigDecimal(10);
		
		switch(dataType) {
		 case "java.lang.String" :
		      // Statements
			  
			  byte[] mins = min.getBytes();
			  byte[] maxs = max.getBytes();
			  
			  BigInteger minsi = new BigInteger(mins);
			  BigInteger maxsi = new BigInteger(maxs);
			  
			  BigDecimal minsd = new BigDecimal(minsi);
			  BigDecimal maxsd = new BigDecimal(maxsi);
			  
			  BigDecimal Sdiff = (maxsd.subtract(minsd)).divide(d);
			   res[0]=minsd;
			   res[10]=maxsd;
			   for(int i=1;i<10;i++) {
				   res[i] = minsd.add(Sdiff.multiply(new BigDecimal(i)));
			   }
			  
			   return res;

			   
		   
		   case "java.lang.Double" :
		      // Statements
			   
			   Double mind = Double.parseDouble(min);
			   Double maxd = Double.parseDouble(max);
			   
			   BigDecimal mindd = new BigDecimal(mind);
			   BigDecimal maxdd = new BigDecimal(maxd);
			   
			   BigDecimal Ddiff = (maxdd.subtract(mindd)).divide(d);
			   res[0]=mindd;
			   res[10]=maxdd;
			   for(int i=1;i<10;i++) {
				   res[i] = mindd.add(Ddiff.multiply(new BigDecimal(i)));
			   }
			  
			   return res;
			
		      
		   case "java.util.Date" :
			      // Statements
			   Date minda = null;
			   Date maxda = null;
			   if(min.contains("/")) {
				   String[] dateSplitmin = min.split("/");
				    minda = new Date(Integer.parseInt(dateSplitmin[2])-1900,Integer.parseInt(dateSplitmin[0])-1 , Integer.parseInt(dateSplitmin[1]));

			   }else if(min.contains("-")){
				   String[] dateSplitmin = min.split("-");
				    minda = new Date(Integer.parseInt(dateSplitmin[0])-1900,Integer.parseInt(dateSplitmin[1])-1 , Integer.parseInt(dateSplitmin[2]));

			   }
			   if(max.contains("/")) {
				   String[] dateSplitmax = max.split("/");
				    maxda = new Date(Integer.parseInt(dateSplitmax[2])-1900,Integer.parseInt(dateSplitmax[0])-1 , Integer.parseInt(dateSplitmax[1]));

			   }else if(max.contains("-")) {
				   String[] dateSplitmax = max.split("-");
				    maxda = new Date(Integer.parseInt(dateSplitmax[0])-1900,Integer.parseInt(dateSplitmax[1])-1 , Integer.parseInt(dateSplitmax[2]));

			   }
//			   Date minda = new SimpleDateFormat("yyyy-MM-dd").parse(min);
//			   Date maxda = new SimpleDateFormat("yyyy-MM-dd").parse(max);
			   
			   long minepoch = minda.getTime();
			   Long maxepoch = maxda.getTime();
			   
			   BigDecimal mindad = new BigDecimal(minepoch);
			   BigDecimal maxdad = new BigDecimal(maxepoch);
			   
			   BigDecimal Dadiff = (maxdad.subtract(mindad)).divide(d);
			   res[0]=mindad;
			   res[10]=maxdad;
			   for(int i=1;i<10;i++) {
				   res[i] = mindad.add(Dadiff.multiply(new BigDecimal(i)));
			   }
			  
			   return res;
			   
			  
			      
		   case "java.lang.Integer" :
			      // Statements
			   
			   int mini = Integer.parseInt(min);
			   int maxi = Integer.parseInt(max);
			   
			   BigDecimal minid = new BigDecimal(mini);
			   BigDecimal maxid = new BigDecimal(maxi);
			   
			   BigDecimal Idiff = (maxid.subtract(minid)).divide(d);
			   res[0]=minid;
			   res[10]=maxid;
			   for(int i=1;i<10;i++) {
				   res[i] = minid.add(Idiff.multiply(new BigDecimal(i)));
			   }
			   
			   return res;
			 
		   default:
			   System.out.println("YOU SHOULD NOT GET HERE");
			   return null;
		}
	}

	private String FormatKey(Object value, String dataType)
	{
		switch(dataType) {
		   case "java.lang.String" :
			   return (String)value;
		   case "java.lang.Double" :
			   return ((Double)value).toString();
		   case "java.util.Date" :
			   return ((Date)value).toString();
		   case "java.lang.Integer" :
			   return ((Integer)value).toString();
		   default:
			   System.out.println("YOU SHOULD NOT GET HERE");
			   return null;
		}
	}
	
	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException, ParseException {
		// TODO Auto-generated method stub
		Table x = null;
		Tuple tmp = new Tuple();
		for(int i = 0; i < alltables.size(); i++) {
			if(alltables.get(i).getTablename().equals(tableName)) {
				x = alltables.get(i);
			}	
		}
		if(x==null) {
			throw new DBAppException("Table not Found");
		}
		
//		for( int i = 0 ; i < x.getHeader().size(); i++) {
//			//read csv instead
//			 if((colNameValue.get(x.getHeader().get(i)).toString().compareTo(x.getColNameType().get(x.getHeader().get(i))) ==0)
//	            		&& (colNameValue.get(x.getHeader().get(i)).toString().compareToIgnoreCase(x.getColNameMin().get(x.getHeader().get(i))) >= 0)  
//	            		&& ((colNameValue.get(x.getHeader().get(i)).toString().compareToIgnoreCase(x.getColNameMax().get(x.getHeader().get(i))) <= 0) )) {
//			
//				 tmp.getTuple().add((String)colNameValue.get(x.getHeader().get(i)));
//			 }
//			
//		}
		//if(colNameValue["date_added"])
		

//		if(tmp.getPK().compareTo("13") == 0)
//		try {
//			if(((Date)colNameValue.get("date_added")).getTime() == 1600898400000l)
//		
//		{
//		 	System.out.print(tmp);
//		}}catch(  NullPointerException x1) {
//			
//		}
//		
		String dataType = "";
		String line = "";  		
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));  
		while ((line = br.readLine()) != null)   //returns a Boolean value  
		{  
		
		String[] row = line.split(",");
		if(row.length!=0) {
		if(row[0].equals(tableName)) {
			// 7otaha fe method bel parse ya basha
//			if( (colNameValue.get(row[1]).toString().compareToIgnoreCase(row[5]) >= 0)  
//            		&& ((colNameValue.get(row[1]).toString().compareToIgnoreCase(row[6])) <= 0) )  {
			
//			if(tmp.getTuple().size() == 0)
//				colNameValue.put(row[1], FormatKey(colNameValue.get(row[1]), row[2]));
//	
				
			
			if(eligibleToInsert(colNameValue.get(row[1]), row))	{
				if(row[3]=="TRUE"&&colNameValue.get(row[1])==null) {
					throw  new DBAppException("cannot insert");
				}

				Object value = colNameValue.get(row[1]);
				if(tmp.getTuple().size() == 0)
				{
					dataType = row[2];
					tmp.getTuple().add(FormatKey(value, row[2]));
				}
				else
					tmp.getTuple().add(value.toString());
//			System.out.println( colNameValue.get(row[1]).toString());
			
			colNameValue.remove(row[1]);
			}
		
		
//
//			}
//			}
	
	} 
		}
		}

	if(colNameValue.size()>0) {
		throw new DBAppException();
	}

//		int tupleIndex=-2;
		
		if(x.getFulltable().isEmpty()) {
			TablePage p = new TablePage();
			p.getPage().add(tmp);
			p.updateminmaxPK();

			x.getFulltable().add(p);
			x.getPageSorting().add(0);
			
			x.serialize(0);
		}
		else
		{
//			System.out.println( x.getPageSorting().size());
			int pageIndex = pagebinarySearch(x,tmp.getPK(), dataType);
			TablePage page = x.getFulltable().get(pageIndex);
			
			if(page.getPage().size()>=TablePage.readconfig()) {
				TablePage newPage = new TablePage();
				x.getFulltable().add(newPage);
				x.getPageSorting().add(x.getPageSorting().indexOf(pageIndex)+1, x.getFulltable().size()-1);
				
				int middle = page.getPage().size() / 2;

				Vector<Tuple> firstHalf = new Vector<Tuple>(page.getPage().subList(0, middle));
				Vector<Tuple> secondHalf = new Vector<Tuple>(page.getPage().subList(middle, page.getPage().size()));

				page.setPage(firstHalf);
				newPage.setPage(secondHalf);

				page.updateminmaxPK();
				newPage.updateminmaxPK();

				pageIndex = pagebinarySearch(x,tmp.getPK(), dataType);
				page = x.getFulltable().get(pageIndex);
//				if(page.getMinPK().compareTo(tmp.getPK())>0)
//					page = newPage;
				
				x.serialize(x.getFulltable().size()-1);
			}
			
			int insertingIndex = page.findInsertingIndex(tmp.getPK(), dataType);
			page.getPage().add(insertingIndex, tmp);
			
			x.serialize(pageIndex);
		}
		
		
		
		
		
		

		
		
		
//		x.setGridIndex(grid);
		if(x.getGridIndex()!=null) {
				
			
					int[] ins = new int[x.getGridIndex().getDimensionsInfo().size()];
					
					for(int k=0 ;k<x.getGridIndex().getDimensionsInfo().size();k++) {
						
						BigDecimal m  = turnIntoBigDecimal(x.getGridIndex().getDimensionsInfo().get(k).getDataType(), tmp.getTuple().get(x.getGridIndex().getDimensionsInfo().get(k).getIndexInTable()));
						
						for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(k).getRange().length;q++) {
							if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(k).getRange()[10])==0) {
								ins[k]=9;
								break;
							}
							
							if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(k).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(k).getRange()[q+1])<0) {
								ins[k]=	q;
								break;
								}
						}
						
						
					}
					
					// law feeh bucket gowa el cell 7ot el tuple fel bucket we law el bucket full e2semha
					if(x.getGridIndex().getByIndex(ins).getBucket()==null) {
						
						Bucket o = new Bucket();
						BucketInfo oi = new BucketInfo(tmp, tmp.getTuple().get(x.getGridIndex().getDimensionsInfo().get(0).getIndexInTable()), x.getGridIndex().getByIndex(ins));
						o.getBucket().add(oi);
						o.getCellsInTheBuckets().add(x.getGridIndex().getByIndex(ins));
						x.getGridIndex().getByIndex(ins).setBucket(o);
						
						
					}else {
						BucketInfo oi = new BucketInfo(tmp, tmp.getTuple().get(x.getGridIndex().getDimensionsInfo().get(0).getIndexInTable()), x.getGridIndex().getByIndex(ins));
						
						if(x.getGridIndex().getByIndex(ins).getBucket().getBucket().size()>=Bucket.readconfig()) {
							if(x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows()==0) {
								Bucket o = new Bucket();
								x.getGridIndex().getByIndex(ins).getBucket().setOverflow(o);
								x.getGridIndex().getByIndex(ins).getBucket().getOverflow().getBucket().add(oi);
								x.getGridIndex().getByIndex(ins).getBucket().setNoOfOverflows(x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows()+1);
								
							}else {
								Bucket tmp1 = x.getGridIndex().getByIndex(ins).getBucket();
								for(int i1=0; i1<x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows();i1++) {
									if(tmp1.getOverflow().getBucket().size()>=Bucket.readconfig()) {
										if(tmp1.getOverflow()!=null) {
										tmp1 = tmp1.getOverflow();
										}else {
											Bucket o = new Bucket();
											tmp1.setOverflow(o);
											o.getBucket().add(oi);
											x.getGridIndex().getByIndex(ins).getBucket().setNoOfOverflows(x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows()+1);
											
										}
									}else {
										tmp1.getOverflow().getBucket().add(oi);
									}
								}
							}
							
							
						}else {
							
						x.getGridIndex().getByIndex(ins).getBucket().getBucket().add(oi);
						}
						
					}
					//law mafeesh e3mel wahda we hotaha fel cell ely el index beta3ha fel ins array
					
				}
			

		
				
		

		//x.serialize();
		 x.updateminmaxPK();
		 x.serializePageSorting();
//		 x.serializeGridIndex();
		 
		 //x.getFulltable().get(x.getfirstpage()).serialize(x.getTablename(), x.getfirstpage());
//		 x.serialize(pageIndex);
//		 x.serialize(x.nextPageIndex(pageIndex));
	}
	
	public Boolean eligibleToInsert(Object value, String[] row) throws ParseException {
		if(value == null) {
//System.out.println(false);
			return false;
		
		
		}
		
		switch(row[2]) {
		   case "java.lang.String" :
		      // Statements
			   String s = (String) value;
			   String mins = row[5];
			   String maxs = row[6];
			   if( (s.compareToIgnoreCase(mins) >= 0)  
	            		&& (s.compareToIgnoreCase(maxs)) <= 0) {
//				   System.out.println(true);
				   return true;
			   }
			   else
				   break; // optional
		   
		   case "java.lang.Double" :
		      // Statements
			   Double d = (Double) value;
			   Double mind = Double.parseDouble(row[5]);
			   Double maxd = Double.parseDouble(row[6]);
			   
			   if( (d>=mind)  && (d<=maxd)) {
//				   System.out.println(true);
				   return true;
			   }
			   else
				   break; // optional
		      
		   case "java.util.Date" :
			      // Statements
			   Date da = (Date) value;
			   Date minda = null;
			   Date maxda = null;
			   if(row[5].contains("/")) {
				   String[] dateSplitmin = row[5].split("/");
				    minda = new Date(Integer.parseInt(dateSplitmin[2])-1900,Integer.parseInt(dateSplitmin[0])-1 , Integer.parseInt(dateSplitmin[1]));

			   }else if(row[5].contains("-")){
				   String[] dateSplitmin = row[5].split("-");
				    minda = new Date(Integer.parseInt(dateSplitmin[0])-1900,Integer.parseInt(dateSplitmin[1])-1 , Integer.parseInt(dateSplitmin[2]));

			   }
			   if(row[6].contains("/")) {
				   String[] dateSplitmax = row[6].split("/");
				    maxda = new Date(Integer.parseInt(dateSplitmax[2])-1900,Integer.parseInt(dateSplitmax[0])-1 , Integer.parseInt(dateSplitmax[1]));

			   }else if(row[6].contains("-")) {
				   String[] dateSplitmax = row[6].split("-");
				    maxda = new Date(Integer.parseInt(dateSplitmax[0])-1900,Integer.parseInt(dateSplitmax[1])-1 , Integer.parseInt(dateSplitmax[2]));

				   			   }
//			   System.out.println(row[6]);
//			   Date maxda = new SimpleDateFormat("yyyy-MM-dd").parse(row[6]);
			   
			   if( (da.compareTo(minda)>=0)  && (da.compareTo(maxda)<=0)) {
//				   System.out.println(true);
				   return true;
			   }
			   else
				   break; // optional
			      
		   case "java.lang.Integer" :
			      // Statements
			   int i = (int) value;
			   int mini = Integer.parseInt(row[5]);
			   int maxi = Integer.parseInt(row[6]);
			   
			   if( (i>=mini)  && (i<=maxi)) { 
//				   System.out.println(true);
				   return true;
			   }
			   else
				   break; // optional
		   default:
			   System.out.println("YOU SHOULD NOT GET HERE");
		 
		}
		
//		System.out.println(false);
		return false;
		
		
	}
	
		// Java implementation of recursive Binary Search
		// Returns index of x if it is present in arr[l..h], else return -1
	public static int pagebinarySearch(Table t, String x, String dataType) throws ParseException
	    {
		
	        int l = 0, r = t.getPageSorting().size() - 1;
	        while (l <= r) {
	        	int m=0;
	        	if(l==r) {
	        		 m = l;
	        	}
	        	else {
	             m = (l + r) / 2;
	        	}
	        	Boolean biggerThanMinPk = m == 0 || dataTypeCompare(t.getFulltable().get(t.getPageSorting().get(m)).getMinPK(), x, dataType)<=0;
	            Boolean lessThanNextMinPk = m == t.getPageSorting().size()-1 || dataTypeCompare(t.getFulltable().get(t.getPageSorting().get(m+1)).getMinPK(), x, dataType)>0;
	            
	            
	            if(biggerThanMinPk && lessThanNextMinPk)
	            	return t.getPageSorting().get(m);
	            else if(biggerThanMinPk)
	            	l = m +1;
	            else if(lessThanNextMinPk)
	            	r = m -1;
	            else
	            {
	            	String otherPk = t.getFulltable().get(t.getPageSorting().get(m)).getMinPK();
	            	System.out.print(otherPk.compareTo(x));
	            	System.out.println();
	            	System.out.println("YOU SHOULD NOT GET HEREE");
	            }
	            	
//	            
//	            if(m==r) return t.getPageSorting().get(m);
//	            else {
//	            // Check if x is present at mid
////	            System.out.println( t.getPageSorting().get(m));
//	            if (t.getFulltable().get(t.getPageSorting().get(m)).getMinPK().compareTo(x)<0 && (m == t.getPageSorting().size()-1 || t.getFulltable().get(t.getPageSorting().get(m+1)).getMinPK().compareTo(x)>0))
//	                return t.getPageSorting().get(m);
//	            
//	            
//	            // If x greater, ignore left half
//	            if (t.getFulltable().get(t.getPageSorting().get(m)).getMinPK().compareTo(x)<0)
//	                l = m + 1;
//	 
//	            // If x is smaller, ignore right half
//	            else
//	                r = m - 1;
//	        }
	        }
	     // if we reach here, then element was
	        // not present
        	System.out.println("YOU SHOULD NOT GET HERE TOO");
	        return -1;
	    }
	public static int rowbinarySearch(TablePage p, String x)
    {
	
        int l = 0, r = p.getPage().size() - 1;
        while (l <= r) {
            int m = l + (r - l) / 2;
 
            // Check if x is present at mid
            if (p.getPage().get(m).getPK().equals(x))
                return m;
 
            // If x greater, ignore left half
            if (p.getPage().get(m).getPK().compareTo(x)<0)
                l = m + 1;
 
            // If x is smaller, ignore right half
            else
                r = m - 1;
        }
     // if we reach here, then element was
        // not present
        return -1;
    }
	
	
	
	@SuppressWarnings("resource")
	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException, IOException, ParseException {
		// TODO Auto-generated method stub
		
		Table x = null;

		for(int i = 0; i < alltables.size(); i++) {
//			System.out.println(alltables.size());
			if(alltables.get(i).getTablename().equals(tableName)) {
//				System.out.println("ana");
				x = alltables.get(i);
			}	
		}
		if(x==null) {
			throw new DBAppException("Table not Found");
		}
		
	
		else
		{
			Boolean indexed = false;
			String dataType = "";
			String line1 = "";  		
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));  
			while ((line1 = br.readLine()) != null)   //returns a Boolean value  
			{  
//				System.out.println(3);
			String[] row = line1.split(",");
			if(row[0].equals(tableName)){
				
				if(columnNameValue.containsKey(row[1])) {
					if(row[4].equals("true")) {
						indexed = true;
					}
					
				}

				if( row[3].equals("true")) {
				dataType = row[2];
				}
			}
			
			}
			
			int pageIndex = pagebinarySearch(x,clusteringKeyValue, dataType);
			TablePage page = x.getFulltable().get(pageIndex);
//			boolean flag = false;
			System.out.println(pageIndex);
			// TODO fakes binary search for 250 elements...
			for(int i = 0; i < page.getPage().size(); i++)
			{
//				System.out.println(1);
				Tuple tmp = page.getPage().get(i);
				
				if(dataTypeCheck(tmp.getPK(), clusteringKeyValue, dataType))
				{
//					flag =true;
//					System.out.println(2);
					int i1 =0;
					String line = "";  		
					BufferedReader br1 = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));  
					while ((line = br1.readLine()) != null)   //returns a Boolean value  
					{  
//						System.out.println(3);
					String[] row = line.split(",");
					if(row[0].equals(tableName)) {
//						System.out.println(4);
//						if( (colNameValue.get(row[1]).toString().compareToIgnoreCase(row[5]) >= 0)  
//			            		&& ((colNameValue.get(row[1]).toString().compareToIgnoreCase(row[6])) <= 0) )  {

						if(eligibleToInsert(columnNameValue.get(row[1]), row) && row[3].equals("false"))	{
//							System.out.println(5);
							
//							if(row[3]=="TRUE"&&columnNameValue.get(row[1])==null) {
//								throw  new DBAppException("cannot insert");
//							}
						tmp.getTuple().remove(i1);
						tmp.getTuple().add( i1, columnNameValue.get(row[1]).toString());
//						System.out.println( colNameValue.get(row[1]).toString());
						
						columnNameValue.remove(row[1]);
						
						
						}
						i1++;
					}
					}
//					System.out.println(columnNameValue.size());
					if(columnNameValue.size()!=0) {
						throw new DBAppException("");
					}
					
					page.getPage().set(i, tmp);
					page.updateminmaxPK();
					
					x.serialize(pageIndex);
//					x.serializeGridIndex();
					return;
				}
				
				removeFromIndex(x, tmp);
				
				
				int[] ins = new int[x.getGridIndex().getDimensionsInfo().size()];
				
				for(int k=0 ;k<x.getGridIndex().getDimensionsInfo().size();k++) {
					
					BigDecimal m  = turnIntoBigDecimal(x.getGridIndex().getDimensionsInfo().get(k).getDataType(), tmp.getTuple().get(x.getGridIndex().getDimensionsInfo().get(k).getIndexInTable()));
					
					for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(k).getRange().length;q++) {
						if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(k).getRange()[10])==0) {
							ins[k]=9;
							break;
						}
						
						if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(k).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(k).getRange()[q+1])<0) {
							ins[k]=	q;
							break;
							}
					}
					
					
				}
				
				// law feeh bucket gowa el cell 7ot el tuple fel bucket we law el bucket full e2semha
				if(x.getGridIndex().getByIndex(ins).getBucket()==null) {
					
					Bucket o = new Bucket();
					BucketInfo oi = new BucketInfo(tmp, tmp.getTuple().get(x.getGridIndex().getDimensionsInfo().get(0).getIndexInTable()), x.getGridIndex().getByIndex(ins));
					o.getBucket().add(oi);
					o.getCellsInTheBuckets().add(x.getGridIndex().getByIndex(ins));
					x.getGridIndex().getByIndex(ins).setBucket(o);
					
					
				}else {
					BucketInfo oi = new BucketInfo(tmp, tmp.getTuple().get(x.getGridIndex().getDimensionsInfo().get(0).getIndexInTable()), x.getGridIndex().getByIndex(ins));
					
					if(x.getGridIndex().getByIndex(ins).getBucket().getBucket().size()>=Bucket.readconfig()) {
						if(x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows()==0) {
							Bucket o = new Bucket();
							x.getGridIndex().getByIndex(ins).getBucket().setOverflow(o);
							x.getGridIndex().getByIndex(ins).getBucket().getOverflow().getBucket().add(oi);
							x.getGridIndex().getByIndex(ins).getBucket().setNoOfOverflows(x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows()+1);
							
						}else {
							Bucket tmp1 = x.getGridIndex().getByIndex(ins).getBucket();
							for(int i1=0; i1<x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows();i1++) {
								if(tmp1.getOverflow().getBucket().size()>=Bucket.readconfig()) {
									if(tmp1.getOverflow()!=null) {
									tmp1 = tmp1.getOverflow();
									}else {
										Bucket o = new Bucket();
										tmp1.setOverflow(o);
										o.getBucket().add(oi);
										x.getGridIndex().getByIndex(ins).getBucket().setNoOfOverflows(x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows()+1);
										
									}
								}else {
									tmp1.getOverflow().getBucket().add(oi);
								}
							}
						}
						
						
					}else {
						
					x.getGridIndex().getByIndex(ins).getBucket().getBucket().add(oi);
					}
					
				}
				
//				x.serializeGridIndex();
				//law mafeesh e3mel wahda we hotaha fel cell ely el index beta3ha fel ins array
				
			
		
				// insert againg into grid index
			}
			throw new DBAppException("row not Found");
//			if(!flag) {
//				throw new DBAppException("row not Found");
//			}
			
//			throw new DBAppException("row not Found");
		}
		
//		
//		int pageIndex = pagebinarySearch(x, clusteringKeyValue);
//		int tupleIndex = rowbinarySearch(x.getFulltable().get(pageIndex), clusteringKeyValue);
//		System.out.println(pageIndex);
//		System.out.println(tupleIndex);
//		if(pageIndex==-1 || tupleIndex==-1) {
//			throw new DBAppException("row not Found");
//		}
//		String line = "";  		
//		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));  
//		while ((line = br.readLine()) != null)   //returns a Boolean value  
//		{  
//			String[] row = line.split(",");
//			if(row[0].equals(tableName)) {
//				int i =1;
//				if(row[3].equals("false")) {
//					if(eligibleToInsert(columnNameValue.get(row[1]), row)) {
//					x.getFulltable().get(pageIndex).getPage().get(tupleIndex).getTuple().remove(i);
//					x.getFulltable().get(pageIndex).getPage().get(tupleIndex).getTuple().add(i, (String) columnNameValue.get(row[1]));
//					i++;
//					}
//				}
//
//		
//		}  
//		
//		}
////		x.sort();
//		x.serialize(pageIndex);
		
//		Enumeration<String> enumeration = columnNameValue.keys();
//		 
//        // iterate using enumeration object
//        while(enumeration.hasMoreElements()) {
//            String key = enumeration.nextElement();
//          x.getFulltable()
//           
//            
//            
//            
//            
//            
//            if (key == clusteringKeyValue) {
//            	// Insert
//            	break;
//            	
//            	
//            }
//            
//        }
		
	}
	
	public boolean dataTypeCheck(String value, String key, String dataType) throws ParseException {
		
		if(value == null) {
			//System.out.println(false);
						return false;
							}
					
					switch(dataType) {
					   case "java.lang.String" :
					      // Statements
						   String s = (String) value;
						   if(value.equals(key))
							   return true;
						   else
							   break; // optional
					   
					   case "java.lang.Double" :
					      // Statements
						   Double d = Double.parseDouble(value);
						   Double d1 = Double.parseDouble(key);
//							   System.out.println(true);
						   if(d.compareTo(d1)==0)
							   return true;
						   else						
							   break; // optional
					      
					   case "java.util.Date" :
						      // Statements

				          DateFormat srcDf = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu");
				          Date da = srcDf.parse(value);
				          
//						   Date da = new SimpleDateFormat("yyyy-MM-dd").parse(value);
						   Date da1 = new SimpleDateFormat("yyyy-MM-dd").parse(key);
						   
						
						   if(da.equals(da1))	
						   {
							   System.out.println(da);
						       System.out.println(da1);
							   System.out.println(true);
							   return true;
							   
						   }
						   else
							   break; // optional
						      
					   case "java.lang.Integer" :
						      // Statements
						   int i = Integer.parseInt(value);
						   int i1 = Integer.parseInt(key);
				
//							   System.out.println(true);
						   if(i==i1)
							   return true;
						   else
							   break; // optional
					 
					}
//					System.out.println(false);
					return false;
					
	
	}
	

	public static int dataTypeCompare(String first, String second, String dataType) throws ParseException {
		
					switch(dataType) {
					   case "java.lang.String" :
						  return ((String)first).compareTo((String)second);
					   case "java.lang.Double" :
						   Double d = Double.parseDouble(first);
						   Double d1 = Double.parseDouble(second);

						   return d.compareTo(d1);
					      
					   case "java.util.Date" :
						      // Statements

				          DateFormat srcDf = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu");
				          Date da= null;
				          Date da1 = null;
				          
				          if(first.length()>10)
				         da = srcDf.parse(first);
				          else
				        	 da = new SimpleDateFormat("yyyy-MM-dd").parse(first); 
				          
				        	  if(second.length()>10)
						  da1= srcDf.parse(second);
				        	  else
				        		  da1= new SimpleDateFormat("yyyy-MM-dd").parse(second);   
						   
						   return da.compareTo(da1);
					   case "java.lang.Integer" :
						      // Statements
						   Integer i = Integer.parseInt(first);
						   Integer i1 = Integer.parseInt(second);

						   return i.compareTo(i1);
						default:
							System.out.println("YOU SHOULD NOT BE HERE");
							return 0;
					 
					}
					
	
	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException, IOException {
		// TODO Auto-generated method stub
		
		Table x = null;
		for(int i = 0; i < alltables.size(); i++) {
//			System.out.println(alltables.size());
			if(alltables.get(i).getTablename().equals(tableName)) {
//				System.out.println(alltables.size());
				x = alltables.get(i);
				
			}
		}
		if(x==null) {
			throw new DBAppException("Table not Found");
		}
		
		
		
		
		
		
		if(!x.getFulltable().isEmpty()) {
			ArrayList<String> colName = new ArrayList<String>();
			ArrayList<Integer> colIndex = new ArrayList<Integer>();
			int i1 = 0;
			String line = "";  
			Boolean indexed = false;
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));  
			while ((line = br.readLine()) != null)   //returns a Boolean value  
			{  
			
			String[] row = line.split(",");
			if(row[0].equals(tableName)) {
				if(row[4].equals("true")) {
					indexed =true;
					
				}
				if(columnNameValue.get(row[1])!=null){
				
				colName.add(row[1]);
				colIndex.add(i1);
				
				}
				i1++;
			}
			
			}
			
			for (int i =0;i<x.getFulltable().size();i++) {
				
								
				for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
					
					Boolean flag = false;
					
					for(int k = 0;k<colName.size();k++) {
						String value = columnNameValue.get(colName.get(k)).toString();
						int index = colIndex.get(k);
						
						if(value.equals(x.getFulltable().get(i).getPage().get(j).getTuple().get(index))){
							flag= true;
						}else {
							flag = false;
							break;
						}
						
					}
					
					if(flag) {
						if(indexed) {
						removeFromIndex(x, x.getFulltable().get(i).getPage().get(j));
						}
						x.getFulltable().get(i).getPage().remove(j);
						
					}
					
				}
				
				if(x.getFulltable().get(i).getPage().isEmpty()) {
					x.getPageSorting().remove(x.getFulltable().get(i));
					x.getFulltable().remove(i);
					
				}
				
			}
			
			
		}
		
//		
		 x.serializePageSorting();
//		 x.serializeGridIndex();
		
	}

	private void removeFromIndex(Table x, Tuple tmp) {
		// TODO Auto-generated method stub
		
		if(x.getGridIndex()!=null) {
			
			
			int[] ins = new int[x.getGridIndex().getDimensionsInfo().size()];
			
			for(int k=0 ;k<x.getGridIndex().getDimensionsInfo().size();k++) {
				
				BigDecimal m = null;
				try {
					m = turnIntoBigDecimal(x.getGridIndex().getDimensionsInfo().get(k).getDataType(), tmp.getTuple().get(x.getGridIndex().getDimensionsInfo().get(k).getIndexInTable()));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(k).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(k).getRange()[10])==0) {
						ins[k]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(k).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(k).getRange()[q+1])<0) {
						ins[k]=	q;
						break;
						}
				}
				
				
			}
			
			Bucket q = x.getGridIndex().getByIndex(ins).getBucket();
			Boolean flag = false;
			
			for(int i=0;i<q.getBucket().size();i++) {
				if(q.getBucket().get(i).getTuple().equals(tmp)) {
					q.getBucket().remove(i);
					break;
				}
				if(i==(q.getBucket().size()-1)&& q.getOverflow()!=null) {
					q = q.getOverflow();
					i=0;					
				}
				
				
			}
			
			
			
			// law feeh bucket gowa el cell 7ot el tuple fel bucket we law el bucket full e2semha
//			if(x.getGridIndex().getByIndex(ins).getBucket()==null) {
//				
//				Bucket o = new Bucket();
//				BucketInfo oi = new BucketInfo(tmp, tmp.getTuple().get(x.getGridIndex().getDimensionsInfo().get(0).getIndexInTable()), x.getGridIndex().getByIndex(ins));
//				o.getBucket().add(oi);
//				o.getCellsInTheBuckets().add(x.getGridIndex().getByIndex(ins));
//				x.getGridIndex().getByIndex(ins).setBucket(o);
//				
//				
//			}else {
//				BucketInfo oi = new BucketInfo(tmp, tmp.getTuple().get(x.getGridIndex().getDimensionsInfo().get(0).getIndexInTable()), x.getGridIndex().getByIndex(ins));
//				
//				if(x.getGridIndex().getByIndex(ins).getBucket().getBucket().size()>=Bucket.readconfig()) {
//					if(x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows()==0) {
//						Bucket o = new Bucket();
//						x.getGridIndex().getByIndex(ins).getBucket().setOverflow(o);
//						x.getGridIndex().getByIndex(ins).getBucket().getOverflow().getBucket().add(oi);
//						x.getGridIndex().getByIndex(ins).getBucket().setNoOfOverflows(x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows()+1);
//						
//					}else {
//						Bucket tmp1 = x.getGridIndex().getByIndex(ins).getBucket();
//						for(int i1=0; i1<x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows();i1++) {
//							if(tmp1.getOverflow().getBucket().size()>=Bucket.readconfig()) {
//								if(tmp1.getOverflow()!=null) {
//								tmp1 = tmp1.getOverflow();
//								}else {
//									Bucket o = new Bucket();
//									tmp1.setOverflow(o);
//									o.getBucket().add(oi);
//									x.getGridIndex().getByIndex(ins).getBucket().setNoOfOverflows(x.getGridIndex().getByIndex(ins).getBucket().getNoOfOverflows()+1);
//									
//								}
//							}else {
//								tmp1.getOverflow().getBucket().add(oi);
//							}
//						}
//					}
//					
//					
//				}else {
//					
//				x.getGridIndex().getByIndex(ins).getBucket().getBucket().add(oi);
//				}
//				
//			}
		}
		
	}

	@Override
	 //start
	public Iterator<Tuple> selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {

		Vector<Vector<Tuple>> res = new Vector<Vector<Tuple>>();

		for(int f = 0; f< sqlTerms.length ; f++) {
		try {
		res.add(new Vector<Tuple>());
		solveSQL(sqlTerms[f], res.get(f), alltables);
		} catch (DBAppException | ParseException | IOException e) {
		e.printStackTrace();
		}
		}

		for(int j= 0; j < arrayOperators.length && res.size()>1 ;j++) {

		if(arrayOperators[j].equals("OR")) {

		performOR( res.get(0) , res.get(1),  res);

		}
		if(arrayOperators[j].equals("AND")) {

		performAND( res.get(0) , res.get(1),  res);

		}
		if(arrayOperators[j].equals("XOR")) {

		performXOR( res.get(0) , res.get(1),  res);

		}
		}
		if(res.size() == 1) {

		Iterator<Tuple> it = res.get(0).iterator();;
		return it;

		}
		else {
		throw new DBAppException("invalid input");
		}



		}



	public static void performOR( Vector<Tuple> op1 , Vector<Tuple> op2, Vector<Vector<Tuple>> res) {
	Vector<Tuple> tmp = new Vector<Tuple>();

	Set<Tuple> set = new HashSet<Tuple>();
	set.addAll(op1);
	set.addAll(op2);
	tmp.addAll(set);

	res.remove(op2);
	res.get(res.indexOf(op1)).clear();
	res.get(res.indexOf(op1)).addAll(tmp);

	}

	public static void performAND( Vector<Tuple> op1 , Vector<Tuple> op2, Vector<Vector<Tuple>> res) {
	Vector<Tuple> tmp = new Vector<Tuple>();

	for(int i = 0 ; i< op1.size() ; i++) {
	for(int j = 0 ; j< op2.size() ; j++) {
	if(op1.get(i).equals(op2.get(j))) {
	tmp.add(op1.get(i));
	}

	}

	}
	res.remove(op2);
	res.get(res.indexOf(op1)).clear();
	res.get(res.indexOf(op1)).addAll(tmp);

	}


	public static void performXOR( Vector<Tuple> op1 , Vector<Tuple> op2, Vector<Vector<Tuple>> res) {
	Vector<Tuple> tmp = new Vector<Tuple>();
	tmp.addAll(op1);

	for(int i = 0 ; i< tmp.size() ; i++) {
	for(int j = 0 ; j< op2.size() ; j++) {
	if(tmp.get(i).equals(op2.get(j))) {
	tmp.remove(i);
	op2.remove(j);
	}
	}
	}
	tmp.addAll(op2);

	res.remove(op2);
	res.get(res.indexOf(op1)).clear();
	res.get(res.indexOf(op1)).addAll(tmp);

	}


	public static void solveSQL(SQLTerm sqlTerm, Vector<Tuple> res, ArrayList<Table> alltables)throws DBAppException, ParseException, IOException{

	//get table
	Table x = null;
	for(int i = 0; i < alltables.size(); i++) {
	if(alltables.get(i).getTablename().equals(sqlTerm._strTableName)) {
	x = alltables.get(i);
	}
	}
	if(x==null) {
	throw new DBAppException("Table not Found");
	}

	boolean indexed = false;
	if(x.getGridIndex()!=null) {
	if( x.getGridIndex().getDimensionsInfo().get(0).getName().equals(sqlTerm._strColumnName)) {
		indexed = true;
	}
	}
	//get column index

	String line = "";  
	BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));  
	int colindex = -1;
	String coldatatype= null;
	int g = 0;
	while ((line = br.readLine()) != null)   //returns a Boolean value  
	{  

	String[] row = line.split(",");
	
	if(row[0].equals(x.getTablename())) {
	if (row[1].equals(sqlTerm._strColumnName)) {
	colindex = g;
	coldatatype = row[2];
	break;
	}
	else {
	g++;
	}

	}
	}
	
	if(colindex == -1) {
		br.close();
		throw new DBAppException("column not found");
		
	}

	switch(coldatatype) {

	  case "java.lang.String" :
	  switch(sqlTerm._strOperator){
	case(">"):  
		if(!indexed) {
	for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex).compareTo((String)sqlTerm._objValue) > 0 ) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	
	}
	}
		else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckString(res,index, x ,(String)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				index[0] = index[0] +1;
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckMoreThan(x, res, index);				
				
			
		}; break;

	   
	   
	   case(">="):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex).compareTo((String)sqlTerm._objValue) >= 0 ) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}
		   }
			else{
					int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
					BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
					
					for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
						if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
							index[0]=9;
							break;
						}
						
						if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
							index[0]=	q;
							break;
							}
						}
					
					for(int i =1;i<index.length;i++) {
						index[i]=0;
					}
					
					res = RecursionBuckString(res,index, x ,(String)sqlTerm._objValue, colindex, sqlTerm._strOperator );
					index[0] = index[0] +1;
					for(int i =1;i<index.length;i++) {
						index[i]=0;
					}
	   				res = RecursionBuckMoreThan(x, res, index);	
			}
	   
	   break;

	   
	   
	   case("<"):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex).compareTo((String)sqlTerm._objValue) < 0 ) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}
		   }
			else{
					int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
					BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
					
					for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
						if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
							index[0]=9;
							break;
						}
						
						if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
							index[0]=	q;
							break;
							}
						}
					
					for(int i =1;i<index.length;i++) {
						index[i]=0;
					}
					
					res = RecursionBuckString(res,index, x ,(String)sqlTerm._objValue, colindex, sqlTerm._strOperator );
					
					int end = index[0];
					for(int i =0;i<index.length;i++) {
						index[i]=0;
					}
					res = RecursionBuckLessThan(x, res, index, end);	
			}
	  break;

	   
	   
	   
	   case("<="):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex).compareTo((String)sqlTerm._objValue) <= 0 ) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}
		   }
			else{
					int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
					BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
					
					for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
						if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
							index[0]=9;
							break;
						}
						
						if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
							index[0]=	q;
							break;
							}
						}
					
					for(int i =1;i<index.length;i++) {
						index[i]=0;
					}
					
					res = RecursionBuckString(res,index, x ,(String)sqlTerm._objValue, colindex, sqlTerm._strOperator );
			   int end = index[0];
				for(int i =0;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckLessThan(x, res, index, end);	
			}
	break;

	   
	   case("!="): 
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(!(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex).equals(sqlTerm._objValue) )) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	    }
		   }
			else{
					int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
					BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
					
					for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
						if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
							index[0]=9;
							break;
						}
						
						if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
							index[0]=	q;
							break;
							}
						}
					
					for(int i =1;i<index.length;i++) {
						index[i]=0;
					}
					
					res = RecursionBuckString(res,index, x ,(String)sqlTerm._objValue, colindex, sqlTerm._strOperator );
			
					int end = index[0];
					for(int i =0;i<index.length;i++) {
						index[i]=0;
					}
					res = RecursionBuckLessThan(x, res, index, end);
					
					index[0]=end+1;
					for(int i =1;i<index.length;i++) {
						index[i]=0;
					}
					res = RecursionBuckMoreThan(x, res, index);

					
			}
	  break;
	   
	case("="):
		if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex).equals(sqlTerm._objValue) ) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}
		   }
					else{
							int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
							BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
							
							for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
								if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
									index[0]=9;
									break;
								}
								
								if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
									index[0]=	q;
									break;
									}
								}
							
							for(int i =1;i<index.length;i++) {
								index[i]=0;
							}
							
							res = RecursionBuckString(res,index, x ,(String)sqlTerm._objValue, colindex, sqlTerm._strOperator );}
			 
	    break;
	
	default: throw new DBAppException();
	  }
	  
	  break;

	 
	 
	  case "java.lang.Double" :  
	  switch(sqlTerm._strOperator){
	  case(">"):  
		  if(!indexed) {
	for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(Double.parseDouble(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex)) > (Double)(sqlTerm._objValue)) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}
		  }
			else{
					int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
					BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
					
					for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
						if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
							index[0]=9;
							break;
						}
						
						if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
							index[0]=	q;
							break;
							}
						}
					
					for(int i =1;i<index.length;i++) {
						index[i]=0;
					}
					
					res = RecursionBuckDouble(res,index, x ,(Double)sqlTerm._objValue, colindex, sqlTerm._strOperator );
					index[0] = index[0] +1;
					for(int i =1;i<index.length;i++) {
						index[i]=0;
					}
					res = RecursionBuckMoreThan(x, res, index);		
					
					
					
				
			};
			break;

	   
	   
	   case(">="):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(Double.parseDouble(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex)) >= (Double)(sqlTerm._objValue)) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	} }
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckDouble(res,index, x ,(Double)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				index[0] = index[0] +1;
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckMoreThan(x, res, index);	
				
				
			
		} break;

	   
	   case("<"):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(Double.parseDouble(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex)) < (Double)(sqlTerm._objValue)) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckDouble(res,index, x ,(Double)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				int end = index[0];
				for(int i =0;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckLessThan(x, res, index, end);
				
			} break;

	   
	   case("<="):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(Double.parseDouble(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex)) <= (Double)(sqlTerm._objValue)) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckDouble(res,index, x ,(Double)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				int end = index[0];
				for(int i =0;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckLessThan(x, res, index, end);
				
			} break;

	   
	   case("!="):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(Double.parseDouble(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex)) != (Double)(sqlTerm._objValue)) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckDouble(res,index, x ,(Double)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				int end = index[0];
				for(int i =0;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckLessThan(x, res, index, end);
				
				index[0]=end+1;
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckMoreThan(x, res, index);
				
			} break;

	   case("="):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(Double.parseDouble(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex)) == (Double)(sqlTerm._objValue)) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	    }}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckDouble(res,index, x ,(Double)sqlTerm._objValue, colindex, sqlTerm._strOperator );

				
			} break;
		default: throw new DBAppException();

	  }
	  break;
	 
	  case "java.lang.Integer" :  
	  switch(sqlTerm._strOperator){
	  case(">"): 
		  if(!indexed) {
	for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(Integer.parseInt(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex)) > (Integer)(sqlTerm._objValue)) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckInteger(res,index, x ,(Integer)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				index[0] = index[0] +1;
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckMoreThan(x, res, index);	
				
			} break;

	   
	   case(">="):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(Integer.parseInt(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex)) >= (Integer)(sqlTerm._objValue)) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckInteger(res,index, x ,(Integer)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				index[0] = index[0] +1;
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckMoreThan(x, res, index);		
				
			} break;

	   
	   case("<"):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(Integer.parseInt(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex)) < (Integer)(sqlTerm._objValue)) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckInteger(res,index, x ,(Integer)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				
				int end = index[0];
				for(int i =0;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckLessThan(x, res, index, end);
				
			} break;

	   
	   case("<="):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(Integer.parseInt(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex)) <= (Integer)(sqlTerm._objValue)) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckInteger(res,index, x ,(Integer)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				int end = index[0];
				for(int i =0;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckLessThan(x, res, index, end);
			} break;

	   
	   case("!="):  
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(Integer.parseInt(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex)) != (Integer)(sqlTerm._objValue)) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckInteger(res,index, x ,(Integer)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				
				int end = index[0];
				for(int i =0;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckLessThan(x, res, index, end);
				
				index[0]=end+1;
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckMoreThan(x, res, index);
				
			} break;

	   case("="):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	if(Integer.parseInt(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex)) == (Integer)(sqlTerm._objValue)) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	    }}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckInteger(res,index, x ,(Integer)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				
				
			} break;
		default: throw new DBAppException();

	  }
	  break;
	 
	 
	 
	  case "java.util.Date" :  
	 
	  switch(sqlTerm._strOperator){
	case(">"):
		   if(!indexed) {

	for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
//		DateFormat srcDf = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu");
	Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex));
	if(da.compareTo((Date)sqlTerm._objValue) > 0 ) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}
		   }
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckDate(res,index, x ,(Date)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				index[0] = index[0] +1;
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckMoreThan(x, res, index);	

				
			} break;

	   
	   
	   case(">="):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex));
	if(da.compareTo((Date)sqlTerm._objValue) >= 0 ) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	} }
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckDate(res,index, x ,(Date)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				index[0] = index[0] +1;
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckMoreThan(x, res, index);	;	

			} break;

	   
	   
	   case("<"):
		   if(!indexed) {

	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex));
	if(da.compareTo((Date)sqlTerm._objValue) < 0 ) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckDate(res,index, x ,(Date)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				int end = index[0];
				for(int i =0;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckLessThan(x, res, index, end);
				
			} break;

	   
	   
	   
	   case("<="):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex));
	if(da.compareTo((Date)sqlTerm._objValue) <= 0 ) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckDate(res,index, x ,(Date)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				int end = index[0];
				for(int i =0;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckLessThan(x, res, index, end);
				
			} break;

	   
	   
	   case("!="): 
		   if(!indexed) {

	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex));
	if(!(da.equals(sqlTerm._objValue) )) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}}
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckDate(res,index, x ,(Date)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				
				int end = index[0];
				for(int i =0;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckLessThan(x, res, index, end);
				
				index[0]=end+1;
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				res = RecursionBuckMoreThan(x, res, index);
			}  break;
	 
	   case("="):
		   if(!indexed) {
	    for (int i =0;i<x.getFulltable().size();i++) {
	for(int j=0; j<x.getFulltable().get(i).getPage().size();j++) {
	Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(x.getFulltable().get(i).getPage().get(j).getTuple().get(colindex));
	if(da.equals(sqlTerm._objValue) ) {
	res.add(x.getFulltable().get(i).getPage().get(j));
	}
	}
	}
		   }
			else{
				int index[] = new int[x.getGridIndex().getDimensionsInfo().size()];
				BigDecimal m  = turnIntoBigDecimal(coldatatype, (String)sqlTerm._objValue);
				
				for(int q=0;q<x.getGridIndex().getDimensionsInfo().get(0).getRange().length;q++) {
					if(m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[10])==0) {
						index[0]=9;
						break;
					}
					
					if(m.compareTo((x.getGridIndex().getDimensionsInfo().get(0).getRange()[q]))>=0&& m.compareTo(x.getGridIndex().getDimensionsInfo().get(0).getRange()[q+1])<0) {
						index[0]=	q;
						break;
						}
					}
				
				for(int i =1;i<index.length;i++) {
					index[i]=0;
				}
				
				res = RecursionBuckDate(res,index, x ,(Date)sqlTerm._objValue, colindex, sqlTerm._strOperator );
				
				
			}; break;
		default: throw new DBAppException();

	  }
	  break;
	  
		default: throw new DBAppException();

	 
	}



	}

	private static Vector<Tuple> RecursionBuckMoreThan(Table x, Vector<Tuple> res, int[] index) {
		  Boolean flag=false;
			for(int i=0;i<index.length;i++) {
				if(index[i]!=9) {
					flag= false;
					break;
				}else
				{
					flag =true;
				}
			}
			
			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			
			
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				
					res.add(tmp);
				
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
			if(!flag) {
				String it="";
				for(int j=0;j<index.length;j++) {
				 it = it+index[j];
				}
				int in =Integer.parseInt(it);
				in++;
				
				for(int i=index.length-1;i>=0;i++) {
					if(in!=0) {
						index[i] = in%10;
						in=in/10;
					}else {
						index[i]=0;
					}
//					if(index[i]!=9) {
//						index[i]=index[i]+1;
//						break;
//					}
				}
				res = RecursionBuckMoreThan(x, res, index);
//				
			}
			
		
		return res;
	}
	private static Vector<Tuple> RecursionBuckLessThan(Table x, Vector<Tuple> res, int[] index, int end) {
		  Boolean flag=false;
		  if(index[0]==end) {
				return res;
			}
//			for(int i=0;i<index.length;i++) {
//				
//				if(index[i]!=9) {
//					flag= false;
//					break;
//				}else
//				{
//					flag =true;
//				}
//			}
			
			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			
			
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				
					res.add(tmp);
				
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
			if(!flag) {
				String it="";
				for(int j=0;j<index.length;j++) {
				 it = it+index[j];
				}
				int in =Integer.parseInt(it);
				in++;
				
				for(int i=index.length-1;i>=0;i++) {
					if(in!=0) {
						index[i] = in%10;
						in=in/10;
					}else {
						index[i]=0;
					}
//					if(index[i]!=9) {
//						index[i]=index[i]+1;
//						break;
//					}
				}
				res = RecursionBuckLessThan(x, res,index, end);
//				
			}
			
		
		return res;
	}


	private static Vector<Tuple> RecursionBuckString(Vector<Tuple> res, int[] index, Table x, String _objValue, int colindex, String _strOperator) {
		// TODO Auto-generated method stub
		  Boolean flag=false;
		for(int i=1;i<index.length;i++) {
			if(index[i]!=9) {
				flag= false;
				break;
			}else
			{
				flag =true;
			}
		}
		
		
		Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
		switch(_strOperator){
		case(">"):
			
//		Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
		for(int i=0;i<bucket.getBucket().size();i++) {
			
			Tuple tmp = bucket.getBucket().get(i).getTuple(); 
			if(tmp.getTuple().get(colindex).compareTo(_objValue) >0) {
				res.add(tmp);
			}
			if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
				bucket = bucket.getOverflow();
				i=0;
			}
			
		}
		
//		
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckString(res,index, x, _objValue, colindex, _strOperator );
//			
		}
		
		break;
		
		
		
		case("<"):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(tmp.getTuple().get(colindex).compareTo(_objValue) <0) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckString(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
		case(">="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(tmp.getTuple().get(colindex).compareTo(_objValue) >=0) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckString(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			break;
			
			
			
		case("<="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(tmp.getTuple().get(colindex).compareTo(_objValue) <=0) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckString(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
			
		case("!="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(!(tmp.getTuple().get(colindex).equals(_objValue))) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckString(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
		case("="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(tmp.getTuple().get(colindex).equals(_objValue)) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckString(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
		}
		
		return res;
	}
	
	private static Vector<Tuple> RecursionBuckDouble(Vector<Tuple> res, int[] index, Table x, Double _objValue, int colindex, String _strOperator) {
		// TODO Auto-generated method stub
		  Boolean flag=false;
		for(int i=1;i<index.length;i++) {
			if(index[i]!=9) {
				flag= false;
				break;
			}else
			{
				flag =true;
			}
		}
		Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
		switch(_strOperator){
		case(">"):
			
//		Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
		for(int i=0;i<bucket.getBucket().size();i++) {
			
			Tuple tmp = bucket.getBucket().get(i).getTuple(); 
			if(Double.parseDouble(tmp.getTuple().get(colindex)) > _objValue) {
				res.add(tmp);
			}
			if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
				bucket = bucket.getOverflow();
				i=0;
			}
			
		}
		
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckDouble(res,index, x, _objValue, colindex, _strOperator );
//			
		}
		
		break;
		
		
		
		case("<"):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(Double.parseDouble(tmp.getTuple().get(colindex)) < _objValue) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckDouble(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
		case(">="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(Double.parseDouble(tmp.getTuple().get(colindex)) >= _objValue) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckDouble(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
			
			
		case("<="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(Double.parseDouble(tmp.getTuple().get(colindex)) <= _objValue) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckDouble(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
			
		case("!="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(Double.parseDouble(tmp.getTuple().get(colindex)) != _objValue) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckDouble(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
		case("="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(Double.parseDouble(tmp.getTuple().get(colindex)) == _objValue) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckDouble(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
		}
		
		return res;
	}

	private static Vector<Tuple> RecursionBuckInteger(Vector<Tuple> res, int[] index, Table x, Integer _objValue, int colindex, String _strOperator) {
		// TODO Auto-generated method stub
		  Boolean flag=false;
		for(int i=1;i<index.length;i++) {
			if(index[i]!=9) {
				flag= false;
				break;
			}else
			{
				flag =true;
			}
		}
		Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
		switch(_strOperator){
		case(">"):
			
//		Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
		for(int i=0;i<bucket.getBucket().size();i++) {
			
			Tuple tmp = bucket.getBucket().get(i).getTuple(); 
			if(Integer.parseInt(tmp.getTuple().get(colindex)) > _objValue) {
				res.add(tmp);
			}
			if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
				bucket = bucket.getOverflow();
				i=0;
			}
			
		}
		
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckInteger(res,index, x, _objValue, colindex, _strOperator );
//			
		}
		
		break;
		
		
		
		case("<"):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(Integer.parseInt(tmp.getTuple().get(colindex)) < _objValue) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckInteger(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
		case(">="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(Integer.parseInt(tmp.getTuple().get(colindex)) >= _objValue) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckInteger(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
			
			
		case("<="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(Integer.parseInt(tmp.getTuple().get(colindex)) <= _objValue) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckInteger(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
			
		case("!="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(Integer.parseInt(tmp.getTuple().get(colindex)) != _objValue) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckInteger(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
		case("="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				if(Integer.parseInt(tmp.getTuple().get(colindex)) == _objValue) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckInteger(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
		}
		
		return res;
	}
	
//	Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(
	private static Vector<Tuple> RecursionBuckDate(Vector<Tuple> res, int[] index, Table x, Date _objValue, int colindex, String _strOperator) throws ParseException {
		// TODO Auto-generated method stub
		  Boolean flag=false;
		for(int i=1;i<index.length;i++) {
			if(index[i]!=9) {
				flag= false;
				break;
			}else
			{
				flag =true;
			}
		}
		Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
		switch(_strOperator){
		case(">"):
			
//		Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
		for(int i=0;i<bucket.getBucket().size();i++) {
			
			Tuple tmp = bucket.getBucket().get(i).getTuple(); 
			Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(tmp.getTuple().get(colindex));
			if(da.compareTo(_objValue) > 0) {
				res.add(tmp);
			}
			if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
				bucket = bucket.getOverflow();
				i=0;
			}
			
		}
		
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckDate(res,index, x, _objValue, colindex, _strOperator );
//			
		}
		
		break;
		
		
		
		case("<"):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(tmp.getTuple().get(colindex));

				if(da.compareTo(_objValue) < 0) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckDate(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
		case(">="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(tmp.getTuple().get(colindex));

				if(da.compareTo(_objValue) >= 0) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckDate(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
			
			
		case("<="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(tmp.getTuple().get(colindex));

				if(da.compareTo(_objValue) <= 0) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckDate(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
			
		case("!="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(tmp.getTuple().get(colindex));

				if(da.compareTo(_objValue) != 0) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckDate(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
		case("="):
			
			
//			Bucket bucket = x.getGridIndex().getByIndex(index).getBucket();
			for(int i=0;i<bucket.getBucket().size();i++) {
				
				Tuple tmp = bucket.getBucket().get(i).getTuple(); 
				Date da = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu").parse(tmp.getTuple().get(colindex));

				if(da.equals(_objValue)) {
					res.add(tmp);
				}
				if(i==bucket.getBucket().size()-1&& bucket.getOverflow()!=null) {
					bucket = bucket.getOverflow();
					i=0;
				}
				
			}
			
		if(!flag) {
			String it="";
			for(int j=0;j<index.length;j++) {
			 it = it+index[j];
			}
			int in =Integer.parseInt(it);
			in++;
			
			for(int i=index.length-1;i>0;i++) {
				if(in!=0) {
					index[i] = in%10;
					in=in/10;
				}else {
					index[i]=0;
				}
//				if(index[i]!=9) {
//					index[i]=index[i]+1;
//					break;
//				}
			}
			res = RecursionBuckDate(res,index, x, _objValue, colindex, _strOperator );
//			
		}
			
			break;
			
		}
		
		return res;
	}

	//end




}
