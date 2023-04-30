import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Vector;

public class GridIndex implements Serializable {
	
	private MultidimensionalArray dimension;
	
	private Vector<DimensionInfo> dimensionsInfo;
	
	
	//private Object dimension;
	
	
	
	public GridIndex(int dimensionCount) {
		dimension = new MultidimensionalArray(dimensionCount);
	
		dimensionsInfo = new Vector<DimensionInfo>();
		
		
//		
//		int[] dimensions = new int[dimensionCount];
//		for(int i=0;i<dimensions.length;i++) {
//			dimensions[i]= 10;
//		}
//		
//		dimension = Array.newInstance(Bucket.class, dimensions);
//		
//		Array.se
	}
	
	public void serialize(String table) throws IOException {
		
//		File file = new File("src/main/resources/data/"+table+"/"+index+".ser");

//		if(!file.exists()){
//			 file.createNewFile();
//		} 
		try {
			  FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/"+table+"/"+"GridIndex.ser");
		         ObjectOutputStream out = new ObjectOutputStream(fileOut);
		         out.writeObject(this);
		         out.close();
		         fileOut.close();
		         //System.out.printf("Serialized data is saved in page.ser");
		      } catch (IOException i) {
		         i.printStackTrace();
		      }
       
}
	
	public MultidimensionalArray getDimension() {
		return dimension;
	}

	public void setDimension(MultidimensionalArray dimension) {
		this.dimension = dimension;
	}

	

	public Vector<DimensionInfo> getDimensionsInfo() {
		return dimensionsInfo;
	}

	public void setDimensionsInfo(Vector<DimensionInfo> dimensionsInfo) {
		this.dimensionsInfo = dimensionsInfo;
	}

	public Cell getByIndex(int[] index)
	{
		MultidimensionalArray current = dimension;
		for(int i = 0; i < index.length-1; i++)
		{
			current = current.getNextDim(index[i]);
		}
		
		return current.getBucket(index[index.length-1]);
	}
}
