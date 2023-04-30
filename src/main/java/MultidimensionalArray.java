import java.util.ArrayList;

public class MultidimensionalArray {
	
	private MultidimensionalArray[] nextDimension = null;
	private Cell[] cell = null;
	
//	private int[] ranges = new int[10];
	
	public MultidimensionalArray(int dimensionCount) {
		if(dimensionCount == 1)
		{
			cell = new Cell[10];
			for(int i=0;i<cell.length;i++) {
				cell[i] = new Cell();
			}
			return;
		}
		
		nextDimension = new MultidimensionalArray[10];
		for(int i = 0; i < 10; i++)
			nextDimension[i] = new MultidimensionalArray(dimensionCount-1);
	}
	
	public MultidimensionalArray getNextDim(int index)
	{
		return nextDimension[index];
	}

	public Cell getBucket(int index)
	{
		return cell[index];
	}
	
//	public int getIndexFromRange(int value)
//	{
//		// TODO
//	}
}
