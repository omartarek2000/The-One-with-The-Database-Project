import java.math.BigDecimal;

public class DimensionInfo {

	private String name;
	private BigDecimal[] range;
	private int indexInTable;
	private String dataType;
	
	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public int getIndexInTable() {
		return indexInTable;
	}

	public void setIndexInTable(int indexInTable) {
		this.indexInTable = indexInTable;
	}

	public DimensionInfo(String name) {
		
		this.name = name;
		range = new BigDecimal[11];
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public BigDecimal[] getRange() {
		return range;
	}

	public void setRange(BigDecimal[] range) {
		this.range = range;
	}
	
	
}
