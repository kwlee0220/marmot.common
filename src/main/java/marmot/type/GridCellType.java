package marmot.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GridCellType extends DataType {
	private static final GridCellType TYPE = new GridCellType();
	
	public static GridCellType get() {
		return TYPE;
	}
	
	private GridCellType() {
		super("grid_cell", TypeCode.GRID_CELL, GridCell.class);
	}
	
	@Override
	public GridCell newInstance() {
		return new GridCell(0,0);
	}
	
	@Override
	public GridCell fromString(String str) {
		return GridCell.fromString(str);
	}
}
