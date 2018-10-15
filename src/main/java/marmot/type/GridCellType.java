package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


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
	
	@Override
	public GridCell readObject(DataInput in) throws IOException {
		int row = in.readInt();
		int col = in.readInt();
		
		return new GridCell(col, row);
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		GridCell cell = (GridCell)obj;
		
		out.writeInt(cell.getRowIdx());
		out.writeInt(cell.getColIdx());
	}
}
