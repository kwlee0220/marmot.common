package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TileType extends DataType {
	private static final TileType TYPE = new TileType();
	
	public static TileType get() {
		return TYPE;
	}
	
	private TileType() {
		super("tile", TypeCode.TILE, MapTile.class);
	}

	@Override
	public MapTile newInstance() {
		return new MapTile(0,0,0);
	}
	
	@Override
	public MapTile fromString(String str) {
		return MapTile.fromString(str);
	}
	
	@Override
	public MapTile readObject(DataInput in) throws IOException {
		byte zoom = in.readByte();
		int x = in.readInt();
		int y = in.readInt();
		
		return new MapTile(zoom, x, y);
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		MapTile tile = (MapTile)obj;
		
		out.writeByte((byte)tile.getZoom());
		out.writeInt(tile.getX());
		out.writeInt(tile.getY());
	}
}
