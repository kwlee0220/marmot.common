package marmot.type;

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
}
