package marmot.optor.geo;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum SpatialQueryOperation {
	ALL, INTERSECTS, CONTAINS, CONTAINED_BY;
	
	public static SpatialQueryOperation parse(String opStr) {
		switch ( opStr.toLowerCase() ) {
			case "intersects":
				return INTERSECTS;
			case "all":
				return ALL;
			case "contains":
				return CONTAINS;
			case "contained_by":
				return CONTAINED_BY;
			default:
				throw new IllegalArgumentException("invalid SpatialQueryOperation: op=" + opStr);
		}
	}
}