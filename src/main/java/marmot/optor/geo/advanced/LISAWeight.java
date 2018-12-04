package marmot.optor.geo.advanced;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum LISAWeight {
	FIXED_DISTANCE_BAND,
	INVERSE_DISTANCE,
	INVERSE_DISTANCE_SQUARED;
	
	public double weigh(double distance, double radius) {
		double w;
		switch ( this ) {
			case FIXED_DISTANCE_BAND:
				return Double.compare(distance, radius) >= 0 ? 0 : 1;
			case INVERSE_DISTANCE:
				w = (radius-distance)/radius;
				return Math.max(0, w);
			case INVERSE_DISTANCE_SQUARED:
				double radiusSquared = radius*radius;
				w = (radiusSquared - (distance*distance)) / radiusSquared;
				return Math.max(0, w);
			default:
				throw new AssertionError("unknown LISAWeight: " + this);
		}
	}
}
