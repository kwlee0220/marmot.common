package marmot.support;

import java.util.List;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface GeoCoder {
	public List<Coordinate> getWgs84Location(String postalAddress) throws Exception;
}
