package marmot.geo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

import marmot.support.DataUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoJsonReader {
	public static Geometry read(Map<String,Object> geojson) {
		if ( geojson == null ) {
			return null;
		}
		
		Object coords = geojson.get("coordinates");
		String type = (String)geojson.get("type");
		switch ( type ) {
			case "Point":
				return GeoClientUtils.GEOM_FACT.createPoint(toCoordinate(coords));
			case "Polygon":
				return toPolygon(coords);
			case "LineString":
				return toLineString(coords);
			case "MultiPoint":
				return toMultiPoint(coords);
			case "MultiLineString":
				return toMultiLineString(coords);
			case "MultiPolygon":
				return toMultiPolygon(coords);
		}
		
		throw new IllegalArgumentException("unsupported GeoJSON type: " + type);
	}
	
	private static MultiPoint toMultiPoint(Object list) {
		@SuppressWarnings("unchecked")
		List<List<Object>> points = (List<List<Object>>)list;
		
		Coordinate[] coords = points.stream()
									.map(GeoJsonReader::toCoordinate)
									.toArray(sz -> new Coordinate[sz]);
		return GeoClientUtils.GEOM_FACT.createMultiPoint(coords);
	}
	
	private static Polygon toPolygon(Object list) {
		@SuppressWarnings("unchecked")
		List<List<Object>> rings = (List<List<Object>>)list;
		
		LinearRing shell = toLinearRing(rings.get(0));
		LinearRing[] holes = rings.stream()
									.skip(1)
									.map(GeoJsonReader::toLinearRing)
									.toArray(sz -> new LinearRing[sz]);
		return GeoClientUtils.GEOM_FACT.createPolygon(shell, holes);
	}
	
	private static MultiPolygon toMultiPolygon(Object list) {
		@SuppressWarnings("unchecked")
		List<Object> polyList = (List<Object>)list;
		Polygon[] polys = polyList.stream()
										.map(GeoJsonReader::toPolygon)
										.toArray(sz -> new Polygon[sz]);
		return GeoClientUtils.GEOM_FACT.createMultiPolygon(polys);
	}
	
	private static LineString toLineString(Object list) {
		@SuppressWarnings("unchecked")
		List<List<Object>> coordList = (List<List<Object>>)list;
		Coordinate[] coords = coordList.stream()
										.map(GeoJsonReader::toCoordinate)
										.toArray(sz -> new Coordinate[sz]);
		return GeoClientUtils.GEOM_FACT.createLineString(coords);
	}
	
	private static MultiLineString toMultiLineString(Object list) {
		@SuppressWarnings("unchecked")
		List<Object> lineList = (List<Object>)list;
		LineString[] lines = lineList.stream()
										.map(GeoJsonReader::toLineString)
										.toArray(sz -> new LineString[sz]);
		return GeoClientUtils.GEOM_FACT.createMultiLineString(lines);
	}
	
	private static LinearRing toLinearRing(Object list) {
		@SuppressWarnings("unchecked")
		List<List<Object>> coordList = (List<List<Object>>)list;
		Coordinate[] coords = coordList.stream()
										.map(GeoJsonReader::toCoordinate)
										.toArray(sz -> new Coordinate[sz]);
		if ( !coords[0].equals(coords[coords.length-1]) ) {
			coords = Arrays.copyOf(coords, coords.length+1);
			coords[coords.length-1] = coords[0];
		}
		
		return GeoClientUtils.GEOM_FACT.createLinearRing(coords);
	}
	
	private static Coordinate toCoordinate(Object list) {
		try {
			@SuppressWarnings("unchecked")
			List<Object> xy = (List<Object>)list;
			return new Coordinate(DataUtils.asDouble(xy.get(0)), DataUtils.asDouble(xy.get(1)));
		}
		catch ( Exception e ) {
			e.printStackTrace();
			throw e;
		}
	}
}
