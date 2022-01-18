package marmot.support;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Serializables {
	private Serializables() {
		throw new AssertionError("Should not be called: " + Serializables.class);
	}
	
	public static Coordinate readCoordinate(ObjectInputStream ois) throws IOException {
		double x = ois.readDouble();
		double y = ois.readDouble();
		double z = ois.readDouble();
		return new Coordinate(x, y, z);
	}
	public static void writeCoordinate(Coordinate coord, ObjectOutputStream oos)
		throws IOException {
		oos.writeDouble(coord.x);
		oos.writeDouble(coord.y);
		oos.writeDouble(coord.z);
	}
	
	public static Point readPoint(ObjectInputStream ois) throws IOException {
		return GeoClientUtils.toPoint(readCoordinate(ois));
	}
	public static void writePoint(Point pt, ObjectOutputStream oos)
		throws IOException {
		writeCoordinate(pt.getCoordinate(), oos);
	}
	
	public static Envelope readEnvelope(ObjectInputStream ois) throws IOException {
		Coordinate min = readCoordinate(ois);
		Coordinate max = readCoordinate(ois);
		
		return new Envelope(min, max);
	}
	public static void writeEnvelope(Envelope envl, ObjectOutputStream oos)
		throws IOException {
		writeCoordinate(new Coordinate(envl.getMinX(), envl.getMinY()), oos);
		writeCoordinate(new Coordinate(envl.getMaxX(), envl.getMaxY()), oos);
	}
}
