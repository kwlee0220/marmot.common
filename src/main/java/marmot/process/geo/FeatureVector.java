package marmot.process.geo;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Point;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.geo.GeoClientUtils;
import marmot.proto.RecordProto;
import marmot.protobuf.PBUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FeatureVector implements Serializable {
	private static final long serialVersionUID = 1192199487862315068L;
	
	private Object[] m_values;
	
	@SuppressWarnings("unused")
	private FeatureVector() { }
	
	public FeatureVector(Object[] values) {
		m_values = values;
	}
	
	public FeatureVector(List<Object> values) {
		m_values = values.toArray();
	}
	
	public FeatureVector(double... values) {
		m_values = Arrays.stream(values).mapToObj(v -> (Double)v).toArray(sz -> new Object[sz]);
	}
	
	public FeatureVector(double value) {
		m_values = new Object[]{value};
	}
	
	public Object get(int idx) {
		return m_values[idx];
	}
	
	public Object[] values() {
		return m_values;
	}
	
	public int length() {
		return m_values.length;
	}
	
	public static FeatureVector take(Record record, List<Integer> colIdxes) {
		Object[] values = colIdxes.stream().map(record::get).toArray();
		return new FeatureVector(values);
	}
	
	public void accumulate(FeatureVector feature) {
		Object[] other = feature.m_values;
		for ( int i =0; i < m_values.length; ++i ) {
			m_values[i] = add(m_values[i], other[i]);
		}
	}
	
	public FeatureVector divideBy(double denominator) {
		return new FeatureVector(Arrays.stream(m_values)
										.map(obj -> divideBy(obj, denominator))
										.toArray());
	}
	
	public double distance(FeatureVector other) {
		Object[] vector = other.m_values;
		double sum = IntStream.range(0, vector.length)
								.mapToDouble(idx ->
									FeatureVector.deltaSquare(m_values[idx], vector[idx]))
								.sum();
		return Math.sqrt(sum);
	}
	
	@Override
	public String toString() {
		return m_values.toString();
	}

	public static FeatureVector calcMean(List<FeatureVector> featureList) {
		FeatureVector accum = new FeatureVector(Lists.newArrayList(featureList.get(0).m_values));
		for ( int i = 1; i < featureList.size(); ++i ) {
			accum.accumulate(featureList.get(i));
		}
		
		return accum.divideBy(featureList.size());
	}

	private static Object divideBy(Object obj, double denominator) {
		if ( obj instanceof Double ) {
			return (Double)obj / denominator;
		}
		else if ( obj instanceof Point ) {
			Point pt = (Point)obj;
			
			return GeoClientUtils.toPoint(pt.getX()/denominator, pt.getY()/denominator);
		}
		else if ( obj instanceof Float ) {
			return (Float)obj / denominator;
		}
		else if ( obj instanceof Long ) {
			return (Long)obj / denominator;
		}
		else if ( obj instanceof Integer ) {
			return (Integer)obj / denominator;
		}
		else {
			throw new IllegalArgumentException("invalid value to divide: " + obj);
		}
	}
	
	private static double deltaSquare(Object o1, Object o2) {
		if ( o1 instanceof Double ) {
			return Math.pow((Double)o1 - (Double)o2, 2);
		}
		else if ( o1 instanceof Point ) {
			return  Math.pow(((Point)o1).distance((Point)o2), 2);
		}
		else if ( o1 instanceof Float ) {
			return Math.pow((Float)o1 - (Float)o2, 2);
		}
		else if ( o1 instanceof Long ) {
			return Math.pow((Long)o1 - (Long)o2, 2);
		}
		else if ( o1 instanceof Integer ) {
			return Math.pow((Integer)o1 - (Integer)o2, 2);
		}
		else {
			throw new IllegalArgumentException("invalid value to get distance: " + o1 + ", " + o2);
		}
	}

	private static double distance(Object o1, Object o2) {
		if ( o1 instanceof Double ) {
			return Math.sqrt(Math.pow((Double)o1 - (Double)o2, 2));
		}
		else if ( o1 instanceof Point ) {
			return ((Point)o1).distance((Point)o2);
		}
		else if ( o1 instanceof Float ) {
			return Math.sqrt(Math.pow((Float)o1 - (Float)o2, 2));
		}
		else if ( o1 instanceof Long ) {
			return Math.sqrt(Math.pow((Long)o1 - (Long)o2, 2));
		}
		else if ( o1 instanceof Integer ) {
			return Math.sqrt(Math.pow((Integer)o1 - (Integer)o2, 2));
		}
		else {
			throw new IllegalArgumentException("invalid value to get distance: " + o1 + ", " + o2);
		}
	}
	
	public static List<String> validate(RecordSchema schema, List<String> featureColNames) {
		List<String> msgs = Lists.newArrayList();
		for ( String colName: featureColNames ) {
			Column col = schema.getColumn(colName, null);
			if ( col == null ) {
				msgs.add(String.format("unknown column: %s", colName));
			}
			else if ( !validate(col) ) {
				msgs.add(String.format("invalid feature column type: name=%s type=%s",
										colName, col.type()));
			}
		}
		
		return msgs;
	}

	private static Object add(Object o1, Object o2) {
		if ( o1 instanceof Double ) {
			return (Double)o1 + (Double)o2;
		}
		else if ( o1 instanceof Point ) {
			Point pt1 = (Point)o1;
			Point pt2 = (Point)o2;
			
			double x = pt1.getX() + pt2.getX();
			double y = pt1.getY() + pt2.getY();
			return GeoClientUtils.toPoint(x, y);
		}
		else if ( o1 instanceof Float ) {
			return (Float)o1 + (Float)o2;
		}
		else if ( o1 instanceof Long ) {
			return (Long)o1 + (Long)o2;
		}
		else if ( o1 instanceof Integer ) {
			return (Integer)o1 + (Integer)o2;
		}
		else {
			throw new IllegalArgumentException("invalid value to add: " + o1 + ", " + o2);
		}
	}
	
	private static boolean validate(Column col) {
		switch ( col.type().getTypeCode() ) {
			case DOUBLE:
			case POINT:
			case FLOAT:
			case LONG:
			case INT:
				return true;
			default:
				return false;
		}
	}

	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = -3323143613452318584L;
		
		private final RecordProto m_proto;
		
		private SerializationProxy(FeatureVector keyValue) {
			m_proto = FStream.of(keyValue.m_values)
								.map(PBUtils::toValueProto)
								.foldLeft(RecordProto.newBuilder(), (b,p) -> b.addValue(p))
								.build();
		}
		
		private Object readResolve() {
			Object[] values = m_proto.getValueList().stream()
									.map(PBUtils::fromProto)
									.toArray();
			return new FeatureVector(values);
		}
	}
}
