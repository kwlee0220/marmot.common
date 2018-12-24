package marmot;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import marmot.proto.GeometryColumnInfoProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class GeometryColumnInfo implements PBSerializable<GeometryColumnInfoProto> {
	private static final Pattern PATTERN = Pattern.compile("(\\S+)\\s*\\(\\s*(\\S+)\\s*\\)");
	
	private final String m_name;
	private final String m_srid;
	
	public GeometryColumnInfo(String colName, String srid) {
		m_name = colName;
		m_srid = srid;
	}
	
	public final String name() {
		return m_name;
	}
	
	public final String srid() {
		return m_srid;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		GeometryColumnInfo other = (GeometryColumnInfo)obj;
		return Objects.equals(m_name, other.m_name)
			&& Objects.equals(m_srid, other.m_srid);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_name, m_srid);
	}
	
	public static GeometryColumnInfo fromString(String str) {
		Matcher matcher = PATTERN.matcher(str.trim());
		if ( !matcher.find() ) {
			throw new IllegalArgumentException(String.format("invalid: '%s'", str));
		}
		
		return new GeometryColumnInfo(matcher.group(1), matcher.group(2));
	}
	
	@Override
	public String toString() {
		return String.format("%s(%s)", m_name, m_srid);
	}
	
	public static GeometryColumnInfo fromProto(GeometryColumnInfoProto proto) {
		return new GeometryColumnInfo(proto.getName(), proto.getSrid());
	}

	@Override
	public GeometryColumnInfoProto toProto() {
		return GeometryColumnInfoProto.newBuilder()
									.setName(m_name)
									.setSrid(m_srid)
									.build();
	}
	
//	private Object writeReplace() {
//		return new SerializationProxy(this);
//	}
//	
//	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
//		throw new InvalidObjectException("Use Serialization Proxy instead.");
//	}
//
//	private static class SerializationProxy implements Serializable {
//		private static final long serialVersionUID = -9086142636497979939L;
//		
//		private final GeometryColumnInfoProto m_proto;
//		
//		private SerializationProxy(GeometryColumnInfo info) {
//			m_proto = info.toProto();
//		}
//		
//		private Object readResolve() {
//			return GeometryColumnInfo.fromProto(m_proto);
//		}
//	}
}
