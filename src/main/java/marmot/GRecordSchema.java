package marmot;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import javax.annotation.Nullable;

import marmot.dataset.GeometryColumnInfo;
import marmot.dataset.GeometryColumnNotExistsException;
import marmot.proto.GRecordSchemaProto;
import marmot.support.PBSerializable;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class GRecordSchema implements PBSerializable<GRecordSchemaProto>, Serializable {
	private static final long serialVersionUID = 1L;
	
	@Nullable private final GeometryColumnInfo m_gcInfo;	// FOption을 serializable하기 싫어서 사용하지 않음
	private final RecordSchema m_schema;
	
	public GRecordSchema(FOption<GeometryColumnInfo> gcInfo, RecordSchema schema) {
		m_schema = schema;
		m_gcInfo = gcInfo.getOrNull();
	}
	
	public GRecordSchema(GeometryColumnInfo gcInfo, RecordSchema schema) {
		m_schema = schema;
		m_gcInfo = gcInfo;
	}
	
	public GRecordSchema(RecordSchema schema) {
		m_schema = schema;
		m_gcInfo = null;
	}
	
	public boolean hasValidGeometry() {
		if ( m_gcInfo == null ) {
			return false;
		}
		
		return m_schema.findColumn(m_gcInfo.name()).isPresent();
	}
	
	public FOption<GeometryColumnInfo> getGeometryColumnInfo() {
		return FOption.ofNullable(m_gcInfo);
	}
	
	public GeometryColumnInfo assertGeometryColumnInfo() {
		if ( m_gcInfo == null ) {
			throw new GeometryColumnNotExistsException();
		}
		
		return m_gcInfo;
	}
	
	public GeometryColumnInfo assertValidGeometryColumnInfo() {
		if ( m_gcInfo == null ) {
			throw new GeometryColumnNotExistsException();
		}
		
		return m_schema.findColumn(m_gcInfo.name()).map(c -> m_gcInfo)
						.getOrThrow(GeometryColumnNotExistsException::new);
	}
	
	public Column getGeometryColumn() {
		return m_schema.getColumn(assertGeometryColumnInfo().name());
	}
	
	public String getGeometryColumnName() {
		return assertGeometryColumnInfo().name();
	}
	
	public int getGeometryColumnIdx() {
		return getGeometryColumn().ordinal();
	}
	
	public int getGeometryColumnIdxOrElse(int defValue) {
		return (m_gcInfo != null) ? m_schema.getColumn(m_gcInfo.name()).ordinal() : -1;
	}
	
	public String getSrid() {
		assertGeometryColumnInfo();
		return m_gcInfo.srid();
	}
	
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public FOption<Column> findColumn(String name) {
		return m_schema.findColumn(name);
	}
	
	public Column getColumnAt(int idx) {
		return m_schema.getColumnAt(idx);
	}
	
	public GRecordSchema derive(RecordSchema schema) {
		if ( m_gcInfo == null ) {
			return new GRecordSchema(schema);
		}
		
		Column prevCol = m_schema.getColumn(m_gcInfo.name());
		GeometryColumnInfo gcInfo = schema.findColumn(m_gcInfo.name())
											.map(col -> prevCol.equals(col) ? m_gcInfo : null)
											.getOrNull();
		return new GRecordSchema(gcInfo, schema);
	}
	
	@Override
	public String toString() {
		return (m_gcInfo != null)
				? String.format("%s: %s", m_gcInfo, m_schema)
				: m_schema.toString();
	}

	public static GRecordSchema fromProto(GRecordSchemaProto proto) {
		Utilities.checkNotNullArgument(proto, "GRecordSchemaProto is null");
		
		RecordSchema schema = RecordSchema.fromProto(proto.getSchema());
		switch ( proto.getOptionalGcInfoCase() ) {
			case GC_INFO:
				return new GRecordSchema(GeometryColumnInfo.fromProto(proto.getGcInfo()), schema);
			case OPTIONALGCINFO_NOT_SET:
				return new GRecordSchema(schema);
			default:
				throw new AssertionError();
		}
	}
	
	@Override
	public GRecordSchemaProto toProto() {
		GRecordSchemaProto.Builder builder = GRecordSchemaProto.newBuilder()
																.setSchema(m_schema.toProto());
		if ( m_gcInfo != null ) {
			builder.setGcInfo(m_gcInfo.toProto());
		}
		return builder.build();
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final GRecordSchemaProto m_proto;
		
		private SerializationProxy(GRecordSchema gschema) {
			m_proto = gschema.toProto();
		}
		
		private Object readResolve() {
			return GRecordSchema.fromProto(m_proto);
		}
	}
}
