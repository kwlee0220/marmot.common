package marmot.geo.catalog;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;

import org.locationtech.jts.geom.Envelope;

import marmot.dataset.GeometryColumnInfo;
import marmot.proto.SpatialIndexInfoProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialIndexInfo implements PBSerializable<SpatialIndexInfoProto>, Serializable {
	private static final long serialVersionUID = 707886010251863982L;
	
	private String m_dataset;
	private GeometryColumnInfo m_geomCol;
	private Envelope m_tileBounds = new Envelope();
	private Envelope m_dataBounds = new Envelope();
	private int m_clusterCount = -1;
	private long m_count = -1;
	private long m_nonDuplicatedCount = -1;
	private String m_hdfsPath;
	private long m_updatedMillis = -1;
	
	public SpatialIndexInfo(String dataset, GeometryColumnInfo geomCol) {
		Utilities.checkNotNullArgument(dataset, "dataset is null");
		Utilities.checkNotNullArgument(geomCol, "geomCol is null");
		
		m_dataset = dataset;
		m_geomCol = geomCol;
	}
	
	public SpatialIndexInfo(String dataset, String geomCol, String srid) {
		this(dataset, new GeometryColumnInfo(geomCol, srid));
	}
	
	/**
	 * 공간 인덱스가 생성된 대상 데이터 세트 식별자를 반환한다.
	 * 
	 * @return	데이터 세트 식별자
	 */
	public String getDataSetId() {
		return m_dataset;
	}
	
	/**
	 * 공간 인덱스 대상 공간 컬럼 정보를 반환한다.
	 * 
	 * @return	공간 컬럼 정보
	 */
	public GeometryColumnInfo getGeometryColumnInfo() {
		return m_geomCol;
	}
	
	public String getHdfsFilePath() {
		return m_hdfsPath;
	}
	
	public void setHdfsFilePath(String path) {
		m_hdfsPath = path;
	}
	
	/**
	 * 공간 인덱스의 타일 단위 MBR(miminal bounding rectangle)을 반환한다.
	 * 
	 * @return	공간 인덱스 MBR
	 */
	public Envelope getTileBounds() {
		return m_tileBounds;
	}
	
	public void setTileBounds(Envelope envl) {
		m_tileBounds = envl;
	}

	/**
	 * 공간 인덱스의 MBR(miminal bounding rectangle)을 반환한다.
	 * 
	 * @return	공간 인덱스 MBR
	 */
	public Envelope getDataBounds() {
		return m_dataBounds;
	}
	
	public void setDataBounds(Envelope envl) {
		m_dataBounds = envl;
	}
	
	public int getClusterCount() {
		return m_clusterCount;
	}
	
	public void setClusterCount(int count) {
		m_clusterCount = count;
	}

	/**
	 * 공간 인덱스에 기록된 레코드의 수를 반환한다.
	 * 
	 * @return	레코드 수
	 */
	public long getRecordCount() {
		return m_count;
	}
	
	public void setRecordCount(long count) {
		m_count = count;
	}
	
	public long getNonDuplicatedRecordCount() {
		return m_nonDuplicatedCount;
	}
	
	public void setNonDuplicatedRecordCount(long count) {
		m_nonDuplicatedCount = count;
	}
	
	public long getUpdatedMillis() {
		return m_updatedMillis;
	}
	
	public void setUpdatedMillis(long millis) {
		m_updatedMillis = millis;
	}

	public static SpatialIndexInfo fromProto(SpatialIndexInfoProto proto) {
		GeometryColumnInfo geomCol = GeometryColumnInfo.fromProto(proto.getGeometryColumn());
		SpatialIndexInfo info = new SpatialIndexInfo(proto.getDataset(), geomCol);
		info.setTileBounds(PBUtils.fromProto(proto.getTileBounds()));
		info.setDataBounds(PBUtils.fromProto(proto.getDataBounds()));
		info.setClusterCount(proto.getClusterCount());
		info.setRecordCount(proto.getRecordCount());
		info.setNonDuplicatedRecordCount(proto.getNonDuplicatedRecordCount());
		info.setHdfsFilePath(proto.getHdfsPath());
		info.setUpdatedMillis(proto.getUpdatedMillis());
		
		return info;
	}

	@Override
	public SpatialIndexInfoProto toProto() {
		return SpatialIndexInfoProto.newBuilder()
									.setDataset(m_dataset)
									.setGeometryColumn(m_geomCol.toProto())
									.setTileBounds(PBUtils.toProto(m_tileBounds))
									.setDataBounds(PBUtils.toProto(m_dataBounds))
									.setClusterCount(m_clusterCount)
									.setRecordCount(m_count)
									.setNonDuplicatedRecordCount(m_nonDuplicatedCount)
									.setHdfsPath(m_hdfsPath)
									.setUpdatedMillis(m_updatedMillis)
									.build();
	}
	
	@Override
	public String toString() {
		return String.format("INDEX[%s, %s, nclusters=%d, nrecords=%d, ndistincts=%d]",
							m_dataset, m_geomCol, m_clusterCount, m_count, m_nonDuplicatedCount);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		SpatialIndexInfo other = (SpatialIndexInfo)obj;
		return Objects.equals(m_dataset, other.m_dataset)
			&& Objects.equals(m_geomCol, other.m_geomCol);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_dataset, m_geomCol);
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = -5230520733062001761L;
		
		private final SpatialIndexInfoProto m_proto;
		
		private SerializationProxy(SpatialIndexInfo info) {
			m_proto = info.toProto();
		}
		
		private Object readResolve() {
			return SpatialIndexInfo.fromProto(m_proto);
		}
	}
}