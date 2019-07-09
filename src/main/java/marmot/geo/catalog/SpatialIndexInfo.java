package marmot.geo.catalog;

import java.util.Objects;

import com.vividsolutions.jts.geom.Envelope;

import marmot.GeometryColumnInfo;
import marmot.proto.SpatialIndexInfoProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialIndexInfo implements PBSerializable<SpatialIndexInfoProto> {
	private String m_dataset;
	private GeometryColumnInfo m_geomCol;
	private Envelope m_tileBounds = new Envelope();
	private Envelope m_dataBounds = new Envelope();
	private long m_count = -1;
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
		info.setRecordCount(proto.getRecordCount());
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
									.setRecordCount(m_count)
									.setHdfsPath(m_hdfsPath)
									.setUpdatedMillis(m_updatedMillis)
									.build();
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		builder.append("INDEX")
				.append("[" + m_dataset)
				.append("[" + m_geomCol + "]");
		return builder.append("]").toString();
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
}