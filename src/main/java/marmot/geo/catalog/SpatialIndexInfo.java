package marmot.geo.catalog;

import java.util.Objects;

import marmot.GeometryColumnInfo;
import marmot.proto.SpatialIndexInfoProto;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialIndexInfo implements PBSerializable<SpatialIndexInfoProto> {
	private String m_dataset;
	private GeometryColumnInfo m_geomCol;
	private String m_hdfsPath;
	
	public SpatialIndexInfo(String dataset, GeometryColumnInfo geomCol) {
		Objects.requireNonNull(dataset);
		Objects.requireNonNull(geomCol);
		
		m_dataset = dataset;
		m_geomCol = geomCol;
	}
	
	public SpatialIndexInfo(String dataset, String geomCol, String srid) {
		this(dataset, new GeometryColumnInfo(geomCol, srid));
	}
	
	public String getDataSetId() {
		return m_dataset;
	}
	
	public GeometryColumnInfo getGeometryColumnInfo() {
		return m_geomCol;
	}
	
	public String getHdfsFilePath() {
		return m_hdfsPath;
	}
	
	public void setHdfsFilePath(String path) {
		m_hdfsPath = path;
	}

	public static SpatialIndexInfo fromProto(SpatialIndexInfoProto proto) {
		GeometryColumnInfo geomCol = GeometryColumnInfo.fromProto(proto.getGeometryColumn());
		SpatialIndexInfo info = new SpatialIndexInfo(proto.getDataset(), geomCol);
		info.setHdfsFilePath(proto.getHdfsPath());
		
		return info;
	}

	@Override
	public SpatialIndexInfoProto toProto() {
		return SpatialIndexInfoProto.newBuilder()
									.setDataset(m_dataset)
									.setGeometryColumn(m_geomCol.toProto())
									.setHdfsPath(m_hdfsPath)
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