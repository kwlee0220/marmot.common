package marmot.geo.catalog;

import java.util.Objects;

import com.vividsolutions.jts.geom.Envelope;

import marmot.GeometryColumnInfo;
import marmot.proto.SpatialIndexInfoProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;


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
	
	public Envelope getTileBounds() {
		return m_tileBounds;
	}
	
	public void setTileBounds(Envelope envl) {
		m_tileBounds = envl;
	}
	
	public Envelope getDataBounds() {
		return m_dataBounds;
	}
	
	public void setDataBounds(Envelope envl) {
		m_dataBounds = envl;
	}
	
	public long getRecordCount() {
		return m_count;
	}
	
	public void setRecordCount(long count) {
		m_count = count;
	}

	public static SpatialIndexInfo fromProto(SpatialIndexInfoProto proto) {
		GeometryColumnInfo geomCol = GeometryColumnInfo.fromProto(proto.getGeometryColumn());
		SpatialIndexInfo info = new SpatialIndexInfo(proto.getDataset(), geomCol);
		info.setTileBounds(PBUtils.fromProto(proto.getTileBounds()));
		info.setDataBounds(PBUtils.fromProto(proto.getDataBounds()));
		info.setRecordCount(proto.getRecordCount());
		info.setHdfsFilePath(proto.getHdfsPath());
		
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