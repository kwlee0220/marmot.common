package marmot.geo.catalog;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Envelope;

import marmot.Column;
import marmot.DataSetType;
import marmot.GeometryColumnInfo;
import marmot.RecordSchema;
import marmot.proto.service.DataSetInfoProto;
import marmot.proto.service.DataSetInfoProto.DataSetGeometryInfoProto;
import marmot.proto.service.DataSetTypeProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class DataSetInfo implements PBSerializable<DataSetInfoProto> {
	private String m_id;
	private String m_dir;
	private DataSetType m_type;
	private FOption<GeometryColumnInfo> m_geomColInfo = FOption.empty();
	private Envelope m_bounds = new Envelope();
	private long m_count = 0;
	private RecordSchema m_schema;
	private String m_filePath;
	private boolean m_compression = false;
	private boolean m_mapOutputCompression = false;
	private long m_blockSize = -1;
	
	@SuppressWarnings("unused")
	private DataSetInfo() { }
	
	public DataSetInfo(String id, DataSetType type, RecordSchema schema) {
		Preconditions.checkArgument(id != null, "DataSet's id should not be null.");
		Preconditions.checkArgument(type != null, "DataSet's type should not be null.");
		Preconditions.checkArgument(schema != null, "DataSet's RecordSchema should not be null.");
		
		m_id = id;
		m_type = type;
		m_schema = schema;
		m_bounds.setToNull();
	}
	
	public String getId() {
		return m_id;
	}
	
	public DataSetType getType() {
		return m_type;
	}
	
	public String getDirName() {
		return m_dir;
	}
	
	void setDirName(String dir) {
		m_dir = dir;
	}
	
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public FOption<GeometryColumnInfo> getGeometryColumnInfo() {
		return m_geomColInfo;
	}
	
	public int getGeometryColumnIndex() {
		return m_geomColInfo.map(GeometryColumnInfo::name)
							.map(name -> m_schema.getColumn(name))
							.map(Column::ordinal)
							.getOrElse(-1);
	}
	
	public void setGeometryColumnInfo(String geomCol, String srid) {
		setGeometryColumnInfo(FOption.of(new GeometryColumnInfo(geomCol, srid)));
	}
	
	public void setGeometryColumnInfo(FOption<GeometryColumnInfo> info) {
		if ( info.isPresent() ) {
			String colName = info.get().name();
			Column col = m_schema.findColumn(colName).getOrNull();
			if ( col == null ) {
				throw new IllegalArgumentException("No such geometry column: name=" + colName);
			}
			if ( !col.type().isGeometryType() ) {
				throw new IllegalArgumentException("Geometry column is not Geometry type: name="
													+ colName + ", type=" + col.type());
			}
		}
		
		m_geomColInfo = info;
	}
	
	public Envelope getBounds() {
		return m_bounds;
	}
	
	public void setBounds(Envelope bounds) {
		m_bounds = bounds;
	}
	
	public long getRecordCount() {
		return m_count;
	}
	
	public void setRecordCount(long count) {
		m_count = count;
	}
	
	public String getFilePath() {
		return m_filePath;
	}
	
	public void setFilePath(String path) {
		m_filePath = path;
	}
	
	public DataSetInfo duplicate() {
		DataSetInfo info = new DataSetInfo(m_id, m_type, m_schema);
		info.m_geomColInfo = m_geomColInfo;
		info.m_filePath = m_filePath;
		
		return info;
	}
	
	public boolean getCompression() {
		return m_compression;
	}
	
	public void setCompression(boolean flag) {
		m_compression = flag;
	}
	
	public boolean getMapOutputCompression() {
		return m_mapOutputCompression;
	}
	
	public void setMapOutputCompression(boolean flag) {
		m_mapOutputCompression = flag;
	}
	
	public long getBlockSize() {
		return m_blockSize;
	}
	
	public void setBlockSize(long blkSz) {
		m_blockSize = blkSz;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		builder.append(m_type.id().toUpperCase())
				.append("[")
				.append(m_id);
		m_geomColInfo.map(GeometryColumnInfo::name)
					.map(m_schema::getColumn)
					.ifPresent(col -> builder.append("," + col));
		return builder.append("]").toString();
	}
	
	@Override
	public boolean equals(Object o) {
		if ( this == o ) {
			return true;
		}
		if ( o == null || !(o instanceof DataSetInfo) ) {
			return false;
		}
		DataSetInfo other = (DataSetInfo)o;
		return m_id.equals(other.m_id);
	}
	
	@Override
	public int hashCode() {
		return m_id.hashCode();
	}
	
	public static DataSetInfo fromProto(DataSetInfoProto proto) {
		DataSetInfo info = new DataSetInfo(proto.getId(),
											DataSetType.fromString(proto.getType().name()),
											RecordSchema.fromProto(proto.getRecordSchema()));
		info.m_dir = proto.getDirName();
		info.m_count = proto.getRecordCount();
		info.m_filePath = proto.getHdfsPath();
		
		FOption<DataSetGeometryInfoProto> gip = PBUtils.getOptionField(proto, "geometry_info");
		gip.ifPresent(geom -> {
			info.m_geomColInfo = FOption.of(new GeometryColumnInfo(geom.getColumn(),
																	geom.getSrid()));
			info.m_bounds = PBUtils.fromProto(geom.getBounds());
		});
		
		info.m_compression = proto.getCompression();
		info.m_blockSize = proto.getBlockSize();
		
		return info;
	}

	@Override
	public DataSetInfoProto toProto() {
		DataSetInfoProto.Builder builder = DataSetInfoProto.newBuilder()
												.setId(m_id)
												.setDirName(m_dir)
												.setType(DataSetTypeProto.valueOf(m_type.id()))
												.setRecordSchema(m_schema.toProto())
												.setRecordCount(m_count)
												.setHdfsPath(m_filePath)
												.setBlockSize(getBlockSize())
												.setCompression(m_compression);
		m_geomColInfo.ifPresent(geomCol -> {
			DataSetGeometryInfoProto geomProto = DataSetGeometryInfoProto.newBuilder()
												.setColumn(geomCol.name())
												.setSrid(geomCol.srid())
												.setColumnIndex(getGeometryColumnIndex())
												.setBounds(PBUtils.toProto(m_bounds))
												.build();
			builder.setGeometryInfo(geomProto);
		});
		
		return builder.build();
	}
}