package marmot.externio;

import marmot.GeometryColumnInfo;
import utils.func.FOption;

public class ImportParameters {
	private String m_dsId;
	private FOption<GeometryColumnInfo> m_geomColInfo = FOption.empty();
	private boolean m_force = false;
	private boolean m_append = false;
	private FOption<Long> m_blockSize = FOption.empty();
	private FOption<Boolean> m_compress = FOption.empty();
	private FOption<Integer> m_reportInterval = FOption.empty();

	public static ImportParameters create() {
		return new ImportParameters();
	}
	
	public String getDatasetId() {
		return m_dsId;
	}
	
	public ImportParameters setDatasetId(String dsId) {
		m_dsId = dsId;
		return this;
	}
	
	public FOption<GeometryColumnInfo> getGeometryColumnInfo() {
		return m_geomColInfo;
	}
	
	public ImportParameters setGeometryColumnInfo(GeometryColumnInfo info) {
		m_geomColInfo = FOption.ofNullable(info);
		return this;
	}
	
	public ImportParameters setGeometryColumnInfo(String geomCol, String srid) {
		m_geomColInfo = FOption.of(new GeometryColumnInfo(geomCol, srid));
		return this;
	}
	
	public boolean getForce() {
		return m_force;
	}
	
	public ImportParameters setForce(boolean flag) {
		m_force = flag;
		return this;
	}
	
	public boolean getAppend() {
		return m_append;
	}
	
	public ImportParameters setAppend(boolean flag) {
		m_append = flag;
		return this;
	}
	
	public FOption<Long> getBlockSize() {
		return m_blockSize;
	}
	
	public ImportParameters setBlockSize(long blockSize) {
		m_blockSize = blockSize > 0 ? FOption.of(blockSize) : FOption.empty();
		return this;
	}
	
	public FOption<Boolean> getCompress() {
		return m_compress;
	}
	
	public ImportParameters setCompress(FOption<Boolean> flag) {
		m_compress = flag;
		return this;
	}
	
	public ImportParameters setCompress(boolean flag) {
		m_compress = FOption.of(flag);
		return this;
	}
	
	public FOption<Integer> getReportInterval() {
		return m_reportInterval;
	}
	
	public ImportParameters setReportInterval(int interval) {
		m_reportInterval = (interval > 0) ? FOption.of(interval) : FOption.empty();
		return this;
	}
}