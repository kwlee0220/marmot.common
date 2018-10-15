package marmot.externio;

import io.vavr.control.Option;
import marmot.GeometryColumnInfo;

public class ImportParameters {
	private String m_dsId;
	private Option<GeometryColumnInfo> m_geomColInfo = Option.none();
	private boolean m_force = false;
	private boolean m_append = false;
	private Option<Long> m_blockSize = Option.none();
	private Option<Boolean> m_compress = Option.none();
	private Option<Integer> m_reportInterval = Option.none();

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
	
	public Option<GeometryColumnInfo> getGeometryColumnInfo() {
		return m_geomColInfo;
	}
	
	public ImportParameters setGeometryColumnInfo(GeometryColumnInfo info) {
		m_geomColInfo = Option.of(info);
		return this;
	}
	
	public ImportParameters setGeometryColumnInfo(String geomCol, String srid) {
		m_geomColInfo = Option.some(new GeometryColumnInfo(geomCol, srid));
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
	
	public Option<Long> getBlockSize() {
		return m_blockSize;
	}
	
	public ImportParameters setBlockSize(long blockSize) {
		m_blockSize = blockSize > 0 ? Option.some(blockSize) : Option.none();
		return this;
	}
	
	public Option<Boolean> getCompress() {
		return m_compress;
	}
	
	public ImportParameters setCompress(Option<Boolean> flag) {
		m_compress = flag;
		return this;
	}
	
	public ImportParameters setCompress(boolean flag) {
		m_compress = Option.some(flag);
		return this;
	}
	
	public Option<Integer> getReportInterval() {
		return m_reportInterval;
	}
	
	public ImportParameters setReportInterval(int interval) {
		m_reportInterval = (interval > 0) ? Option.some(interval) : Option.none();
		return this;
	}
}