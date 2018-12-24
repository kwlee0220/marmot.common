package marmot.command;

import marmot.GeometryColumnInfo;
import picocli.CommandLine.Option;
import utils.UnitUtils;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreDataSetParameters {
	private FOption<GeometryColumnInfo> m_geomColInfo = FOption.empty();
	private boolean m_force = false;
	private boolean m_append = false;
	private FOption<Long> m_blockSize = FOption.empty();
	private FOption<Boolean> m_compress = FOption.empty();
	private FOption<Integer> m_reportInterval = FOption.empty();
	
	public FOption<GeometryColumnInfo> getGeometryColumnInfo() {
		return m_geomColInfo;
	}

	@Option(names={"-g", "-geom_col"}, paramLabel="col-name(EPSG code)",
			description="default Geometry column info")
	public void setGeometryColumnInfo(String gcInfoStr) {
		setGeometryColumnInfo(GeometryColumnInfo.fromString(gcInfoStr));
	}
	
	public void setGeometryColumnInfo(GeometryColumnInfo info) {
		m_geomColInfo = FOption.ofNullable(info);
	}
	
	public void setGeometryColumnInfo(String geomCol, String srid) {
		m_geomColInfo = FOption.of(new GeometryColumnInfo(geomCol, srid));
	}
	
	public boolean getForce() {
		return m_force;
	}

	@Option(names={"-f", "-force"}, description="force to create a new dataset")
	public void setForce(boolean flag) {
		m_force = flag;
	}
	
	public boolean getAppend() {
		return m_append;
	}

	@Option(names={"-a", "-append"}, description="append to the existing dataset")
	public void setAppend(boolean flag) {
		m_append = flag;
	}
	
	public FOption<Long> getBlockSize() {
		return m_blockSize;
	}

	@Option(names={"-b", "-block_size"}, paramLabel="nbyte", description="block size (eg: '64mb')")
	public void setBlockSize(String blockSizeStr) {
		setBlockSize(UnitUtils.parseByteSize(blockSizeStr));
	}
	
	public void setBlockSize(long blockSize) {
		m_blockSize = blockSize > 0 ? FOption.of(blockSize) : FOption.empty();
	}
	
	public FOption<Boolean> getCompress() {
		return m_compress;
	}
	
	public void setCompress(FOption<Boolean> flag) {
		m_compress = flag;
	}

	@Option(names={"-compress"}, description="compress while stored data")
	public void setCompress(boolean flag) {
		m_compress = FOption.of(flag);
	}
	
	public FOption<Integer> getReportInterval() {
		return m_reportInterval;
	}

	@Option(names={"-report_interval"}, paramLabel="record count",
			description="progress report interval")
	public void setReportInterval(int interval) {
		m_reportInterval = (interval > 0) ? FOption.of(interval) : FOption.empty();
	}
}