package marmot.command;

import marmot.GeometryColumnInfo;
import marmot.StoreDataSetOptions;
import picocli.CommandLine.Option;
import utils.UnitUtils;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreDataSetParameters {
	private final StoreDataSetOptions m_options;
	private FOption<Integer> m_reportInterval = FOption.empty();
	
	public StoreDataSetParameters() {
		m_options = StoreDataSetOptions.create()
										.force(false)
										.append(false);
	}
	private StoreDataSetParameters(StoreDataSetOptions opts) {
		m_options = opts;
	}
	
	public FOption<GeometryColumnInfo> getGeometryColumnInfo() {
		return m_options.geometryColumnInfo();
	}

	@Option(names={"-g", "-geom_col"}, paramLabel="col-name(EPSG code)",
			description="default Geometry column info")
	public void setGeometryColumnInfo(String gcInfoStr) {
		setGeometryColumnInfo(GeometryColumnInfo.fromString(gcInfoStr));
	}
	
	public void setGeometryColumnInfo(GeometryColumnInfo info) {
		m_options.geometryColumnInfo(info);
	}
	
	public void setGeometryColumnInfo(String geomCol, String srid) {
		setGeometryColumnInfo(new GeometryColumnInfo(geomCol, srid));
	}
	
	public boolean getForce() {
		return m_options.force().getOrElse(false);
	}

	@Option(names={"-f", "-force"}, description="force to create a new dataset")
	public void setForce(boolean flag) {
		m_options.force(flag);
	}
	
	/**
	 * 기존 데이터세트에 추가 여부를 반환한다.
	 * 
	 * @return 기존 데이터세트에 추가 여부
	 */
	public boolean getAppend() {
		return m_options.append().getOrElse(false);
	}

	/**
	 * 기존 데이터세트에 추가 여부를 설정한다.
	 * 
	 * @param flag	기존 데이터 세트에 추가여부.
	 */
	@Option(names={"-a", "-append"}, description="append to the existing dataset")
	public void setAppend(boolean flag) {
		m_options.append(flag);
	}
	
	public FOption<Long> getBlockSize() {
		return m_options.blockSize();
	}

	@Option(names={"-b", "-block_size"}, paramLabel="nbyte", description="block size (eg: '64mb')")
	public void setBlockSize(String blockSizeStr) {
		setBlockSize(UnitUtils.parseByteSize(blockSizeStr));
	}
	
	public void setBlockSize(long blockSize) {
		m_options.blockSize(blockSize);
	}
	
	public FOption<Boolean> getCompression() {
		return m_options.compression();
	}

	@Option(names={"-compression"}, description="compress while stored data")
	public void setCompression(boolean flag) {
		m_options.compression(flag);
	}
	
	public FOption<Integer> getReportInterval() {
		return m_reportInterval;
	}

	@Option(names={"-report_interval"}, paramLabel="record count",
			description="progress report interval")
	public void setReportInterval(int interval) {
		m_reportInterval = (interval > 0) ? FOption.of(interval) : FOption.empty();
	}
	
	public StoreDataSetOptions toOptions() {
		return m_options.duplicate();
	}
	
	public static StoreDataSetParameters fromOptions(StoreDataSetOptions options) {
		StoreDataSetOptions opts = options.duplicate();
		opts.force().ifAbsent(() -> opts.force(false));
		opts.append().ifAbsent(() -> opts.append(false));
		
		return new StoreDataSetParameters(opts);
	}
}