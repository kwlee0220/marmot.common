package marmot.command;

import java.util.Optional;

import utils.UnitUtils;

import marmot.dataset.GeometryColumnInfo;
import marmot.optor.CreateDataSetOptions;
import marmot.optor.StoreDataSetOptions;

import picocli.CommandLine.Option;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreDataSetParameters {
	private StoreDataSetOptions m_options = StoreDataSetOptions.DEFAULT;
	private Optional<Integer> m_reportInterval = Optional.empty();
	
	public StoreDataSetParameters() {
	}
	private StoreDataSetParameters(StoreDataSetOptions opts) {
		m_options = opts;
	}
	
	public Optional<GeometryColumnInfo> getGeometryColumnInfo() {
		return m_options.geometryColumnInfo();
	}

	@Option(names={"-g", "-geom_col"}, paramLabel="col-name(EPSG code)",
			description="default Geometry column info")
	public void setGeometryColumnInfo(String gcInfoStr) {
		setGeometryColumnInfo(GeometryColumnInfo.fromString(gcInfoStr));
	}
	
	public void setGeometryColumnInfo(GeometryColumnInfo info) {
		m_options = m_options.geometryColumnInfo(info);
	}
	
	public void setGeometryColumnInfo(String geomCol, String srid) {
		setGeometryColumnInfo(new GeometryColumnInfo(geomCol, srid));
	}
	
	public boolean getForce() {
		return m_options.force();
	}

	@Option(names={"-f", "-force"}, description="force to create a new dataset")
	public void setForce(boolean flag) {
		m_options = m_options.force(flag);
	}
	
	/**
	 * 기존 데이터세트에 추가 여부를 반환한다.
	 * 
	 * @return 기존 데이터세트에 추가 여부
	 */
	public boolean getAppend() {
		return m_options.append().orElse(false);
	}

	/**
	 * 기존 데이터세트에 추가 여부를 설정한다.
	 * 
	 * @param flag	기존 데이터 세트에 추가여부.
	 */
	@Option(names={"-a", "-append"}, description="append to the existing dataset")
	public void setAppend(boolean flag) {
		m_options = m_options.append(flag);
	}
	
	public Optional<Long> getBlockSize() {
		return m_options.blockSize();
	}

	@Option(names={"-b", "-block_size"}, paramLabel="nbyte", description="block size (eg: '64mb')")
	public void setBlockSize(String blockSizeStr) {
		setBlockSize(UnitUtils.parseByteSize(blockSizeStr));
	}
	
	public void setBlockSize(long blockSize) {
		m_options = m_options.blockSize(blockSize);
	}
	
	public Optional<String> getCompressionCodecName() {
		return m_options.compressionCodecName();
	}

	@Option(names={"-c", "-compress"}, description="compression codec name")
	public void setCompressionCodecName(String codecName) {
		m_options = m_options.compressionCodecName(codecName);
	}
	
	public Optional<Integer> getReportInterval() {
		return m_reportInterval;
	}

	@Option(names={"-report_interval"}, paramLabel="record count",
			description="progress report interval")
	public void setReportInterval(int interval) {
		m_reportInterval = (interval > 0) ? Optional.of(interval) : Optional.empty();
	}
	
	public StoreDataSetOptions toOptions() {
		return m_options;
	}
	
	public CreateDataSetOptions toCreateOptions() {
		return m_options.toCreateOptions();
	}
	
	public static StoreDataSetParameters fromOptions(StoreDataSetOptions options) {
		return new StoreDataSetParameters(options);
	}
}