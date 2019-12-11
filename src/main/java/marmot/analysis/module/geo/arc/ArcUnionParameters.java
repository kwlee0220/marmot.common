package marmot.analysis.module.geo.arc;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import marmot.support.DataUtils;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcUnionParameters {
	private static final String LEFT_DATASET = "left_dataset";
	private static final String LEFT_KEY_COLS = "left_key_cols";
	private static final String LEFT_PREFIX = "left_prefix";
	private static final String RIGHT_DATASET = "right_dataset";
	private static final String RIGHT_KEY_COLS = "right_key_cols";
	private static final String RIGHT_PREFIX = "right_prefix";
	private static final String OUTPUT_DATASET = "output_dataset";
	private static final String FORCE = "force";
	private static final String COMPRESSION = "compression";
	private static final String BLOCK_SIZE = "block_size";
	
	private final Map<String,String> m_params;
	
	public static List<String> getParameterNameAll() {
		return Arrays.asList(LEFT_DATASET, LEFT_KEY_COLS, LEFT_PREFIX,
							RIGHT_DATASET, RIGHT_KEY_COLS, RIGHT_PREFIX,
							OUTPUT_DATASET, FORCE, COMPRESSION, BLOCK_SIZE);
	}
	
	public ArcUnionParameters() {
		m_params = Maps.newHashMap();
	}
	
	public ArcUnionParameters(Map<String,String> paramsMap) {
		this();
		
		m_params.putAll(paramsMap);
	}
	
	public String getLeftDataSet() {
		return m_params.get(LEFT_DATASET);
	}
	public void setLeftDataSet(String dsId) {
		Utilities.checkNotNullArgument(dsId, "left dataset id");
		
		m_params.put(LEFT_DATASET, dsId);
	}
	
	public String getLeftKeyColumns() {
		return m_params.get(LEFT_KEY_COLS);
	}
	public void setLeftKeyColumns(String keyCols) {
		Utilities.checkNotNullArgument(keyCols, "left key columns");
		
		m_params.put(LEFT_KEY_COLS, keyCols);
	}
	
	public FOption<String> getLeftPrefix() {
		return FOption.ofNullable(m_params.get(LEFT_PREFIX));
	}
	public void setLeftPrefix(String prefix) {
		Utilities.checkNotNullArgument(prefix, "left prefix is null");
		
		m_params.put(LEFT_PREFIX, prefix);
	}
	
	public String getRightDataSet() {
		return m_params.get(RIGHT_DATASET);
	}
	public void setRightDataSet(String dsId) {
		Utilities.checkNotNullArgument(dsId, "right dataset id");
		
		m_params.put(RIGHT_DATASET, dsId);
	}
	
	public String getRightKeyColumns() {
		return m_params.get(RIGHT_KEY_COLS);
	}
	public void setRightKeyColumns(String keyCols) {
		Utilities.checkNotNullArgument(keyCols, "right key columns");
		
		m_params.put(RIGHT_KEY_COLS, keyCols);
	}
	
	public FOption<String> getRightPrefix() {
		return FOption.ofNullable(m_params.get(RIGHT_PREFIX));
	}
	public void setRightPrefix(String prefix) {
		Utilities.checkNotNullArgument(prefix, "right prefix is null");
		
		m_params.put(RIGHT_PREFIX, prefix);
	}
	
	public String getOutputDataset() {
		return m_params.get(OUTPUT_DATASET);
	}
	public void setOutputDataset(String dsId) {
		Utilities.checkNotNullArgument(dsId, "output dataset");
		
		m_params.put(OUTPUT_DATASET, dsId);
	}
	
	public FOption<Boolean> getForce() {
		return FOption.ofNullable(m_params.get(FORCE))
						.map(DataUtils::asBoolean);
	}
	
	public void setForce(boolean flag) {
		m_params.put(FORCE, "" + flag);
	}
	
	public FOption<String> getCompressionCodecName() {
		return FOption.ofNullable(m_params.get(COMPRESSION));
	}
	
	public void setCompressionCodecName(String codecName) {
		if ( codecName != null ) {
			m_params.put(COMPRESSION, codecName);
		}
		else {
			m_params.remove(COMPRESSION);
		}
	}
	
	public FOption<Long> getBlockSize() {
		return FOption.ofNullable(m_params.get(BLOCK_SIZE))
						.map(DataUtils::asLong);
	}
	public void setBlockSize(long sz) {
		m_params.put(BLOCK_SIZE, "" + sz);
	}
	
	public static ArcUnionParameters fromMap(Map<String,String> paramsMap) {
		return new ArcUnionParameters(paramsMap);
	}
	
	public Map<String,String> toMap() {
		return m_params;
	}
	
	public void checkValidity() {
		checkValidity(LEFT_DATASET);
		checkValidity(LEFT_KEY_COLS);
		checkValidity(RIGHT_DATASET);
		checkValidity(RIGHT_KEY_COLS);
		checkValidity(OUTPUT_DATASET);
	}
	
	private void checkValidity(String param) {
		if ( m_params.get(param) == null ) {
			throw new IllegalArgumentException("parameter is undefined: " + param);
		}
	}
	
	@Override
	public String toString() {
		return m_params.toString();
	}
}
