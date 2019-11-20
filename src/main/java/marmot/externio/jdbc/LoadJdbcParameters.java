package marmot.externio.jdbc;

import picocli.CommandLine.Option;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadJdbcParameters extends JdbcParameters {
	private FOption<String> m_selectExpr = FOption.empty();
	private FOption<String> m_wkbCols = FOption.empty();
	private FOption<String> m_srid = FOption.empty();
	
	public LoadJdbcParameters() { }
	
	public LoadJdbcParameters(String jdbcUrl, String user, String passwd, String driverClassName) {
		super(jdbcUrl, user, passwd, driverClassName);
	}
	
	public FOption<String> selectExpr() {
		return m_selectExpr;
	}

	@Option(names={"-select"}, paramLabel="select_expr", description={"column selection"})
	public LoadJdbcParameters selectExpr(String expr) {
		m_selectExpr = FOption.ofNullable(expr);
		return this;
	}
	
	public FOption<String> wkbColumns() {
		return m_wkbCols;
	}

	@Option(names={"-wkb_cols"}, paramLabel="wkb_columns",
			description={"WKB column names for Geometry data"})
	public void wkbColumns(String cols) {
		m_wkbCols = FOption.ofNullable(cols);
	}
	
	public FOption<String> srid() {
		return m_srid;
	}

	@Option(names={"-srid"}, paramLabel="EPSG-code", description="EPSG code for input table")
	public LoadJdbcParameters srid(String srid) {
		m_srid = FOption.ofNullable(srid);
		return this;
	}
	
	public LoadJdbcParameters duplicate() {
		LoadJdbcParameters dupl = new LoadJdbcParameters(jdbcUrl(), user(), password(),
															jdbcDriverClassName());
		jdbcJarPath().ifPresent(dupl::jdbcJarPath);
		
		dupl.m_selectExpr = m_selectExpr;
		dupl.m_wkbCols = m_wkbCols;
		dupl.m_srid = m_srid;
		
		return dupl;
	}
	
	@Override
	public String toString() {
		String selectStr = m_selectExpr.map(s -> String.format("select=%s", s))
									.getOrElse("");
		return String.format("%s", selectStr);
	}
}
