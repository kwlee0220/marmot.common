package marmot.externio.jdbc;

import marmot.externio.csv.CsvParameters;
import marmot.plan.JdbcConnectOptions;
import picocli.CommandLine.Option;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcParameters {
	private String m_jdbcUrl;
	private String m_user;
	private String m_passwd;
	private String m_driverClassName;
	private FOption<String> m_jdbcJarPath = FOption.empty();
	private FOption<String> m_selectExpr = FOption.empty();
	private FOption<String> m_wkbCols = FOption.empty();
	private FOption<String> m_srid = FOption.empty();
	
	public JdbcParameters() { }
	
	public JdbcParameters(String jdbcUrl, String user, String passwd, String driverClassName) {
		m_jdbcUrl = jdbcUrl;
		m_user = user;
		m_passwd = passwd;
		m_driverClassName = driverClassName;
	}
	
	public String jdbcUrl() {
		return m_jdbcUrl;
	}

	@Option(names={"-jdbc_url"}, paramLabel="url", required=true, description={"JDBC connection URL"})
	public JdbcParameters jdbcUrl(String url) {
		m_jdbcUrl = url;
		return this;
	}
	
	public String user() {
		return m_user;
	}

	@Option(names={"-jdbc_user"}, paramLabel="user_id", required=true, description={"JDBC database user id"})
	public JdbcParameters user(String userId) {
		m_user = userId;
		return this;
	}
	
	public String password() {
		return m_passwd;
	}

	@Option(names={"-jdbc_passwd"}, paramLabel="user_passwd", required=true,
			description={"JDBC database user password"})
	public JdbcParameters password(String passwd) {
		m_passwd = passwd;
		return this;
	}
	
	public String jdbcDriverClassName() {
		return m_driverClassName;
	}

	@Option(names={"-jdbc_driver_class"}, paramLabel="class_name", required=true,
			description={"JDBC driver class name"})
	public JdbcParameters jdbcDriverClassName(String clsName) {
		m_driverClassName = clsName;
		return this;
	}
	
	public FOption<String> selectExpr() {
		return m_selectExpr;
	}

	@Option(names={"-select"}, paramLabel="select_expr", description={"column selection"})
	public JdbcParameters selectExpr(String expr) {
		m_selectExpr = FOption.ofNullable(expr);
		return this;
	}
	
	public FOption<String> jdbcJarPath() {
		return m_jdbcJarPath;
	}

	@Option(names={"-jdbc_jar"}, paramLabel="jdbc_jar_path", required=true,
			description={"the path to JDBC driver jar"})
	public void jdbcJarPath(String jarPath) {
		m_jdbcJarPath = FOption.ofNullable(jarPath);
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
	public JdbcParameters srid(String srid) {
		m_srid = FOption.ofNullable(srid);
		return this;
	}
	
	public JdbcConnectOptions toOptions() {
		return new JdbcConnectOptions(m_jdbcUrl, m_user, m_passwd, m_driverClassName);
	}
	
	public JdbcParameters duplicate() {
		JdbcParameters dupl = new JdbcParameters(m_jdbcUrl, m_user, m_passwd, m_driverClassName);
		dupl.m_selectExpr = m_selectExpr;
		dupl.m_jdbcJarPath = m_jdbcJarPath;
		
		return dupl;
	}
	
	@Override
	public String toString() {
		String selectStr = m_selectExpr.map(s -> String.format("select=%s", s))
									.getOrElse("");
		return String.format("%s", selectStr);
	}
}
