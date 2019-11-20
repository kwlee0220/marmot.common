package marmot.externio.jdbc;

import java.util.Map;

import com.google.common.collect.Maps;

import picocli.CommandLine.Option;
import utils.CSV;
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
	
	private static final Map<String,String> JDBC_DRIVERS;
	static {
		JDBC_DRIVERS = Maps.newHashMap();
		JDBC_DRIVERS.put("postgresql", "org.postgresql.Driver");
	}
	
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

	@Option(names={"-jdbc_url"}, paramLabel="url", required=true,
			description={"JDBC connection URL"})
	public JdbcParameters jdbcUrl(String url) {
		m_jdbcUrl = url;
		
		if ( m_driverClassName == null ) {
			String protocol = CSV.parseCsv(m_jdbcUrl, ':')
								.take(2).findLast()
								.getOrThrow(() -> new IllegalArgumentException("jdbc_url=" + url));
			m_driverClassName = JDBC_DRIVERS.get(protocol);
		}
		
		return this;
	}
	
	public String user() {
		return m_user;
	}

	@Option(names={"-jdbc_user"}, paramLabel="user_id", required=true,
			description={"JDBC database user id"})
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

	@Option(names={"-jdbc_driver_class"}, paramLabel="class_name", description={"JDBC driver class name"})
	public JdbcParameters jdbcDriverClassName(String clsName) {
		m_driverClassName = clsName;
		return this;
	}
	
	public FOption<String> jdbcJarPath() {
		return m_jdbcJarPath;
	}

	@Option(names={"-jdbc_jar"}, paramLabel="jdbc_jar_path", required=false,
			description={"the path to JDBC driver jar"})
	public void jdbcJarPath(String jarPath) {
		m_jdbcJarPath = FOption.ofNullable(jarPath);
	}
	
	public JdbcParameters duplicate() {
		JdbcParameters dupl = new JdbcParameters(m_jdbcUrl, m_user, m_passwd, m_driverClassName);
		dupl.m_jdbcJarPath = m_jdbcJarPath;
		
		return dupl;
	}
	
	@Override
	public String toString() {
		return String.format("%s:user=%s", m_jdbcUrl, m_user);
	}
}
