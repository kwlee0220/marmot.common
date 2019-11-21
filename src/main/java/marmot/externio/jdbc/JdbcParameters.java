package marmot.externio.jdbc;

import picocli.CommandLine.Option;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcParameters {
	private String m_system;
	private String m_host;
	private int m_port;
	private String m_user;
	private String m_passwd;
	private String m_dbName;
	private GeometryFormat m_geomFormat = GeometryFormat.NATIVE;
	private FOption<String> m_jdbcJarPath = FOption.empty();
	
	public JdbcParameters() { }
	
	public JdbcParameters(String system, String host, int port, String user, String passwd, String dbName) {
		m_system = system;
		m_host = host;
		m_port = port;
		m_user = user;
		m_passwd = passwd;
		m_dbName = dbName;
	}
	
	public String system() {
		return m_system;
	}

	@Option(names={"-system"}, paramLabel="name", required=true,
			description={"JDBC driver system name (eg. postgresql, mysql, ...)"})
	public JdbcParameters system(String system) {
		m_system = system;
		return this;
	}
	
	public String host() {
		return m_host;
	}

	@Option(names={"-host"}, paramLabel="ip", required=true, description={"JDBC host name"})
	public JdbcParameters host(String host) {
		m_host = host;
		return this;
	}
	
	public int port() {
		return m_port;
	}

	@Option(names={"-port"}, paramLabel="port_no", required=true, description={"JDBC port number"})
	public JdbcParameters port(int port) {
		m_port = port;
		return this;
	}
	
	public String user() {
		return m_user;
	}

	@Option(names={"-user"}, paramLabel="user_id", required=true,
			description={"JDBC database user id"})
	public JdbcParameters user(String userId) {
		m_user = userId;
		return this;
	}
	
	public String password() {
		return m_passwd;
	}

	@Option(names={"-passwd"}, paramLabel="user_passwd", required=true,
					description={"JDBC database user password"})
	public JdbcParameters password(String passwd) {
		m_passwd = passwd;
		return this;
	}
	
	public String database() {
		return m_dbName;
	}

	@Option(names={"-database"}, paramLabel="name", required=true,
			description={"JDBC database name"})
	public JdbcParameters database(String name) {
		m_dbName = name;
		return this;
	}
	
	public GeometryFormat geometryFormat() {
		return m_geomFormat;
	}

	@Option(names={"-geom_format"}, paramLabel="geometry_format",
			description={"data format for geometry column"})
	public JdbcParameters geometryFormat(String format) {
		m_geomFormat = GeometryFormat.valueOf(format.toUpperCase());
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
		JdbcParameters dupl = new JdbcParameters(m_system, m_host, m_port, m_user, m_passwd, m_dbName);
		dupl.m_geomFormat = m_geomFormat;
		dupl.m_jdbcJarPath = m_jdbcJarPath;
		
		return dupl;
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s:%d, user=%s, db=%s]", m_system, m_host, m_port, m_user, m_dbName);
	}
}
