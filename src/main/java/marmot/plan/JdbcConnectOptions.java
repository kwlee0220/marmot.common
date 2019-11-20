package marmot.plan;

import java.io.File;

import javax.annotation.Nullable;

import marmot.proto.optor.JdbcConnectOptionsProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcConnectOptions implements PBSerializable<JdbcConnectOptionsProto> {
	private final String m_jdbcUrl;
	private final String m_user;
	private final String m_passwd;
	private final String m_driverClassName;
	
	public JdbcConnectOptions(String jdbcUrl, String user, String passwd, String driverClassName) {
		m_jdbcUrl = jdbcUrl;
		m_user = user;
		m_passwd = passwd;
		m_driverClassName = driverClassName;
	}
	
	public static JdbcConnectOptions POSTGRES_SQL(String host, int port, String dbName,
													String user, String passwd, @Nullable File jarFile) {
		String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, dbName);
		return new JdbcConnectOptions(url, user, passwd, "org.postgresql.Driver");
	}
	
	public String jdbcUrl() {
		return m_jdbcUrl;
	}
	
	public JdbcConnectOptions jdbcUrl(String url) {
		return new JdbcConnectOptions(url, m_user, m_passwd, m_driverClassName);
	}
	
	public String user() {
		return m_user;
	}
	
	public JdbcConnectOptions user(String user) {
		return new JdbcConnectOptions(m_jdbcUrl, user, m_passwd, m_driverClassName);
	}
	
	public String passwd() {
		return m_passwd;
	}
	
	public JdbcConnectOptions passwd(String passwd) {
		return new JdbcConnectOptions(m_jdbcUrl, m_user, passwd, m_driverClassName);
	}
	
	public String driverClassName() {
		return m_driverClassName;
	}
	
	public JdbcConnectOptions driverClassName(String clsName) {
		return new JdbcConnectOptions(m_jdbcUrl, m_user, m_passwd, clsName);
	}

	public static JdbcConnectOptions fromProto(JdbcConnectOptionsProto proto) {
		return new JdbcConnectOptions(proto.getJdbcUrl(), proto.getUser(), proto.getPasswd(),
										proto.getDriverClassName());
	}

	@Override
	public JdbcConnectOptionsProto toProto() {
		return JdbcConnectOptionsProto.newBuilder()
									.setJdbcUrl(m_jdbcUrl)
									.setUser(m_user)
									.setPasswd(m_passwd)
									.setDriverClassName(m_driverClassName)
									.build();
	}
}
