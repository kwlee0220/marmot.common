package marmot.plan;

import marmot.proto.optor.JdbcConnectOptionsProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcConnectOptions implements PBSerializable<JdbcConnectOptionsProto> {
	private String m_jdbcUrl;
	private String m_user;
	private String m_passwd;
	private String m_driverClassName;
	
	public static JdbcConnectOptions create() {
		return new JdbcConnectOptions();
	}
	
	public String jdbcUrl() {
		return m_jdbcUrl;
	}
	
	public JdbcConnectOptions jdbcUrl(String url) {
		m_jdbcUrl = url;
		return this;
	}
	
	public String user() {
		return m_user;
	}
	
	public JdbcConnectOptions user(String user) {
		m_user = user;
		return this;
	}
	
	public String passwd() {
		return m_passwd;
	}
	
	public JdbcConnectOptions passwd(String passwd) {
		m_passwd = passwd;
		return this;
	}
	
	public String driverClassName() {
		return m_driverClassName;
	}
	
	public JdbcConnectOptions driverClassName(String clsName) {
		m_driverClassName = clsName;
		return this;
	}

	public static JdbcConnectOptions fromProto(JdbcConnectOptionsProto proto) {
		return JdbcConnectOptions.create()
									.jdbcUrl(proto.getJdbcUrl())
									.user(proto.getUser())
									.passwd(proto.getPasswd())
									.driverClassName(proto.getDriverClassName());
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
