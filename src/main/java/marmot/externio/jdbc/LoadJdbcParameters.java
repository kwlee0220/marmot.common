package marmot.externio.jdbc;

import java.util.Map;

import marmot.type.DataType;
import marmot.type.DataTypes;
import picocli.CommandLine.Option;
import utils.CSV;
import utils.LazySplitter;
import utils.func.FOption;
import utils.func.KeyValue;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadJdbcParameters extends JdbcParameters {
	private FOption<String> m_selectExpr = FOption.empty();
	private FOption<Map<String,DataType>> m_geomCols = FOption.empty();
	private FOption<String> m_srid = FOption.empty();
	
	public LoadJdbcParameters() { }
	
	public LoadJdbcParameters(String system, String host, int port, String user, String passwd,
								String dbName) {
		super(system, host, port, user, passwd, dbName);
	}
	
	public FOption<String> selectExpr() {
		return m_selectExpr;
	}

	@Option(names={"-select"}, paramLabel="select_expr", description={"column selection"})
	public LoadJdbcParameters selectExpr(String expr) {
		m_selectExpr = FOption.ofNullable(expr);
		return this;
	}
	
	public FOption<Map<String,DataType>> geomColumns() {
		return m_geomCols;
	}

	@Option(names={"-geom_cols"}, paramLabel="column_names_csv",
			description={"geometry column names for Geometry data (eg. 'col1:multi_polygon,col2:point'"})
	public LoadJdbcParameters geomColumns(String cols) {
		Map<String,DataType> geomCols = CSV.parseCsv(cols, ',')
											.map(decl -> LazySplitter.parseKeyValue(decl, ':'))
											.toKeyValueStream(KeyValue::key, KeyValue::value)
											.mapValue(DataTypes::fromName)
											.toMap();
		m_geomCols = FOption.of(geomCols);
		return this;
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
		LoadJdbcParameters dupl = new LoadJdbcParameters(system(), host(), port(), user(),
															password(), database());
		jdbcJarPath().ifPresent(dupl::jdbcJarPath);
		
		dupl.m_selectExpr = m_selectExpr;
		dupl.m_geomCols = m_geomCols;
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
