package marmot.externio.jdbc;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import marmot.Column;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.command.ImportParameters;
import marmot.externio.ImportIntoDataSet;
import marmot.rset.JdbcRecordAdaptor;
import marmot.rset.JdbcRecordSet;
import marmot.type.DataType;
import marmot.type.DataTypes;
import utils.CSV;
import utils.DelayedSplitter;
import utils.KeyValue;
import utils.func.FOption;
import utils.jdbc.JdbcProcessor;
import utils.stream.KVFStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportJdbcTable extends ImportIntoDataSet {
	protected final String m_tableName;
	protected final JdbcParameters m_jdbcParams;
	
	public static ImportJdbcTable from(String tableName, JdbcParameters jdbcParams,
										ImportParameters params) {
		return new ImportJdbcTable(tableName, jdbcParams, params);
	}

	private ImportJdbcTable(String tableName, JdbcParameters jdbcParams, ImportParameters params) {
		super(params);
		
		m_tableName = tableName;
		m_jdbcParams = jdbcParams.duplicate();
	}
	@Override
	protected RecordSet loadRecordSet(MarmotRuntime marmot) {
		try {
			JdbcProcessor jdbc = new JdbcProcessor(m_jdbcParams.jdbcUrl(), m_jdbcParams.user(),
													m_jdbcParams.password(),
													m_jdbcParams.jdbcDriverClassName());
			if ( m_jdbcParams.jdbcJarPath().isPresent() ) {
				String path = m_jdbcParams.jdbcJarPath().get();
				
				try {
					URL url = new URL(String.format("jar:file:%s!/", path));
					URLClassLoader cloader = new URLClassLoader(new URL[]{url});
					jdbc.setClassLoader(cloader);
				}
				catch ( MalformedURLException e ) {
					throw new RecordSetException("fails to create " + getClass()
										+ ", invalid jar path=" + m_jdbcParams.jdbcJarPath());
				}
			}
			
			RecordSchema schema = calcRecordSchema(jdbc);
			JdbcRecordAdaptor adaptor = new JdbcRecordAdaptor(schema);
			
			StringBuilder sqlBuilder = new StringBuilder("select ");
			String colsExpr = m_jdbcParams.selectExpr()
										.getOrElse(() -> schema.streamColumns()
																.map(Column::name)
																.join(","));
			sqlBuilder.append(colsExpr);
			sqlBuilder.append(" from ").append(m_tableName);
			ResultSet rs = jdbc.executeQuery(sqlBuilder.toString());

			return new JdbcRecordSet(adaptor, rs);
			
		}
		catch ( SQLException e ) {
			throw new RecordSetException("fails to create " + getClass(), e);
		}
	}

	@Override
	protected FOption<Plan> loadImportPlan(MarmotRuntime marmot) {
		return m_params.getGeometryColumnInfo().map(gcInfo -> {
				PlanBuilder builder = new PlanBuilder("import_jdbc_table");
				
				if ( m_jdbcParams.srid().isPresent() ) {
					String srcSrid = m_jdbcParams.srid().get();
					if ( !srcSrid.equals(gcInfo.srid()) ) {
						builder = builder.transformCrs(gcInfo.name(), srcSrid, gcInfo.srid());
					}
				}
				
				return builder.build();
			});
	}
	
	private RecordSchema calcRecordSchema(JdbcProcessor jdbc) throws SQLException {
		if ( m_jdbcParams.selectExpr().isAbsent() && m_jdbcParams.wkbColumns().isAbsent() ) {
			return KVFStream.from(jdbc.getColumns(m_tableName))
							.mapValue((k,v) -> JdbcRecordAdaptor.fromSqlType(v.type(), v.typeName()))
							.foldLeft(RecordSchema.builder(),
										(b,kv) -> b.addColumn(kv.key(), kv.value()))
							.build();
		}
		
		String selectExpr = m_jdbcParams.selectExpr().getOrElse("*");
		Map<String, DataType> wkbCols = m_jdbcParams.wkbColumns()
											.fstream()
											.flatMap(csv -> CSV.parseCsv(csv, ','))
											.map(str -> DelayedSplitter.splitIntoTwo(str, ':'))
											.map(arr -> KeyValue.of(arr[0], arr[1]))
											.map(kv -> KeyValue.of(kv.key(), DataTypes.fromName(kv.value())))
											.toMap(KeyValue::key, KeyValue::value);
		String sql = String.format("select %s from %s limit 1", selectExpr, m_tableName);
		ResultSet rs = jdbc.executeQuery(sql);
		ResultSetMetaData meta = rs.getMetaData();
		
		RecordSchema.Builder builder = RecordSchema.builder();
		for ( int i =1; i <= meta.getColumnCount(); ++i ) {
			String colName = meta.getColumnLabel(i);
			DataType type = JdbcRecordAdaptor.fromSqlType(meta.getColumnType(i),
														meta.getColumnName(i));
			DataType geomType = wkbCols.get(colName);
			if ( geomType != null ) {
				if ( type != DataType.BINARY ) {
					throw new IllegalArgumentException("invalid wtb column (not binary): name=" + colName);
				}
				builder.addColumn(colName, geomType);
			}
			else {
				builder.addColumn(meta.getColumnLabel(i), type);
			}
		}
		
		return builder.build();
	}
}
