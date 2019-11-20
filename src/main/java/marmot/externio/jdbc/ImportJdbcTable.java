package marmot.externio.jdbc;

import java.io.File;
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
import marmot.type.DataType;
import marmot.type.DataTypes;
import utils.CSV;
import utils.LazySplitter;
import utils.func.FOption;
import utils.func.Funcs;
import utils.func.KeyValue;
import utils.jdbc.JdbcProcessor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportJdbcTable extends ImportIntoDataSet {
	protected final String m_tableName;
	protected final LoadJdbcParameters m_jdbcParams;
	
	public static ImportJdbcTable from(String tableName, LoadJdbcParameters jdbcParams,
										ImportParameters params) {
		return new ImportJdbcTable(tableName, jdbcParams, params);
	}

	private ImportJdbcTable(String tableName, LoadJdbcParameters jdbcParams, ImportParameters params) {
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
			m_jdbcParams.jdbcJarPath().map(File::new).ifPresent(jdbc::setJdbcJarFile);
			RecordSchema schema = buildRecordSchema(jdbc);
			JdbcRecordAdaptor adaptor = JdbcRecordAdaptor.create(jdbc, schema);
			
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
	
	private RecordSchema buildRecordSchemaFromSelectExpr(JdbcProcessor jdbc, String tblName,
														String selectExpr) throws SQLException {
		String sql = String.format("select %s from %s limit 1", selectExpr, tblName);
		ResultSet rs = jdbc.executeQuery(sql);
		ResultSetMetaData meta = rs.getMetaData();
		
		RecordSchema.Builder builder = RecordSchema.builder();
		for ( int i =1; i <= meta.getColumnCount(); ++i ) {
			DataType type = JdbcRecordAdaptor.fromSqlType(meta.getColumnType(i),
														meta.getColumnName(i));
			builder.addColumn(meta.getColumnLabel(i), type);
		}
		
		return builder.build();
	}
	
	private RecordSchema buildRecordSchema(JdbcProcessor jdbc) throws SQLException {
		RecordSchema schema = m_jdbcParams.selectExpr()
					.mapOrThrow(expr -> buildRecordSchemaFromSelectExpr(jdbc, m_tableName, expr))
					.getOrElseThrow(() -> JdbcRecordAdaptor.buildRecordSchema(jdbc, m_tableName));
		
		return m_jdbcParams.wkbColumns()
							.transform(schema, this::replaceWkbWithGeometryType);
	}
	
	private RecordSchema replaceWkbWithGeometryType(RecordSchema schema, String wkbColumns) {
		Map<String, DataType> wkbCols = m_jdbcParams.wkbColumns().fstream()
													.flatMap(csv -> CSV.parseCsv(csv, ','))
													.map(str -> LazySplitter.splitIntoKeyValue(str, ':'))
													.toKeyValueStream(KeyValue::key, KeyValue::value)
													.mapValue(DataTypes::fromName)
													.toMap();
		return schema.streamColumns()
					.map(col -> {
						DataType type = wkbCols.get(col.name());
						type = Funcs.getIfNotNull(type,  col.type());
						return new Column(col.name(), type);
					})
					.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
					.build();
	}
}
