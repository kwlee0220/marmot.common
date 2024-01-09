package marmot.externio.jdbc;

import java.io.File;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import utils.func.FOption;
import utils.func.Funcs;
import utils.jdbc.JdbcProcessor;

import marmot.Column;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.command.ImportParameters;
import marmot.externio.ImportIntoDataSet;
import marmot.support.MetaPlanLoader;
import marmot.type.DataType;


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
			JdbcProcessor jdbc = JdbcProcessor.create(m_jdbcParams.system(), m_jdbcParams.host(),
													m_jdbcParams.port(), m_jdbcParams.user(),
													m_jdbcParams.password(), m_jdbcParams.database());
			m_jdbcParams.jdbcJarPath().map(File::new).ifPresent(jdbc::setJdbcJarFile);
			RecordSchema schema = buildRecordSchema(marmot, jdbc);
			JdbcRecordAdaptor adaptor = JdbcRecordAdaptor.create(jdbc, schema, m_jdbcParams.geometryFormat());
			
			StringBuilder sqlBuilder = new StringBuilder("select ");
			String colsExpr = m_jdbcParams.selectExpr()
										.getOrElse(() -> schema.streamColumns()
																.map(Column::name)
																.join(","));
			sqlBuilder.append(colsExpr);
			sqlBuilder.append(" from ").append(m_tableName);
			String sql = sqlBuilder.toString();
			ResultSet rs = ( m_jdbcParams.fetchSize() > 0 )
						? jdbc.executeQuery(sql, stmt -> stmt.setFetchSize(m_jdbcParams.fetchSize()))
						: jdbc.executeQuery(sql);
			return new JdbcRecordSet(adaptor, rs);
			
		}
		catch ( SQLException e ) {
			throw new RecordSetException("fails to create " + getClass(), e);
		}
	}

	@Override
	protected FOption<Plan> loadImportPlan(MarmotRuntime marmot) {
		FOption<Plan> toGeomPlan = m_params.getGeometryColumnInfo().map(gcInfo -> {
				PlanBuilder builder = new PlanBuilder("import_jdbc_table");
				
				if ( m_jdbcParams.srid().isPresent() ) {
					String srcSrid = m_jdbcParams.srid().get();
					if ( !srcSrid.equals(gcInfo.srid()) ) {
						builder = builder.transformCrs(gcInfo.name(), srcSrid, gcInfo.srid());
					}
				}
				
				return builder.build();
			});
		
		// 추가 작업 플랜이 있는 경우, 이를 반영할 경우의 레코드 스키마를 계산한다.
		FOption<Plan> importPlan = m_jdbcParams.plan()
												.flatMapSneakily(file -> MetaPlanLoader.load(file));
		if ( importPlan.isAbsent() ) {
			return toGeomPlan;
		}
		else if ( toGeomPlan.isAbsent() ) {
			return importPlan;
		}
		else {
			return FOption.of(Plan.concat(toGeomPlan.get(), importPlan.get()));
		}
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
	
	private RecordSchema buildRecordSchema(MarmotRuntime marmot, JdbcProcessor jdbc)
		throws SQLException {
		RecordSchema schema = m_jdbcParams.selectExpr()
					.mapOrThrow(expr -> buildRecordSchemaFromSelectExpr(jdbc, m_tableName, expr))
					.getOrElseThrow(() -> JdbcRecordAdaptor.buildRecordSchema(jdbc, m_tableName));
		
		// 공간정보 관련 인자가 있는 경우, 공간 정보를 추가한다.
		return m_jdbcParams.geomColumns()
							.transform(schema, this::replaceWkbWithGeometryType);
	}
	
	private RecordSchema replaceWkbWithGeometryType(RecordSchema schema,
													Map<String,DataType> geomCols) {
		return schema.streamColumns()
						.map(col -> {
							DataType type = geomCols.get(col.name());
							type = Funcs.asNonNull(type,  col.type());
							return new Column(col.name(), type);
						})
						.fold(RecordSchema.builder(), (b,c) -> b.addColumn(c))
						.build();
	}
}
