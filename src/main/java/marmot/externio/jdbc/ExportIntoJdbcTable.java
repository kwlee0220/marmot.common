package marmot.externio.jdbc;

import java.io.File;
import java.io.IOException;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;

import utils.Preconditions;
import utils.func.FOption;
import utils.jdbc.JdbcProcessor;
import utils.rx.ProgressReporter;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportIntoJdbcTable implements ProgressReporter<Long> {
	private final String m_dsId;
	private final String m_tableName;
	private final StoreJdbcParameters m_params;
	private FOption<Long> m_reportInterval = FOption.empty();
	private final BehaviorSubject<Long> m_subject = BehaviorSubject.createDefault(0L);
	
	public ExportIntoJdbcTable(String dsId, String tblName, StoreJdbcParameters params) {
		Preconditions.checkNotNullArgument(dsId, "dataset id is null");
		Preconditions.checkNotNullArgument(tblName, "table-name is null");
		Preconditions.checkNotNullArgument(params, "JdbcParameters is null");
		
		m_dsId = dsId;
		m_tableName = tblName;
		m_params = params;
	}
	
	public ExportIntoJdbcTable reportInterval(long interval) {
		m_reportInterval = (interval > 0) ? FOption.of(interval) : FOption.empty();
		return this;
	}
	
	@Override
	public Observable<Long> getProgressObservable() {
		return m_subject;
	}
	
	public long run(MarmotRuntime marmot) throws IOException {
		Preconditions.checkNotNullArgument(marmot, "MarmotRuntime is null");

		JdbcProcessor.Builder jdbcBuilder = JdbcProcessor.builder()
															.system(m_params.system())
															.host(m_params.host())
															.port(m_params.port())
															.user(m_params.user())
															.password(m_params.password())
															.dbName(m_params.database());
		m_params.jdbcJarPath().map(File::new).ifPresent(jdbcBuilder::jarFile);
		JdbcProcessor jdbc = jdbcBuilder.build();

		try ( RecordSet rset = loadRecordSet(marmot);
				JdbcRecordSetWriter writer = new JdbcRecordSetWriter(m_tableName, jdbc,
																	m_params.geometryFormat()) ) {
			return writer.write(rset);
		}
	}
	
	private RecordSet loadRecordSet(MarmotRuntime marmot) {
		PlanBuilder builder = Plan.builder("load_for_jdbc_export")
									.load(m_dsId);
		
		RecordSet rset = marmot.executeLocally(builder.build());
		return m_reportInterval.transform(rset, (r,intvl) -> r.reportProgress(m_subject,intvl));
	}
}
