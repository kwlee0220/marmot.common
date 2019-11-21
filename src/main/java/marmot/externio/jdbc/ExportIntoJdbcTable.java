package marmot.externio.jdbc;

import java.io.File;
import java.io.IOException;

import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.RecordSet;
import utils.Utilities;
import utils.async.ProgressReporter;
import utils.func.FOption;
import utils.jdbc.JdbcProcessor;


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
		Utilities.checkNotNullArgument(dsId, "dataset id is null");
		Utilities.checkNotNullArgument(tblName, "table-name is null");
		Utilities.checkNotNullArgument(params, "JdbcParameters is null");
		
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
		Utilities.checkNotNullArgument(marmot, "MarmotRuntime is null");

		JdbcProcessor jdbc = JdbcProcessor.create(m_params.system(), m_params.host(),
													m_params.port(), m_params.user(),
													m_params.password(), m_params.database());
		m_params.jdbcJarPath().map(File::new).ifPresent(jdbc::setJdbcJarFile);

		try ( RecordSet rset = loadRecordSet(marmot);
				JdbcRecordSetWriter writer = new JdbcRecordSetWriter(m_tableName, jdbc,
																	m_params.geometryFormat()) ) {
			return writer.write(rset);
		}
	}
	
	private RecordSet loadRecordSet(MarmotRuntime marmot) {
		PlanBuilder builder = marmot.planBuilder("load_for_jdbc_export")
									.load(m_dsId);
		
		RecordSet rset = marmot.executeLocally(builder.build());
		return m_reportInterval.transform(rset, (r,intvl) -> r.reportProgress(m_subject,intvl));
	}
}
