package marmot.externio;

import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.command.ImportParameters;
import marmot.proto.optor.OperatorProto;
import utils.Throwables;
import utils.Utilities;
import utils.async.ProgressReporter;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ImportIntoDataSet implements ProgressReporter<Long> {
	protected final ImportParameters m_params;
	private final BehaviorSubject<Long> m_subject = BehaviorSubject.create();
	
	protected abstract RecordSet loadRecordSet(MarmotRuntime marmot);
	protected abstract FOption<Plan> loadImportPlan(MarmotRuntime marmot);
	
	public ImportIntoDataSet(ImportParameters params) {
		Utilities.checkNotNullArgument(params, "params is null");
		Utilities.checkNotNullArgument(params.getDataSetId(), "params.dataset_id is null");

		m_params = params;
	}
	
	@Override
	public Observable<Long> getProgressObservable() {
		return m_subject;
	}
	
	public long run(MarmotRuntime marmot) {
		boolean force = m_params.getForce();
		boolean append = m_params.getAppend();
		if ( force && append ) {
			throw new IllegalArgumentException("Both 'force' and 'append' cannot be set simultaneously");
		}
		
		String dsId = m_params.getDataSetId();
		try {
			FOption<Plan> importPlan = loadImportPlan(marmot)
											.map(this::adjustImportPlan);
			
			RecordSet rset0 = loadRecordSet(marmot);
			RecordSet rset = m_params.getReportInterval()
									.transform(rset0,
											(r,n) -> r.reportProgress(m_subject).reportInterval(n));

			DataSet ds;
			if ( !append ) {
				RecordSchema outSchema = importPlan.transform(rset.getRecordSchema(),
														(s,p) -> marmot.getOutputRecordSchema(p,s));
				ds = marmot.createDataSet(dsId, outSchema, m_params.toOptions());
			}
			else {
				ds = marmot.getDataSet(dsId);
			}
			
			return importPlan.map(p -> ds.append(rset, p))
							.getOrElse(() -> ds.append(rset));
		}
		catch ( Throwable e ) {
			if ( !append ) {
				marmot.deleteDataSet(dsId);
			}
			
			throw Throwables.toRuntimeException(e);
		}
	}
	
	private Plan adjustImportPlan(Plan plan) {
		OperatorProto last = plan.getLastOperator().get();
		switch ( last.getOperatorCase() ) {
			case STORE_INTO_DATASET:
				break;
			default:
				plan = plan.toBuilder()
							.store(m_params.getDataSetId())
							.build();
				break;
		}
		
		return plan;
	}
}
