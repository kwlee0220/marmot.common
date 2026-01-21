package marmot.externio;

import static marmot.optor.StoreDataSetOptions.APPEND;

import java.util.Optional;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;

import utils.Throwables;
import utils.Utilities;
import utils.rx.ProgressReporter;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSets.CountingRecordSet;
import marmot.command.ImportParameters;
import marmot.dataset.DataSet;
import marmot.optor.StoreDataSetOptions;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ImportIntoDataSet implements ProgressReporter<Long> {
	protected final ImportParameters m_params;
	private final BehaviorSubject<Long> m_subject = BehaviorSubject.create();
	
	protected abstract RecordSet loadRecordSet(MarmotRuntime marmot);
	protected abstract Optional<Plan> loadImportPlan(MarmotRuntime marmot);
	
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
			Optional<Plan> importPlan = loadImportPlan(marmot);
			
			RecordSet rset0 = loadRecordSet(marmot);
			RecordSet rset = rset0;
			if ( m_params.getReportInterval().isPresent() ) {
				rset = rset0.reportProgress(m_subject, m_params.getReportInterval().get());
			}
			
			DataSet ds;
			if ( append ) {
				ds = marmot.getDataSet(dsId);
			}
			else {
				RecordSchema outSchema = rset.getRecordSchema();
				if ( importPlan.isPresent() )  {
					outSchema = marmot.getOutputRecordSchema(importPlan.get(), rset.getRecordSchema());
				}
				ds = marmot.createDataSet(dsId, outSchema, m_params.toCreateOptions());
			}
			
			if ( importPlan.isPresent() ) {
				PlanBuilder builder = importPlan.get().toBuilder();
				switch ( builder.getLastOperatorProto().getOperatorCase() ) {
					case STORE_DATASET:
						throw new IllegalArgumentException("import-plan should not be "
														+ "a store operator: plan=" + importPlan);
					default:
				}

				StoreDataSetOptions opts = APPEND.blockSize(ds.getBlockSize());
				if ( ds.getCompressionCodecName().isPresent() ) {
					opts = opts.compressionCodecName(ds.getCompressionCodecName().get());
				}
				Plan adjusted = builder.store(dsId, opts)
										.build();	
				try ( CountingRecordSet countingRSet = rset.asCountingRecordSet() ) {
					marmot.executeLocally(adjusted, countingRSet);
					return countingRSet.getCount();
				}
			}
			else {
				return ds.append(rset);
			}
		}
		catch ( Throwable e ) {
			if ( !append ) {
				marmot.deleteDataSet(dsId);
			}
			
			throw Throwables.toRuntimeException(e);
		}
	}
}
