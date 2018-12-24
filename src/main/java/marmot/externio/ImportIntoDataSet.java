package marmot.externio;

import static marmot.DataSetOption.BLOCK_SIZE;
import static marmot.DataSetOption.COMPRESS;
import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import java.util.List;
import java.util.Objects;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.command.ImportParameters;
import marmot.proto.optor.OperatorProto;
import marmot.rset.RecordSets;
import utils.Throwables;
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
		Objects.requireNonNull(params);

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
		
		try {
			String dsId = m_params.getDataSetId();
			
			FOption<Plan> importPlan = loadImportPlan(marmot)
											.map(this::adjustImportPlan);
			
			RecordSet rset0 = loadRecordSet(marmot);
			RecordSet rset = m_params.getReportInterval()
								.map(n -> (RecordSet)RecordSets.reportProgress(rset0, m_subject, n))
								.getOrElse(rset0);

			DataSet ds;
			if ( !append ) {
				List<DataSetOption> optList = Lists.newArrayList();
				m_params.getGeometryColumnInfo().ifPresent(info -> optList.add(GEOMETRY(info)));
				m_params.getBlockSize().ifPresent(sz -> optList.add(BLOCK_SIZE(sz)));
				m_params.getCompress().ifPresent(b -> optList.add(COMPRESS));
				
				if ( force ) {
					optList.add(FORCE);
				}
				DataSetOption[] opts = Iterables.toArray(optList, DataSetOption.class);
				
				RecordSchema outSchema = rset.getRecordSchema();
				if ( importPlan.isPresent() ) {
					outSchema = marmot.getOutputRecordSchema(importPlan.get(), outSchema);
				}
				ds = marmot.createDataSet(dsId, outSchema, opts);
			}
			else {
				ds = marmot.getDataSet(m_params.getDataSetId());
			}

			if ( importPlan.isPresent() ) {
				return ds.append(rset, importPlan.get());
			}
			else {
				return ds.append(rset);
			}
		}
		catch ( Throwable e ) {
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
