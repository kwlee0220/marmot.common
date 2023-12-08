package marmot.externio.csv;

import java.io.BufferedWriter;
import java.io.IOException;

import utils.Utilities;
import utils.func.FOption;
import utils.func.Tuple;
import utils.rx.ProgressReporter;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.type.DataType;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportAsCsv implements ProgressReporter<Long> {
	private final String m_dsId;
	private FOption<Long> m_reportInterval = FOption.empty();
	private final CsvParameters m_params;
	private final BehaviorSubject<Long> m_subject = BehaviorSubject.createDefault(0L);
	
	public ExportAsCsv(String dsId, CsvParameters params) {
		Utilities.checkNotNullArgument(dsId, "dataset id is null");
		Utilities.checkNotNullArgument(params, "CsvParameters is null");
		
		m_dsId = dsId;
		m_params = params;
	}
	
	public ExportAsCsv reportInterval(long interval) {
		m_reportInterval = (interval > 0) ? FOption.of(interval) : FOption.empty();
		return this;
	}
	
	@Override
	public Observable<Long> getProgressObservable() {
		return m_subject;
	}
	
	public long run(MarmotRuntime marmot, BufferedWriter writer) throws IOException {
		Utilities.checkNotNullArgument(marmot, "MarmotRuntime is null");
		Utilities.checkNotNullArgument(writer, "writer is null");
		
		return CsvRecordWriter.write(writer, locateRecordSet(marmot), m_params.toCsvOptions());
	}
	
	private RecordSet locateRecordSet(MarmotRuntime marmot) {
		PlanBuilder builder = Plan.builder("export_csv")
									.load(m_dsId);
		
		DataSet ds = marmot.getDataSet(m_dsId);
		GeometryColumnInfo geomColInfo = ds.hasGeometryColumn()
										? ds.getGeometryColumnInfo() : null;
		if ( geomColInfo != null ) {
			String geomCol = geomColInfo.name();
			String srid = geomColInfo.srid();
			
			if ( m_params.srid().isPresent() ) {
				String csvSrid = m_params.srid().get();
				if ( !csvSrid.equals(srid) ) {
					builder = builder.transformCrs(geomCol, srid, csvSrid);
				}
			}

			DataType geomType = ds.getRecordSchema().getColumn(geomCol).type();
			if ( m_params.pointColumns().isPresent() ) {
				if ( geomType != DataType.POINT ) {
					throw new IllegalArgumentException("geometry is not POINT type, but " + geomType);
				}
				Tuple<String,String> pointCol = m_params.pointColumns().get();
				builder = builder.toXY(geomCol, pointCol._1, pointCol._2);
			}
			else if ( m_params.tiger() ) {
				String decl = String.format("%s:string", geomCol);
				String initExpr = String.format("ST_AsHexString(%s)", geomCol);
				builder = builder.defineColumn(decl, initExpr);
				builder = builder.project(String.format("%s,*-{%s}", geomCol, geomCol));
			}
		}
		
		RecordSet rset = marmot.executeLocally(builder.build());
		return m_reportInterval.transform(rset, (r,intvl) -> r.reportProgress(m_subject,intvl));
	}
}
