package marmot.externio.csv;

import java.io.BufferedWriter;
import java.io.IOException;

import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import io.vavr.Tuple2;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.optor.StoreAsCsvOptions;
import marmot.type.DataType;
import utils.Utilities;
import utils.async.ProgressReporter;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportAsCsv implements ProgressReporter<Long> {
	private final String m_dsId;
	private FOption<Long> m_reportInterval = FOption.empty();
	private final CsvParameters m_options;
	private final BehaviorSubject<Long> m_subject = BehaviorSubject.createDefault(0L);
	
	public ExportAsCsv(String dsId, CsvParameters options) {
		Utilities.checkNotNullArgument(dsId, "dataset id is null");
		Utilities.checkNotNullArgument(options, "CsvOptions is null");
		
		m_dsId = dsId;
		m_options = options;
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
		
		StoreAsCsvOptions storeOpts = m_options.toStoreOptions();
		try ( RecordSet rset = locateRecordSet(marmot);
			CsvRecordSetWriter csvWriter = CsvRecordSetWriter.get(writer, storeOpts) ) {
			return csvWriter.write(rset);
		}
	}
	
	private RecordSet locateRecordSet(MarmotRuntime marmot) {
		PlanBuilder builder = marmot.planBuilder("export_csv")
									.load(m_dsId);
		
		DataSet ds = marmot.getDataSet(m_dsId);
		GeometryColumnInfo geomColInfo = ds.hasGeometryColumn()
										? ds.getGeometryColumnInfo() : null;
		if ( geomColInfo != null ) {
			String geomCol = geomColInfo.name();
			String srid = geomColInfo.srid();
			
			if ( m_options.srid().isPresent() ) {
				String csvSrid = m_options.srid().get();
				if ( !csvSrid.equals(srid) ) {
					builder = builder.transformCrs(geomCol, srid, csvSrid);
				}
			}

			DataType geomType = ds.getRecordSchema().getColumn(geomCol).type();
			if ( m_options.pointColumns().isPresent() ) {
				if ( geomType != DataType.POINT ) {
					throw new IllegalArgumentException("geometry is not POINT type, but " + geomType);
				}
				Tuple2<String,String> pointCol = m_options.pointColumns().get();
				builder = builder.toXY(geomCol, pointCol._1, pointCol._2);
			}
			else if ( m_options.tiger() ) {
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
