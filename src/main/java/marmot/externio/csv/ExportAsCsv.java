package marmot.externio.csv;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Objects;

import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.externio.ExternIoUtils;
import marmot.rset.RecordSets;
import marmot.type.DataType;
import utils.CommandLine;
import utils.async.ProgressReporter;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportAsCsv implements ProgressReporter<Long> {
	private final String m_dsId;
	private Option<Long> m_reportInterval = Option.none();
	private final CsvParameters m_csvParams;
	private final BehaviorSubject<Long> m_subject = BehaviorSubject.create();
	
	public ExportAsCsv(String dsId, CsvParameters csvParams) {
		Objects.requireNonNull(dsId, "dataset id is null");
		Objects.requireNonNull(csvParams, "CsvParameters is null");
		
		m_dsId = dsId;
		m_csvParams = csvParams;
	}
	
	public ExportAsCsv reportInterval(long interval) {
		m_reportInterval = (interval > 0) ? Option.some(interval) : Option.none();
		return this;
	}
	
	@Override
	public Observable<Long> getProgressObservable() {
		return m_subject;
	}
	
	public long run(MarmotRuntime marmot, BufferedWriter writer) throws IOException {
		Objects.requireNonNull(marmot, "MarmotRuntime is null");
		Objects.requireNonNull(writer, "writer is null");
		
		try ( RecordSet rset = locateRecordSet(marmot);
			CsvRecordSetWriter csvWriter = CsvRecordSetWriter.get(writer, m_csvParams) ) {
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
			
			if ( m_csvParams.csvSrid().isDefined() ) {
				String csvSrid = m_csvParams.csvSrid().get();
				if ( !csvSrid.equals(srid) ) {
					builder = builder.transformCrs(geomCol, srid, csvSrid);
				}
			}

			DataType geomType = ds.getRecordSchema().getColumn(geomCol).type();
			if ( m_csvParams.pointColumn().isDefined() && geomType == DataType.POINT ) {
				Tuple2<String,String> pointCol = m_csvParams.pointColumn().get();
				builder = builder.toXYCoordinates(geomCol, pointCol._1, pointCol._2);
			}
			else if ( m_csvParams.tiger() ) {
				String decl = String.format("%s:string", geomCol);
				String initExpr = String.format("ST_AsHexString(%s)", geomCol);
				builder = builder.expand1(decl, initExpr);
				builder = builder.project(String.format("%s,*-{%s}", geomCol, geomCol));
			}
		}
		
		RecordSet rset = marmot.executeLocally(builder.build());
		if ( m_reportInterval.isDefined() ) {
			rset = RecordSets.reportProgress(rset, m_subject, m_reportInterval.get());
		}
		
		return rset;
	}
	
	public static final long run(MarmotRuntime marmot, CommandLine cl) throws Exception {
		CsvParameters csvParams = CsvParameters.create()
												.headerFirst(cl.hasOption("header_first"))
												.tiger(cl.hasOption("tiger"));
		cl.getOptionString("delim").map(s -> s.trim().charAt(0))
									.forEach(csvParams::delimiter);
		cl.getOptionString("quote").map(s -> s.trim().charAt(0))
									.forEach(csvParams::quote);
		cl.getOptionString("charset").map(Charset::forName).forEach(csvParams::charset);
		cl.getOptionString("point_col").forEach(csvParams::pointColumn);
		cl.getOptionString("csv_srid").forEach(csvParams::csvSrid);

		String dsId = cl.getArgument(0);
		ExportAsCsv export = new ExportAsCsv(dsId, csvParams);
		
		Option<String> output = cl.getOptionString("output");
		BufferedWriter writer = ExternIoUtils.toWriter(output, csvParams.charset());
		return export.run(marmot, writer);
	}
}
