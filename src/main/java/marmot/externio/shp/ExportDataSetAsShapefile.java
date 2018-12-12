package marmot.externio.shp;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Objects;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import utils.CommandLine;
import utils.UnitUtils;
import utils.async.ProgressiveExecution;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportDataSetAsShapefile extends ExportAsShapefile {
	private static final Charset DEFAULT_CS = Charset.forName("utf-8");
	
	private final String m_dsId;
	
	public static ExportDataSetAsShapefile create(String dsId, String output, ShapefileParameters params) {
		return new ExportDataSetAsShapefile(dsId, output, params);
	}
	
	public ExportDataSetAsShapefile(String dsId, String outputDir, ShapefileParameters params) {
		super(outputDir, params);
		Objects.requireNonNull(dsId, "dataset id is null");
		
		m_dsId = dsId;
	}
	
	public ProgressiveExecution<Long,Long> start(MarmotRuntime marmot) throws IOException {
		DataSet ds = marmot.getDataSet(m_dsId);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();
		
		return start(ds.read(), info.srid());
	}
	
	public static final long run(MarmotRuntime marmot, CommandLine cl) throws Exception {
		Charset charset = cl.getOptionString("charset")
							.map(Charset::forName)
							.getOrElse(DEFAULT_CS);
		FOption<Integer> splitSize = cl.getOptionString("split_size")
										.map(UnitUtils::parseByteSize)
										.map(sz -> sz.intValue());
		boolean force = cl.hasOption("f");
		FOption<Long> interval = cl.getOptionLong("report_interval");
		
		String dsId = cl.getArgument(0);
		String outputDir = cl.getString("output_dir");
		ShapefileParameters params = ShapefileParameters.create()
														.charset(charset);
		splitSize.ifPresent(params::splitSize);
		
		ExportDataSetAsShapefile export = new ExportDataSetAsShapefile(dsId, outputDir, params);
		export.setForce(force);
		interval.ifPresent(export::setProgressInterval);
		
		ProgressiveExecution<Long, Long> act = export.start(marmot);
		return act.get();
	}
}
