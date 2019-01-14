package marmot.geo.command;

import java.io.File;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import marmot.command.ImportParameters;
import marmot.command.UsageHelp;
import marmot.externio.shp.ImportShapefile;
import marmot.externio.shp.ShapefileParameters;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportShapefileCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private ImportParameters m_importParams;
	@Mixin private ShapefileParameters m_shpParams;
	@Mixin private UsageHelp m_help;

	@Parameters(paramLabel="shp_file", index="0", arity="1..1",
				description={"path to the target shapefile (or directory)"})
	private String m_shpPath;

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		StopWatch watch = StopWatch.start();
		
		if ( m_importParams.getGeometryColumnInfo().isAbsent() ) {
			throw new IllegalArgumentException("Option '-geom_col' is missing");
		}
		
		File shpFile = new File(m_shpPath);
		ImportShapefile importFile = ImportShapefile.from(shpFile, m_shpParams, m_importParams);
		importFile.getProgressObservable()
					.subscribe(report -> {
						double velo = report / watch.getElapsedInFloatingSeconds();
						System.out.printf("imported: count=%d, elapsed=%s, velo=%.1f/s%n",
										report, watch.getElapsedMillisString(), velo);
					});
		long count = importFile.run(marmot);
		
		double velo = count / watch.getElapsedInFloatingSeconds();
		System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
							m_importParams.getDataSetId(), count, watch.getElapsedMillisString(),
							velo);
	}
}