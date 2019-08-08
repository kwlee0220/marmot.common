package marmot.geo.command;

import io.vavr.CheckedConsumer;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import static marmot.StoreDataSetOptions.*;
import marmot.command.UsageHelp;
import marmot.optor.geo.SquareGrid;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildSquareGridCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private Params m_params;
	@Mixin private UsageHelp m_help;
	
	public static class Params {
		@Parameters(paramLabel="dataset", index="0", arity="1..1",
					description={"dataset id for grid boundary"})
		private String m_input;
		
		@Parameters(paramLabel="grid_dataset", index="1", arity="1..1",
					description={"square-grid dataset"})
		private String m_output;
		
//		@Option(names={"-grid"}, paramLabel="grid_bounds", required=false,
//				description={"grid boundary"})
//		private void setGridBounds(String boundsExpr) {
//			m_gridBounds = GeoClientUtils.parseEnvelope(boundsExpr)
//										.getOrElseThrow(() -> new IllegalArgumentException("grid=" + boundsExpr));
//		}
//		private Envelope m_gridBounds;
		
		@Option(names={"-cell_size"}, paramLabel="cell_size", required=true,
				description={"grid cell size (in meter)"})
		private void setCellSize(String sizeExpr) {
			m_cellSize = Size2d.fromString(sizeExpr);
		}
		private Size2d m_cellSize;

		@Option(names={"-o", "-overlap"}, description={"skip un-overlapped cells)"})
		private boolean m_overlap = false;
	}

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		StopWatch watch = StopWatch.start();

		SquareGrid grid = new SquareGrid(m_params.m_input, m_params.m_cellSize);
		GeometryColumnInfo gcInfo = marmot.getDataSet(m_params.m_input)
											.getGeometryColumnInfo();
		String prjExpr = String.format("cell_geom as %s,cell_id,cell_pos", gcInfo.name());
		
		Plan plan;
		if ( m_params.m_overlap ) {
			plan = marmot.planBuilder("build_square_grid")
						.load(m_params.m_input)
						.assignGridCell(gcInfo.name(), grid, false)
						.project(prjExpr)
						.distinct("cell_id")
						.build();
		}
		else {
			plan = marmot.planBuilder("build_square_grid")
						.loadGrid(grid)
						.build();
		}
		marmot.createDataSet(m_params.m_output, plan, FORCE(gcInfo));
		
		watch.stop();
		System.out.printf("grid_output=%s, elapsed time: %s%n",
						m_params.m_output, watch.getElapsedSecondString());
	}
}
