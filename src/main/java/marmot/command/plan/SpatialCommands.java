package marmot.command.plan;

import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.plan.GeomOpOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialCommands {
	@Command(name="buffer", description="add a 'buffer' operator")
	public static class AddBuffer extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="geom_col", index="0",
					description={"column name for the input geometry data"})
		private String m_geomCol;

		@Parameters(paramLabel="distance", index="1", arity="1..1", description={"buffer distance"})
		private String m_distance;

		private GeomOpOptions m_opts = GeomOpOptions.DEFAULT;
		@Option(names={"-o", "-output"}, paramLabel="colname", description="output column name")
		private void setOutput(String col) {
			m_opts = m_opts.outputColumn(col);
		}

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			return builder.buffer(m_geomCol, UnitUtils.parseLengthInMeter(m_distance), m_opts);
		}
	}
	
	@Command(name="centroid", description="add a 'centroid' operator")
	public static class AddCentroid extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="geom_col", index="0",
					description={"column name for the input geometry data"})
		private String m_geomCol;

		private GeomOpOptions m_opts = GeomOpOptions.DEFAULT;
		@Option(names={"-o", "-output"}, paramLabel="colname", description="output column name")
		private void setOutput(String col) {
			m_opts = m_opts.outputColumn(col);
		}

		@Option(names={"-inside"}, description="find a centroid inside source geometry")
		private boolean m_inside = false;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			return builder.centroid(m_geomCol, m_inside, m_opts);
		}
	}
	
	@Command(name="transform_crs", description="add a 'transform_crs' operator")
	public static class AddTransformCrs extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="geom_col", index="0",
					description={"column name for the input geometry data"})
		private String m_geomCol;
		
		@Option(names={"-from"}, paramLabel="epsg_code", required=true,
				description="source  EPSG code")
		private String m_fromSrid;
		
		@Option(names={"-to"}, paramLabel="epsg_code", required=true,
				description="source  EPSG code")
		private String m_toSrid;

		private GeomOpOptions m_opts = GeomOpOptions.DEFAULT;
		@Option(names={"-o", "-output"}, paramLabel="colname", description="output column name")
		private void setOutput(String col) {
			m_opts = m_opts.outputColumn(col);
		}

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			return builder.transformCrs(m_geomCol, m_fromSrid, m_toSrid, m_opts);
		}
	}
	
	@Command(name="arc_clip", description="add a 'arc_clip' operator")
	public static class AddArcClip extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="geom_col", index="0",
					description={"column name for the input geometry data"})
		private String m_geomCol;
		
		@Parameters(paramLabel="clip_dataset_id", index="1",
					description={"dataset id for clip"})
		private String m_clipDsId;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			return builder.arcClip(m_geomCol, m_clipDsId);
		}
	}
}
