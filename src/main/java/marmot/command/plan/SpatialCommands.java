package marmot.command.plan;

import org.locationtech.jts.geom.Envelope;

import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.SpatialRelation;
import marmot.plan.GeomOpOptions;
import marmot.plan.PredicateOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class SpatialCommands {
	@Command(name="buffer", description="add a 'buffer' operator")
	static class AddBuffer extends AbstractAddOperatorCommand {
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
	static class AddCentroid extends AbstractAddOperatorCommand {
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
	static class AddTransformCrs extends AbstractAddOperatorCommand {
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
	
	@Command(name="filter_spatially", description="add a 'filter_spatially' operator")
	static class AddFilterSpatially extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="geom_col", index="0", arity="1..1",
					description={"column name for the input geometry data"})
		private String m_geomCol;
		
		@Option(names={"-key_dataset"}, paramLabel="dataset_id", description="key dataset id")
		private String m_keyDsId;
		
		@Option(names={"-bounds"}, paramLabel="envelope_string", description="key bounds")
		private String m_boundsStr;

		@Option(names={"-relation"}, paramLabel="expr",
				description={"spatial relation. (eg: within_distance(15))"})
		private String m_relExpr;
		
		private PredicateOptions m_opts = PredicateOptions.DEFAULT;
		
		@Option(names={"-negated"}, description="negated search")
		private void setNegated(boolean flag) {
			m_opts = m_opts.negated(flag);
		}

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			SpatialRelation rel = SpatialRelation.INTERSECTS;
			if ( m_relExpr != null ) {
				rel = SpatialRelation.parse(m_relExpr);
			}
			
			if ( m_boundsStr != null ) {
				Envelope bounds = GeoClientUtils.parseEnvelope(m_boundsStr).get();
				return builder.filterSpatially(m_geomCol, rel, bounds, m_opts);
			}
			else if ( m_keyDsId != null ) {
				return builder.filterSpatially(m_geomCol, rel, m_keyDsId, m_opts);
			}
			
			throw new IllegalArgumentException("filter key is not defined");
		}
	}
	
	@Command(name="intersection", description="add a 'intersection' operator")
	static class AddIntersection extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="geom_col", index="0",
					description={"column name for the input geometry data"})
		private String m_geomCol;
		
		@Option(names={"-column"}, paramLabel="name", required=true,
				description="column name for second geometry")
		private String m_geomCol2;
		
		@Option(names={"-output"}, paramLabel="column_name", required=true,
				description="column name for output geometry")
		private String m_output;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			return builder.intersection(m_geomCol, m_geomCol2, m_output);
		}
	}
	
	@Command(name="arc_clip", description="add a 'arc_clip' operator")
	static class AddArcClip extends AbstractAddOperatorCommand {
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
