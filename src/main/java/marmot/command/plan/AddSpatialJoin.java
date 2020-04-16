package marmot.command.plan;

import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.optor.AggregateFunction;
import marmot.plan.SpatialJoinOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="spatial_join", description="add a 'spatial_join' operator")
class AddSpatialJoin extends AbstractAddOperatorCommand {
	@Parameters(paramLabel="geom_col", index="0", arity="1..1",
				description={"geometry column name in input dataset"})
	private String m_geomCol;
	
	@Parameters(paramLabel="param_dataset", index="1",
				description={"dataset id for parameter dataset"})
	private String m_paramDsId;
	
	private SpatialJoinOptions m_opts = SpatialJoinOptions.DEFAULT;

	@Option(names={"-o", "-output"}, paramLabel="column_expr",
			description="the result output columns")
	private void setOutputColumns(String outCols) {
		m_opts = m_opts.outputColumns(outCols);
	}

	@Option(names={"-join_expr"}, paramLabel="expr",
			description={"join expression. (eg: within_distance(15))"})
	private void setJoinExpr(String expr) {
		m_opts = m_opts.joinExpr(expr);
	}

	@Option(names={"-join_type"}, paramLabel="type",
			description={"join type: inner|outer|semi|semi_negated|aggregate|difference (default: inner)"})
	private String m_joinType = "inner";

	@Option(names={"-aggregates"}, paramLabel="funcs", description={"aggregate functions)"})
	private String m_aggrFuncs;

	@Override
	public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
		switch ( m_joinType.toLowerCase() ) {
			case "inner":
				return builder.spatialJoin(m_geomCol, m_paramDsId, m_opts);
			case "semi":
				return builder.spatialSemiJoin(m_geomCol, m_paramDsId, m_opts);
			case "semi_negated":
				return builder.spatialSemiJoin(m_geomCol, m_paramDsId, m_opts);
			case "outer":
				return builder.spatialOuterJoin(m_geomCol, m_paramDsId, m_opts);
			case "difference":
				return builder.differenceJoin(m_geomCol, m_paramDsId);
			case "aggregate":
				if ( m_aggrFuncs == null ) {
					throw new IllegalArgumentException("aggregate functions are not provided");
				}
				AggregateFunction[] aggrs = parseAggregates(m_aggrFuncs);
				return builder.spatialAggregateJoin(m_geomCol, m_paramDsId, aggrs, m_opts);
			default:
				throw new IllegalArgumentException("invalid spatialjoin type: " + m_joinType);
		}
	}
}
