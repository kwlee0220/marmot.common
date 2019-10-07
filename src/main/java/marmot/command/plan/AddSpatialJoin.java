package marmot.command.plan;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.CONVEX_HULL;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.ENVELOPE;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.STDDEV;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.AggregateFunction.UNION_GEOM;

import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.optor.AggregateFunction;
import marmot.plan.SpatialJoinOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.CSV;

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
	
	private SpatialJoinOptions m_opts = SpatialJoinOptions.EMPTY;

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
			description={"join type: inner|outer|semi|semi_negated|aggregate (default: inner)"})
	private String m_joinType = "inner";

	@Option(names={"-aggregates"}, paramLabel="funcs", description={"aggregate functions)"})
	private String m_aggrFuncs;

	@Override
	public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
		switch ( m_joinType.toLowerCase() ) {
			case "inner":
				builder.spatialJoin(m_geomCol, m_paramDsId, m_opts);
				break;
			case "semi":
				builder.spatialSemiJoin(m_geomCol, m_paramDsId, m_opts);
				break;
			case "semi_negated":
				builder.spatialSemiJoin(m_geomCol, m_paramDsId, m_opts);
				break;
			case "outer":
				builder.spatialOuterJoin(m_geomCol, m_paramDsId, m_opts);
				break;
			case "aggregate":
				if ( m_aggrFuncs == null ) {
					throw new IllegalArgumentException("aggregate functions are not provided");
				}
				AggregateFunction[] aggrs = parseAggregate(m_aggrFuncs);
				builder.spatialAggregateJoin(m_geomCol, m_paramDsId, aggrs, m_opts);
				break;
			default:
				throw new IllegalArgumentException("invalid spatialjoin type: " + m_joinType);
		}
		
		return builder;
	}
	
	private AggregateFunction[] parseAggregate(String aggrsStr) {
		List<AggregateFunction> aggrs = Lists.newArrayList();
		for ( String aggrSpecStr: CSV.parseCsv(aggrsStr, ',', '\\').toList() ) {
			List<String> aggrSpec = CSV.parseCsv(aggrSpecStr, ':', '\\').toList();
			
			AggregateFunction aggr = null;
			switch ( aggrSpec.get(0).toUpperCase()) {
				case "COUNT":
					aggr = COUNT();
					break;
				case "SUM":
					if ( aggrSpec.size() == 2 ) {
						aggr = SUM(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("SUM: target column is not specified");
					}
					break;
				case "AVG":
					if ( aggrSpec.size() == 2 ) {
						aggr = AVG(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("AVG: target column is not specified");
					}
					break;
				case "MAX":
					if ( aggrSpec.size() == 2 ) {
						aggr = MAX(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("MAX: target column is not specified");
					}
					break;
				case "MIN":
					if ( aggrSpec.size() == 2 ) {
						aggr = MIN(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("MIN: target column is not specified");
					}
					break;
				case "STDDEV":
					if ( aggrSpec.size() == 2 ) {
						aggr = STDDEV(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("STDDEV: target column is not specified");
					}
					break;
				case "CONVEX_HULL":
					if ( aggrSpec.size() == 2 ) {
						aggr = CONVEX_HULL(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("CONVEX_HULL: target column is not specified");
					}
					break;
				case "ENVELOPE":
					if ( aggrSpec.size() == 2 ) {
						aggr = ENVELOPE(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("ENVELOPE: target column is not specified");
					}
					break;
				case "GEOM_UNION":
					if ( aggrSpec.size() == 2 ) {
						aggr = UNION_GEOM(aggrSpec.get(1));
					}
					else {
						throw new IllegalArgumentException("GEOM_UNION: target column is not specified");
					}
					break;
				default:
					String details = String.format("invalid aggregation function: %s'%n", aggrSpec.get(0));
					throw new IllegalArgumentException(details);
					
			}
			if ( aggrSpec.size() >= 3 ) {
				aggr = aggr.as(aggrSpec.get(2));
			}
			aggrs.add(aggr);
		}
		
		return Iterables.toArray(aggrs, AggregateFunction.class);
	}
}
