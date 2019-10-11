package marmot.command.plan;

import java.util.List;

import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.optor.JoinOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.CSV;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HashJoinCommands {
	@Command(name="hash_join", description="add a 'hash_join' operator")
	static class AddHashJoin extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="join_cols", index="0", arity="1..1",
					description={"left participant (<ds_id>:<join_cols>)"})
		private String m_joinCols;
	
		@Parameters(paramLabel="right_spec", index="1", arity="1..1",
					description={"right participant (<ds_id>:<join_cols>)"})
		private String m_rightSpec;
	
		@Option(names={"-o", "-output"}, paramLabel="column_expr", required=true,
				description="the result output columns")
		private String m_outCols;
	
		@Option(names={"-join_type"}, paramLabel="type",
				description={"join type: inner|left_outer|right_outer|full_outer|semi (default: inner)"})
		private String m_joinType = "inner";
	
		@Option(names={"-workers"}, paramLabel="count", description="worker count")
		private int m_workerCount = 0;
	
		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			List<String> rightSpec = CSV.parseCsv(m_rightSpec, ':', '\\').toList();
			
			JoinOptions opts = JoinOptions.INNER_JOIN;
			if ( m_joinType != null ) {
				switch ( m_joinType.toLowerCase() ) {
					case "inner":
						opts = JoinOptions.INNER_JOIN;
						break;
					case "left_outer":
						opts = JoinOptions.LEFT_OUTER_JOIN;
					case "right_outer":
						opts = JoinOptions.RIGHT_OUTER_JOIN;
						break;
					case "full_outer":
						opts = JoinOptions.FULL_OUTER_JOIN;
						break;
					case "semi":
						opts = JoinOptions.SEMI_JOIN;
						break;
					default:
						throw new IllegalArgumentException("invalid spatialjoin type: " + m_joinType);
				}
			}
			if ( m_workerCount > 0 ) {
				opts = opts.workerCount(m_workerCount);
			}
			
			return builder.hashJoin(m_joinCols, rightSpec.get(0), rightSpec.get(1), m_outCols, opts);
		}
	}

	@Command(name="load_hash_join", description="add a 'load_hash_join' operator")
	static class AddLoadHashJoin extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="left_spec", index="0", arity="1..1",
					description={"left participant (<ds_id>:<join_cols>)"})
		private String m_leftSpec;
	
		@Parameters(paramLabel="right_spec", index="1", arity="1..1",
					description={"right participant (<ds_id>:<join_cols>)"})
		private String m_rightSpec;
	
		@Option(names={"-o", "-output"}, paramLabel="column_expr", required=true,
				description="the result output columns")
		private String m_outCols;
	
		@Option(names={"-join_type"}, paramLabel="type",
				description={"join type: inner|left_outer|right_outer|full_outer|semi (default: inner)"})
		private String m_joinType = "inner";
	
		@Option(names={"-workers"}, paramLabel="count", description="worker count")
		private int m_workerCount = 0;
	
		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			List<String> leftSpec = CSV.parseCsv(m_leftSpec, ':', '\\').toList();
			List<String> rightSpec = CSV.parseCsv(m_rightSpec, ':', '\\').toList();
			
			JoinOptions opts = JoinOptions.INNER_JOIN;
			if ( m_joinType != null ) {
				switch ( m_joinType.toLowerCase() ) {
					case "inner":
						opts = JoinOptions.INNER_JOIN;
						break;
					case "left_outer":
						opts = JoinOptions.LEFT_OUTER_JOIN;
					case "right_outer":
						opts = JoinOptions.RIGHT_OUTER_JOIN;
						break;
					case "full_outer":
						opts = JoinOptions.FULL_OUTER_JOIN;
						break;
					case "semi":
						opts = JoinOptions.SEMI_JOIN;
						break;
					default:
						throw new IllegalArgumentException("invalid spatialjoin type: " + m_joinType);
				}
			}
			if ( m_workerCount > 0 ) {
				opts = opts.workerCount(m_workerCount);
			}
			
			return builder.loadHashJoin(leftSpec.get(0), leftSpec.get(1),
										rightSpec.get(0), rightSpec.get(1), m_outCols, opts);
		}
	}
}
