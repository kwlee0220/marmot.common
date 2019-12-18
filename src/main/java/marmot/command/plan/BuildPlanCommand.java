package marmot.command.plan;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Writer;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.command.PicocliCommands.SubCommand;
import marmot.command.plan.BuildPlanCommand.AddAggregate;
import marmot.command.plan.BuildPlanCommand.AddDefineColumn;
import marmot.command.plan.BuildPlanCommand.AddExpand;
import marmot.command.plan.BuildPlanCommand.AddFilter;
import marmot.command.plan.BuildPlanCommand.AddLoad;
import marmot.command.plan.BuildPlanCommand.AddProject;
import marmot.command.plan.BuildPlanCommand.AddShard;
import marmot.command.plan.BuildPlanCommand.AddStore;
import marmot.command.plan.BuildPlanCommand.AddUpdate;
import marmot.command.plan.BuildPlanCommand.Create;
import marmot.command.plan.HashJoinCommands.AddHashJoin;
import marmot.command.plan.HashJoinCommands.AddLoadHashJoin;
import marmot.command.plan.SpatialCommands.AddArcClip;
import marmot.command.plan.SpatialCommands.AddBuffer;
import marmot.command.plan.SpatialCommands.AddCentroid;
import marmot.command.plan.SpatialCommands.AddFilterSpatially;
import marmot.command.plan.SpatialCommands.AddIntersection;
import marmot.command.plan.SpatialCommands.AddTransformCrs;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.optor.StoreDataSetOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.func.Funcs;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="build",
		subcommands = {
			Create.class,
			AddLoad.class, AddStore.class, AddQuery.class,
			AddFilter.class, AddProject.class,
			AddDefineColumn.class, AddUpdate.class, AddExpand.class,
			GroupByCommands.class, AddAggregate.class,
			AddLoadHashJoin.class, AddHashJoin.class,
			AssignCommands.class, AddShard.class,
			AddBuffer.class, AddCentroid.class, AddTransformCrs.class, AddIntersection.class,
			AddFilterSpatially.class, AddSpatialJoin.class,
			AddArcClip.class,
		},
		description="add a operator into the plan")
public class BuildPlanCommand extends SubCommand {
	@Parameters(paramLabel="plan_file", arity="0..1", description={"Json plan file path"})
	private String m_file;

	String getPlanFile() {
		return m_file;
	}
	
	@Override
	public void run(MarmotRuntime marmot) throws Exception {
		getCommandLine().usage(System.out, Ansi.OFF);
	}
	
	@Command(name="create", description="create an empty plan")
	public static class Create extends SubCommand {
		@Parameters(paramLabel="plan_name", arity="1..1", description={"plan name"})
		private String m_planName;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			Plan plan = Plan.builder(m_planName).build();
			String file = ((BuildPlanCommand)getParent()).m_file;
			
			Writer writer;
			if ( file != null ) {
				writer = new FileWriter(file);
			}
			else {
				writer = new PrintWriter(System.out);
			}
			
			try ( Writer w = writer ) {
				writer.write(plan.toJson(false));
				writer.write("\n");
			}
		}
	}
	
	@Command(name="load", description="add a 'load_dataset' operator")
	public static class AddLoad extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="dataset_id", index="0",
					description={"dataset id to load"})
		private String m_dsId;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			return builder.load(m_dsId);
		}
	}
	
	@Command(name="filter", description="add a 'filter' operator")
	public static class AddFilter extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="predicate", index="0",
					description={"filter predicate expression"})
		private String m_predicate;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			return builder.filter(m_predicate);
		}
	}
	
	@Command(name="project", description="add a 'project' operator")
	public static class AddProject extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="columns", index="0",
					description={"columns expression"})
		private String m_cols;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			return builder.project(m_cols);
		}
	}
	
	@Command(name="define_column", description="add a 'define_column' operator")
	public static class AddDefineColumn extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="column", index="0", arity="1..1",
					description={"column declaration"})
		private String m_colDecl;

		@Parameters(paramLabel="expr", index="1", arity="0..1",
					description={"column initialization expression"})
		private String m_colInit;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			if ( m_colInit != null ) {
				return builder.defineColumn(m_colDecl, m_colInit);
			}
			else {
				return builder.defineColumn(m_colDecl);
			}
		}
	}
	
	@Command(name="expand", description="add a 'expand' operator")
	public static class AddExpand extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="column", index="0", arity="1..1",
					description={"column declarations"})
		private String m_colsDecl;

		@Parameters(paramLabel="expr", index="1", arity="0..1",
					description={"column expression"})
		private String m_expr;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			if ( m_expr != null ) {
				return builder.expand(m_colsDecl, m_expr);
			}
			else {
				return builder.expand(m_colsDecl);
			}
		}
	}
	
	@Command(name="update", description="add a 'update' operator")
	public static class AddUpdate extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="expr", index="0", arity="1..1",
					description={"column expression"})
		private String m_expr;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			return builder.update(m_expr);
		}
	}
	
	@Command(name="store", description="add a 'store' operator")
	public static class AddStore extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;
		
		@Option(names={"-g", "-geom_col"}, paramLabel="col-name(EPSG code)",
				description="default Geometry column info")
		public void setGeometryColumnInfo(String gcInfoStr) {
			m_gcInfo = GeometryColumnInfo.fromString(gcInfoStr);
		}
		private GeometryColumnInfo m_gcInfo;

		@Option(names={"-f"}, description="force to create a new dataset")
		private boolean m_force = false;
		
		@Option(names={"-a", "-append"}, description="append to the existing dataset")
		private boolean m_append = false;

		@Option(names={"-b", "-block_size"}, paramLabel="nbyte", description="block size (eg: '64mb')")
		private String m_blockSize;

		@Option(names={"-c", "-compress"}, description="compression codec name")
		private String m_codecName;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			StoreDataSetOptions opts = StoreDataSetOptions.EMPTY;
			if ( m_append ) {
				opts = opts.append(true);
			}
			else if ( m_force ) {
				opts = opts.force(true);
			}

			opts = Funcs.applyIfNotNull(m_gcInfo, opts::geometryColumnInfo, opts);
			opts = Funcs.applyIfNotNull(m_blockSize, opts::blockSize, opts);
			opts = Funcs.applyIfNotNull(m_codecName, opts::compressionCodecName, opts);
			
			return builder.store(m_dsId, opts);
		}
	}
	
//	@Command(name="group",
//			subcommands = {
//				AddAggregateByGroup.class,
//			},
//			description="add a 'group-by' operators")
//	public abstract static class GroupByCommand extends AbstractAddOperatorCommand {
//		@Parameters(paramLabel="keys", index="0", arity="1..1",
//					description={"group key columns"})
//		private String m_keyCols;
//		
//		@Option(names={"-tags"}, paramLabel="columns", description="tag columns")
//		private String m_tagCols;
//		
//		@Option(names={"-order_by"}, paramLabel="columns", description="order-by columns")
//		private String m_orderByCols;
//		
//		abstract protected PlanBuilder addGroupByCommand(MarmotRuntime marmot,
//														PlanBuilder plan, Group group) throws Exception;
//
//		@Override
//		protected PlanBuilder add(MarmotRuntime marmot, PlanBuilder plan) throws Exception {
//			Group group = Group.ofKeys(m_keyCols);
//			if ( m_orderByCols != null ) {
//				group.orderBy(m_orderByCols);
//			}
//			if ( m_tagCols != null ) {
//				group.tags(m_tagCols);
//			}
//			
//			return addGroupByCommand(marmot, plan, group);
//		}
//	}
	
	@Command(name="aggregate", description="add a 'aggregate' operator")
	public static class AddAggregate extends AbstractAddOperatorCommand {
		@Option(names={"-aggregates"}, paramLabel="funcs", description={"aggregate functions)"})
		private String m_aggrFuncs;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			AggregateFunction[] aggrs = parseAggregates(m_aggrFuncs);
			return builder.aggregate(aggrs);
		}
	}
	
	@Command(name="shard", description="add a 'shard' operator")
	public static class AddShard extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="count", index="0", description={"shard count"})
		private int m_partCount;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			return builder.shard(m_partCount);
		}
	}
}
