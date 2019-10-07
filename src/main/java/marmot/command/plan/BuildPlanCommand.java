package marmot.command.plan;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Writer;

import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.StoreDataSetOptions;
import marmot.command.PicocliCommands.SubCommand;
import marmot.command.plan.BuildPlanCommand.AddArcClip;
import marmot.command.plan.BuildPlanCommand.AddBuffer;
import marmot.command.plan.BuildPlanCommand.AddCentroid;
import marmot.command.plan.BuildPlanCommand.AddFilter;
import marmot.command.plan.BuildPlanCommand.AddLoad;
import marmot.command.plan.BuildPlanCommand.AddProject;
import marmot.command.plan.BuildPlanCommand.AddStore;
import marmot.command.plan.BuildPlanCommand.Create;
import marmot.plan.GeomOpOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.UnitUtils;
import utils.func.Funcs;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="build",
		subcommands = {
			Create.class,
			AddLoad.class, AddStore.class,
			AddFilter.class, AddProject.class,
			AddBuffer.class, AddCentroid.class,
			AddSpatialJoin.class,
			AddArcClip.class,
		},
		description="add a operator into the plan")
public class BuildPlanCommand extends SubCommand {
	@Override
	public void run(MarmotRuntime marmot) throws Exception {
		getCommandLine().usage(System.out, Ansi.OFF);
	}
	
	@Command(name="create", description="create an empty plan")
	public static class Create extends SubCommand {
		@Option(names={"-f"}, paramLabel="plan_file", description={"target plan file to build"})
		private String m_file;

		@Parameters(paramLabel="plan_name", arity="1..1", description={"plan name"})
		private String m_planName;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			Plan plan = marmot.planBuilder(m_planName).build();
			
			Writer writer;
			if ( m_file != null ) {
				writer = new FileWriter(m_file);
			}
			else {
				writer = new PrintWriter(System.out);
			}
			
			try ( Writer w = writer ) {
				writer.write(plan.toJson());
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

		@Option(names={"-force"}, description="force to create a new dataset")
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

			opts = Funcs.applyIfNotNull(m_gcInfo, opts::geometryColumnInfo);
			opts = Funcs.applyIfNotNull(m_blockSize, opts::blockSize);
			opts = Funcs.applyIfNotNull(m_codecName, opts::compressionCodecName);
			
			return builder.store(m_dsId, opts);
		}
	}
	
	@Command(name="buffer", description="add a 'buffer' operator")
	public static class AddBuffer extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="geom_col", index="0",
					description={"column name for the input geometry data"})
		private String m_geomCol;

		@Parameters(paramLabel="distance", index="1", arity="1..1", description={"buffer distance"})
		private String m_distance;

		private GeomOpOptions m_opts = GeomOpOptions.DEFAULT;
		@Option(names={"-o"}, paramLabel="colname", description="output column name")
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
		@Option(names={"-o"}, paramLabel="colname", description="output column name")
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
