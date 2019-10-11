package marmot.command;

import java.io.FileReader;
import java.io.Reader;
import java.util.Map;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.PicocliCommands.SubCommand;
import marmot.exec.CompositeAnalysis;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotExecution;
import marmot.exec.ModuleAnalysis;
import marmot.exec.PlanAnalysis;
import marmot.exec.SystemAnalysis;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.KeyValue;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotAnalysisCommands {
	@Command(name="list", description="list MarmotAnalytics")
	public static class List extends SubCommand {
		@Parameters(paramLabel="path", arity = "0..*", description={"directory path to display from"})
		private String m_start;

		@Option(names={"-r"}, description="list all descendant analysis")
		private boolean m_recursive;

		@Option(names={"-l"}, description="list in detail")
		private boolean m_details;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			java.util.List<MarmotAnalysis> analList;
			if ( m_start != null ) {
				analList = marmot.getMarmotAnalysisAllInDir(m_start, m_recursive);
			}
			else {
				analList = marmot.getMarmotAnalysisAll();
			}
			
			for ( MarmotAnalysis analysis: analList ) {
				System.out.print(analysis.getId());
				
				if ( m_details ) {
					System.out.printf(" %s", analysis.getType());
				}
				System.out.println();
			}
		}
	}

	@Command(name="run", description="run MarmotAnalytics")
	public static class Run extends SubCommand {
		@Parameters(paramLabel="id", arity = "1..1", description={"analysis id"})
		private String m_id;
		
		@Option(names={"-async"}, description="plan JSON file to register")
		private boolean m_async;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			MarmotAnalysis analysis = marmot.getMarmotAnalysis(m_id);
			
			if ( m_async ) {
				MarmotExecution exec = marmot.start(analysis);
				System.out.println(exec.getId());
			}
			else {
				marmot.execute(analysis);
			}
		}
	}

	@Command(name="cancel", description="cancel running MarmotExecution")
	public static class Cancel extends SubCommand {
		@Parameters(paramLabel="id", arity = "1..1", description={"execution id"})
		private String m_id;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			MarmotExecution exec = marmot.getMarmotExecution(m_id);
			exec.cancel();
		}
	}

	@Command(name="show", description="show analysis")
	public static class Show extends SubCommand {
		@Parameters(paramLabel="id", index="0", arity = "1..1", description={"analysis id"})
		private String m_id;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			MarmotAnalysis anal = marmot.getMarmotAnalysis(m_id);
			System.out.println(anal);
		}
	}

	@Command(name="add", description="add a MarmotAnalytics",
			subcommands= {
				AddPlan.class,
				AddSystem.class,
				AddModule.class,
				AddComposite.class
			})
	public static class Add extends SubCommand {
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			getCommandLine().usage(System.out, Ansi.OFF);
		}
	}
	
	private abstract static class AddCommand extends SubCommand {
		@Parameters(paramLabel="id", index="0", description={"analysis id"})
		private String m_id;
		
		@Option(names={"-f", "-force"}, description="force to add")
		private boolean m_force;
		
		abstract protected void add(MarmotRuntime marmot, String id) throws Exception;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			if ( m_force ) {
				marmot.deleteMarmotAnalysis(m_id);
			}
			
			add(marmot, m_id);
		}
		
	}

	@Command(name="plan", description="add a plan analysis")
	public static class AddPlan extends AddCommand {
		@Parameters(paramLabel="file_path", index="1", description={"plan file path"})
		private String m_planFile;
		
		@Override
		protected void add(MarmotRuntime marmot, String id) throws Exception {
			try ( Reader reader = new FileReader(m_planFile) ) {
				PlanAnalysis analysis = new PlanAnalysis(id, Plan.parseJson(reader));
				marmot.addMarmotAnalysis(analysis);
			}
		}
	}

	@Command(name="system", description="add a system analysis")
	public static class AddSystem extends AddCommand {
		@Parameters(paramLabel="func_id", index="1", description={"system function id"})
		private String m_funcId;
		
		@Parameters(paramLabel="func_args", index="2..*", description={"system function args"})
		private java.util.List<String> m_args;
		
		@Override
		protected void add(MarmotRuntime marmot, String id) throws Exception {
			SystemAnalysis analysis = new SystemAnalysis(id, m_funcId, m_args);
			marmot.addMarmotAnalysis(analysis);
		}
	}

	@Command(name="module", description="add a module analysis")
	public static class AddModule extends AddCommand {
		@Parameters(paramLabel="module_id", index="1", description={"module id"})
		private String m_moduleId;
		
		@Parameters(paramLabel="arguments", index="2..*",
					description={"module arguments (key-value pairs)"})
		private java.util.List<String> m_kvArgs;
		
		@Override
		protected void add(MarmotRuntime marmot, String id) throws Exception {
			Map<String,String> args = FStream.from(m_kvArgs)
											.map(KeyValue::parse)
											.toMap(KeyValue::key, KeyValue::value);
			ModuleAnalysis analysis = new ModuleAnalysis(id, m_moduleId, args);
			marmot.addMarmotAnalysis(analysis);
		}
	}

	@Command(name="composite", description="add a composite analysis")
	public static class AddComposite extends AddCommand {
		@Parameters(paramLabel="components", index="1..*", description={"component analysis list"})
		private java.util.List<String> componentsList;
		
		@Override
		protected void add(MarmotRuntime marmot, String id) throws Exception {
			CompositeAnalysis analysis = new CompositeAnalysis(id, componentsList);
			marmot.addMarmotAnalysis(analysis);
		}
	}

	@Command(name="delete", description="delete analysis")
	public static class Delete extends SubCommand {
		@Parameters(paramLabel="analysis_id (or directory-id)", arity = "1..1",
					description={"analysis id to delete"})
		private String m_target;
		
		@Option(names={"-r"}, description="delete all analysis in subdirectories recursively")
		private boolean m_recursive;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			try {
				if ( m_recursive ) {
					marmot.deleteMarmotAnalysisAll(m_target);
				}
				else {
					marmot.deleteMarmotAnalysis(m_target);
				}
			}
			catch ( Exception e ) {
				System.err.println(e);
			}
		}
	}
}
