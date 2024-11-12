package marmot.command;

import java.io.FileReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.analysis.system.SystemAnalysis;
import marmot.exec.CompositeAnalysis;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotAnalysis.Type;
import marmot.exec.MarmotExecution;
import marmot.exec.ModuleAnalysis;
import marmot.exec.PlanAnalysis;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.PicocliSubCommand;
import utils.KeyValue;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotAnalysisCommands {
	@Command(name="list", description="list MarmotAnalysis")
	public static class ListAnalysis extends PicocliSubCommand<MarmotRuntime> {
		@Parameters(paramLabel="path", arity = "0..*", description={"directory path to display from"})
		private String m_start;

		@Option(names={"-r"}, description="list all descendant analysis")
		private boolean m_recursive;

		@Option(names={"-l"}, description="list in detail")
		private boolean m_details;

		@Option(names={"-t", "-top"}, description="list top-level analyses only")
		private boolean m_topLevel;
		
		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			List<MarmotAnalysis> analList;
			if ( m_start != null ) {
				analList = initialContext.getDescendantAnalysisAll(m_start);
			}
			else {
				analList = initialContext.getAnalysisAll();
			}
			
			if ( m_topLevel ) {
				Set<String> compList = Sets.newHashSet();
				FStream.from(analList)
						.castSafely(CompositeAnalysis.class)
						.forEach(c -> compList.addAll(c.getComponents()));
				analList = FStream.from(analList)
									.filter(a -> !compList.contains(a.getId()))
									.toList();
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

	@Command(name="run", description="run MarmotAnalysis")
	public static class Run extends PicocliSubCommand<MarmotRuntime> {
		@Parameters(paramLabel="id", arity = "1..1", description={"analysis id"})
		private String m_id;
		
		@Option(names={"-async"}, description="plan JSON file to register")
		private boolean m_async;
		
		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			MarmotAnalysis analysis = initialContext.getAnalysis(m_id);
			
			if ( m_async ) {
				MarmotExecution exec = initialContext.startAnalysis(analysis);
				System.out.println(exec.getId());
			}
			else {
				initialContext.executeAnalysis(analysis);
			}
		}
	}

	@Command(name="show", description="show analysis")
	public static class Show extends PicocliSubCommand<MarmotRuntime> {
		@Parameters(paramLabel="id", index="0", arity = "1..1", description={"analysis id"})
		private String m_id;
		
		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			MarmotAnalysis anal = initialContext.getAnalysis(m_id);
			if ( anal.getType() != Type.COMPOSITE ) {
				System.out.println(anal);
			}
			else {
				CompositeAnalysis comp = (CompositeAnalysis)anal;
				try ( PrintWriter pw = new PrintWriter(System.out) ) {
					showComponents(initialContext, pw, comp, "");
				}
			}
		}
		
		private void showComponents(MarmotRuntime marmot, PrintWriter pw, CompositeAnalysis parent,
									String indent) {
			int index = 1;
			for ( String compId: parent.getComponents() ) {
				MarmotAnalysis comp = marmot.getAnalysis(compId);
				
				pw.print(String.format("%s%02d: %s, %s%n", indent, index, comp.getId(), comp.getType()));
				if ( comp.getType() == Type.COMPOSITE ) {
					showComponents(marmot, pw, (CompositeAnalysis)comp, indent + "    ");
				}
				++index;
			}
		}
	}

	@Command(name="add", description="add a MarmotAnalysis",
			subcommands= {
				AddPlan.class,
				AddSystem.class,
				AddModule.class,
				AddComposite.class
			})
	public static class Add extends PicocliSubCommand<MarmotRuntime> {
		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			getCommandLine().usage(System.out, Ansi.OFF);
		}
	}
	
	private abstract static class AddCommand extends PicocliSubCommand<MarmotRuntime> {
		@Parameters(paramLabel="id", index="0", description={"analysis id"})
		private String m_id;
		
		@Option(names={"-f", "-force"}, description="force to add")
		private boolean m_force;
		
		abstract protected void add(MarmotRuntime marmot, String id, boolean force) throws Exception;
		
		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			add(initialContext, m_id, m_force);
		}
		
	}

	@Command(name="plan", description="add a plan analysis")
	public static class AddPlan extends AddCommand {
		@Parameters(paramLabel="file_path", index="1", description={"plan file path"})
		private String m_planFile;
		
		@Override
		protected void add(MarmotRuntime marmot, String id, boolean force) throws Exception {
			try ( Reader reader = new FileReader(m_planFile) ) {
				PlanAnalysis analysis = new PlanAnalysis(id, Plan.parseJson(reader));
				marmot.addAnalysis(analysis, force);
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
		protected void add(MarmotRuntime marmot, String id, boolean force) throws Exception {
			SystemAnalysis analysis = new SystemAnalysis(id, m_funcId, m_args);
			marmot.addAnalysis(analysis, force);
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
		protected void add(MarmotRuntime marmot, String id, boolean force) throws Exception {
			Map<String,String> args = FStream.from(m_kvArgs)
											.map(KeyValue::parse)
											.toMap(KeyValue::key, KeyValue::value);
			ModuleAnalysis analysis = new ModuleAnalysis(id, m_moduleId, args);
			marmot.addAnalysis(analysis, force);
		}
	}

	@Command(name="composite", description="add a composite analysis")
	public static class AddComposite extends AddCommand {
		@Parameters(paramLabel="components", index="1..*", description={"component analysis list"})
		private java.util.List<String> componentsList;
		
		@Override
		protected void add(MarmotRuntime marmot, String id, boolean force) throws Exception {
			CompositeAnalysis analysis = new CompositeAnalysis(id, componentsList);
			marmot.addAnalysis(analysis, force);
		}
	}

	@Command(name="delete", aliases= {"remove"}, description="delete analysis")
	public static class Delete extends PicocliSubCommand<MarmotRuntime> {
		@Parameters(paramLabel="analysis_id (or directory-id)", arity = "1..1",
					description={"analysis id to delete"})
		private String m_id;
		
		@Option(names={"-r"}, description="delete all its descendant analyses")
		private boolean m_recursive;
		
		@Option(names={"-f"}, description="delete analysis forcibly")
		private boolean m_force;
		
		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			if ( !m_force ) {
				CompositeAnalysis parent = initialContext.findParentAnalysis(m_id);
				if ( parent != null ) {
					throw new IllegalStateException("some analysises refer to this: " + parent);
				}
			}
			
			try {
				initialContext.deleteAnalysis(m_id, m_recursive);
			}
			catch ( Exception e ) {
				System.err.println(e);
			}
		}
	}
}
