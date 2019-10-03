package marmot.command;

import java.io.FileReader;
import java.io.Reader;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.exec.CompositeAnalysis;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotExecution;
import marmot.exec.PlanAnalysis;
import marmot.exec.SystemAnalysis;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotAnalysisCommand {
	@Command(name="list", description="list MarmotAnalytics")
	public static class List implements CheckedConsumer<MarmotRuntime> {
		@Parameters(paramLabel="path", arity = "0..*", description={"directory path to display from"})
		private String m_start;

		@Option(names={"-r"}, description="list all descendant analysis")
		private boolean m_recursive;

		@Option(names={"-l"}, description="list in detail")
		private boolean m_details;
		
		@Override
		public void accept(MarmotRuntime marmot) throws Exception {
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
	public static class Run implements CheckedConsumer<MarmotRuntime> {
		@Parameters(paramLabel="id", arity = "1..1", description={"analysis id"})
		private String m_id;
		
		@Option(names={"-async"}, description="plan JSON file to register")
		private boolean m_async;
		
		@Override
		public void accept(MarmotRuntime marmot) throws Exception {
			if ( m_async ) {
				MarmotExecution exec = marmot.startAnalysis(m_id);
				System.out.println(exec.getId());
			}
			else {
				marmot.executeAnalysis(m_id);
			}
		}
	}

	@Command(name="cancel", description="cancel running MarmotExecution")
	public static class Cancel implements CheckedConsumer<MarmotRuntime> {
		@Parameters(paramLabel="id", arity = "1..1", description={"execution id"})
		private String m_id;
		
		@Override
		public void accept(MarmotRuntime marmot) throws Exception {
			MarmotExecution exec = marmot.getMarmotExecution(m_id);
			exec.cancel();
		}
	}

	@Command(name="show", description="show analysis")
	public static class Show implements CheckedConsumer<MarmotRuntime> {
		@Parameters(paramLabel="id", index="0", arity = "1..1", description={"analysis id"})
		private String m_id;
		
		@Override
		public void accept(MarmotRuntime marmot) throws Exception {
			MarmotAnalysis anal = marmot.getMarmotAnalysis(m_id);
			System.out.println(anal);
		}
	}

	@Command(name="add", description="add a MarmotAnalytics",
			subcommands= { AddPlan.class, AddSystem.class, AddComposite.class })
	public static class Add {
		@Option(names={"-force"}, description="force to add")
		private boolean m_force;
	}

	@Command(name="plan", description="add a plan analysis")
	public static class AddPlan implements CheckedConsumer<MarmotRuntime> {
		@Parameters(paramLabel="id", index="0", description={"analysis id"})
		private String m_id;
		
		@Parameters(paramLabel="plan_file", index="1", description={"plan JSON file to register"})
		private String m_planFile;
		
		@ParentCommand
		private Add m_parent;
		
		@Override
		public void accept(MarmotRuntime marmot) throws Exception {
			if ( m_parent.m_force ) {
				marmot.deleteMarmotAnalysis(m_id);
			}
			
			try ( Reader reader = new FileReader(m_planFile) ) {
				PlanAnalysis analysis = new PlanAnalysis(m_id, Plan.parseJson(reader));
				marmot.addMarmotAnalysis(analysis);
			}
		}
	}

	@Command(name="system", description="add a system analysis")
	public static class AddSystem implements CheckedConsumer<MarmotRuntime> {
		@Parameters(paramLabel="id", index="0", description={"analysis id"})
		private String m_id;
		
		@Parameters(paramLabel="func_id", index="1", description={"system function id"})
		private String m_funcId;
		
		@Parameters(paramLabel="func_args", index="2..*", description={"system function args"})
		private java.util.List<String> m_args;
		
		@ParentCommand
		private Add m_parent;
		
		@Override
		public void accept(MarmotRuntime marmot) throws Exception {
			if ( m_parent.m_force ) {
				marmot.deleteMarmotAnalysis(m_id);
			}
			
			SystemAnalysis analysis = new SystemAnalysis(m_id, m_funcId, m_args);
			marmot.addMarmotAnalysis(analysis);
		}
	}

	@Command(name="composite", description="add a composite analysis")
	public static class AddComposite implements CheckedConsumer<MarmotRuntime> {
		@Parameters(paramLabel="id", index="0", arity = "1..1", description={"analysis id"})
		private String m_id;
		
		@Parameters(paramLabel="component_id", index="1..*", description={"component analysis list"})
		private java.util.List<String> componentsList;
		
		@Override
		public void accept(MarmotRuntime marmot) throws Exception {
			CompositeAnalysis analysis = new CompositeAnalysis(m_id, componentsList);
			marmot.addMarmotAnalysis(analysis);
		}
	}

	@Command(name="delete", description="delete analysis")
	public static class Delete implements CheckedConsumer<MarmotRuntime> {
		@Parameters(paramLabel="analysis_id (or directory-id)", arity = "1..1",
					description={"analysis id to delete"})
		private String m_target;
		
		@Option(names={"-r"}, description="delete all analysis in subdirectories recursively")
		private boolean m_recursive;
		
		@Override
		public void accept(MarmotRuntime marmot) throws Exception {
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
