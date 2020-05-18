package marmot.command.plan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.exec.MarmotExecution;
import marmot.plan.STScriptPlanLoader;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import utils.PicocliSubCommand;
import utils.io.FileUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PlanCommands {
	@Command(name="show", description="show a plan")
	public static class Show extends PicocliSubCommand<MarmotRuntime> {
		@Option(names={"-f"}, paramLabel="plan_file", required = true,
				description={"target plan file to print"})
		private String m_file;

		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			Plan plan = loadPlan(new File(m_file));
			System.out.println(plan.toString());
		}
	}
	
	@Command(name="schema", description="show the output RecordSchema of a plan")
	public static class Schema extends PicocliSubCommand<MarmotRuntime> {
		@Option(names={"-f"}, paramLabel="plan_file", required = true,
				description={"plan file to print"})
		private String m_file;

		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			Plan plan = loadPlan(new File(m_file));
			RecordSchema schema = initialContext.getOutputRecordSchema(plan);
			
			System.out.println(schema.toString());
		}
	}
	
	@Command(name="run", description="execute a plan")
	public static class Run extends PicocliSubCommand<MarmotRuntime> {
		@Option(names={"-f"}, paramLabel="plan_file", required = true,
				description={"target plan file to print"})
		private String m_file;

		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			Plan plan = loadPlan(new File(m_file));
			initialContext.execute(plan);
		}
	}
	
	@Command(name="start", description="start a plan")
	public static class Start extends PicocliSubCommand<MarmotRuntime> {
		@Option(names={"-f"}, paramLabel="plan_file", required = true,
				description={"target plan file to print"})
		private String m_file;

		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			Plan plan = loadPlan(new File(m_file));
			MarmotExecution exec = initialContext.start(plan);
			System.out.println(exec);
		}
	}
	
	private static Plan loadPlan(File file) throws FileNotFoundException, IOException {
		String ext = FileUtils.getExtension(file).toLowerCase();
		switch ( ext ) {
			case "json":
			case "":
				try ( Reader reader = new BufferedReader(new FileReader(file)) ) {
					return Plan.parseJson(reader);
				}
			case "st":
				return STScriptPlanLoader.load(file);
			default:
				throw new UnsupportedOperationException("invalid file extenstion: " + ext);
		}
	}
}
