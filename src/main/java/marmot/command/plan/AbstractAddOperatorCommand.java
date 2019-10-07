package marmot.command.plan;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.command.PicocliCommands.SubCommand;
import picocli.CommandLine.Option;

public abstract class AbstractAddOperatorCommand extends SubCommand {
	@Option(names={"-f"}, paramLabel="plan_file", required = true,
			description={"target plan file to build"})
	private String m_file;
	
	@Override
	public void run(MarmotRuntime marmot) throws Exception {
		Plan plan = loadPlan(marmot);
		plan = add(marmot, plan.toBuilder()).build();
		
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
	
	abstract protected PlanBuilder add(MarmotRuntime marmot, PlanBuilder plan) throws Exception;
	
	private Plan loadPlan(MarmotRuntime marmot) throws IOException {
		Reader reader = null;
		if ( m_file != null ) {
			reader = new FileReader(m_file);
		}
		else {
			reader = new InputStreamReader(System.in);
		}
		
		try ( Reader r = reader ) {
			return Plan.parseJson(r);
		}
	}
}