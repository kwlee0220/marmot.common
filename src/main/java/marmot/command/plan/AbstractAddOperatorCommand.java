package marmot.command.plan;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.command.PicocliCommands.PicocliCommand;
import marmot.command.PicocliCommands.SubCommand;
import marmot.optor.AggregateFunction;
import utils.CSV;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
abstract class AbstractAddOperatorCommand extends SubCommand<MarmotRuntime> {
	@Override
	public void run(MarmotRuntime marmot) throws Exception {
		Plan plan = loadPlan(marmot);
		plan = add(marmot, plan.toBuilder()).build();
		
		String file = getPlanFile();
		
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
	
	abstract protected PlanBuilder add(MarmotRuntime marmot, PlanBuilder plan) throws Exception;
	
	private String getPlanFile() {
		PicocliCommand parent = getParent();
		if ( parent instanceof GroupByCommands ) {
			parent = ((GroupByCommands)parent).getParent();
		}
		else if ( parent instanceof AssignCommands ) {
			parent = ((AssignCommands)parent).getParent();
		}
		
		return ((BuildPlanCommand)parent).getPlanFile();
	}
	
	private Plan loadPlan(MarmotRuntime marmot) throws IOException {
		String file = getPlanFile();
		
		Reader reader = null;
		if ( file != null ) {
			reader = new FileReader(file);
		}
		else {
			reader = new InputStreamReader(System.in);
		}
		
		try ( Reader r = reader ) {
			return Plan.parseJson(r);
		}
	}
	
	protected AggregateFunction[] parseAggregates(String aggrsStr) {
		List<AggregateFunction> aggrs = Lists.newArrayList();
		for ( String aggrSpecStr: CSV.parseCsv(aggrsStr.trim(), ',', '\\').toList() ) {
			AggregateFunction aggr = AggregateFunction.fromProto(aggrSpecStr.trim());
			aggrs.add(aggr);
		}
		
		return Iterables.toArray(aggrs, AggregateFunction.class);
	}
}