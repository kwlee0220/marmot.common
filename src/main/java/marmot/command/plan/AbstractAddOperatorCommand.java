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
abstract class AbstractAddOperatorCommand extends SubCommand {
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
			writer.write(plan.toJson());
			writer.write("\n");
		}
	}
	
	abstract protected PlanBuilder add(MarmotRuntime marmot, PlanBuilder plan) throws Exception;
	
	private String getPlanFile() {
		PicocliCommand parent = getParent();
		if ( parent instanceof GroupByCommands ) {
			parent = ((GroupByCommands)parent).getParent();
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
	
	protected AggregateFunction[] parseAggregate(String aggrsStr) {
		List<AggregateFunction> aggrs = Lists.newArrayList();
		for ( String aggrSpecStr: CSV.parseCsv(aggrsStr.trim(), ',', '\\').toList() ) {
			AggregateFunction aggr = parseAggrFunction(aggrSpecStr.trim());
			aggrs.add(aggr);
		}
		
		return Iterables.toArray(aggrs, AggregateFunction.class);
	}
	
	protected AggregateFunction parseAggrFunction(String aggrStr) {
		List<String> aggrSpec = CSV.parseCsv(aggrStr, ':', '\\').toList();
		
		AggregateFunction aggr = null;
		switch ( aggrSpec.get(0).toUpperCase()) {
			case "COUNT":
				aggr = COUNT();
				break;
			case "SUM":
				if ( aggrSpec.size() >= 2 ) {
					aggr = SUM(aggrSpec.get(1));
				}
				else {
					throw new IllegalArgumentException("SUM: target column is not specified");
				}
				break;
			case "AVG":
				if ( aggrSpec.size() >=  2 ) {
					aggr = AVG(aggrSpec.get(1));
				}
				else {
					throw new IllegalArgumentException("AVG: target column is not specified");
				}
				break;
			case "MAX":
				if ( aggrSpec.size() >=  2 ) {
					aggr = MAX(aggrSpec.get(1));
				}
				else {
					throw new IllegalArgumentException("MAX: target column is not specified");
				}
				break;
			case "MIN":
				if ( aggrSpec.size() >=  2 ) {
					aggr = MIN(aggrSpec.get(1));
				}
				else {
					throw new IllegalArgumentException("MIN: target column is not specified");
				}
				break;
			case "STDDEV":
				if ( aggrSpec.size() >=  2 ) {
					aggr = STDDEV(aggrSpec.get(1));
				}
				else {
					throw new IllegalArgumentException("STDDEV: target column is not specified");
				}
				break;
			case "CONVEX_HULL":
				if ( aggrSpec.size() >=  2 ) {
					aggr = CONVEX_HULL(aggrSpec.get(1));
				}
				else {
					throw new IllegalArgumentException("CONVEX_HULL: target column is not specified");
				}
				break;
			case "ENVELOPE":
				if ( aggrSpec.size() >=  2 ) {
					aggr = ENVELOPE(aggrSpec.get(1));
				}
				else {
					throw new IllegalArgumentException("ENVELOPE: target column is not specified");
				}
				break;
			case "GEOM_UNION":
				if ( aggrSpec.size() >=  2 ) {
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
		
		return aggr;
	}
}