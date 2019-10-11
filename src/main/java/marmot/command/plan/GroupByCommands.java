package marmot.command.plan;

import java.util.List;

import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.command.PicocliCommands.SubCommand;
import marmot.command.plan.GroupByCommands.AddAggregateByGroup;
import marmot.command.plan.GroupByCommands.AddReduceToRecord;
import marmot.command.plan.GroupByCommands.AddTakeByGroup;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="group_by",
		subcommands = {
			AddAggregateByGroup.class,
			AddTakeByGroup.class,
			AddReduceToRecord.class,
		},
		description="add a 'group-by' operators")
class GroupByCommands extends SubCommand {
	@Parameters(paramLabel="keys", index="0", arity="1..1",
				description={"group key columns"})
	private String m_keyCols;
	
	@Option(names={"-tags"}, paramLabel="columns", description="tag columns")
	private String m_tagCols;
	
	@Option(names={"-order_by"}, paramLabel="columns", description="order-by columns")
	private String m_orderByCols;

	@Option(names={"-workers"}, paramLabel="count", description="worker count")
	private int m_workerCount = 0;
	
	@Override
	public void run(MarmotRuntime marmot) throws Exception {
		getCommandLine().usage(System.out, Ansi.OFF);
	}
	
	Group getGroup() throws Exception {
		Group group = Group.ofKeys(m_keyCols);
		if ( m_orderByCols != null ) {
			group.orderBy(m_orderByCols);
		}
		if ( m_tagCols != null ) {
			group.tags(m_tagCols);
		}
		if ( m_workerCount > 0 ) {
			group.workerCount(m_workerCount);
		}
		
		return group;
	}
	
	static abstract class AbstractGroupByCommand extends AbstractAddOperatorCommand {
		abstract protected PlanBuilder addGroupByCommand(MarmotRuntime marmot,
											PlanBuilder builder, Group group) throws Exception;
		
		@Override
		protected PlanBuilder add(MarmotRuntime marmot, PlanBuilder plan) throws Exception {
			Group group = ((GroupByCommands)getParent()).getGroup();
			return addGroupByCommand(marmot, plan, group);
		}
	}

	@Command(name="aggregate", description="add a 'aggregate by group' operator")
	static class AddAggregateByGroup extends AbstractGroupByCommand {
		@Parameters(paramLabel="aggregates", index="0", arity="1..*",
					description={"group key columns"})
		private List<String> m_aggrFuncs;
		
		@Override
		public PlanBuilder addGroupByCommand(MarmotRuntime marmot, PlanBuilder builder,
											Group group) throws Exception {
			List<AggregateFunction> aggrs = FStream.from(m_aggrFuncs)
												.map(AggregateFunction::fromProto)
												.toList();
			return builder.aggregateByGroup(group, aggrs);
		}
	}

	@Command(name="take", description="add a 'take by group' operator")
	static class AddTakeByGroup extends AbstractGroupByCommand {
		@Parameters(paramLabel="count", index="0", arity="1..1",
					description={"take count"})
		private int m_count;
		
		@Override
		public PlanBuilder addGroupByCommand(MarmotRuntime marmot, PlanBuilder builder,
											Group group) throws Exception {
			return builder.takeByGroup(null, m_count);
		}
	}

	@Command(name="reduce_to_record", description="add a 'reduce_to_record by group' operator")
	static class AddReduceToRecord extends AbstractGroupByCommand {
		@Option(names={"-schema"}, paramLabel="schema", required= true, description="output schema")
		private String m_schemaStr;
		
		@Option(names={"-tag"}, paramLabel="column", required= true, description="tag column")
		private String m_tagCol;
		
		@Option(names={"-value"}, paramLabel="column", required= true, description="value column")
		private String m_valueCol;
		
		@Override
		public PlanBuilder addGroupByCommand(MarmotRuntime marmot, PlanBuilder builder,
											Group group) throws Exception {
			RecordSchema schema = RecordSchema.parse(m_schemaStr);
			return builder.reduceToSingleRecordByGroup(group, schema, m_tagCol, m_valueCol);
		}
	}
}
