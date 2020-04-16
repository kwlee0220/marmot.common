package marmot.command.plan;

import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.command.PicocliCommands.SubCommand;
import marmot.command.plan.AssignCommands.AddAssignGridCell;
import marmot.command.plan.AssignCommands.AddAssignUid;
import marmot.optor.geo.SquareGrid;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="assign",
		subcommands = {
			AddAssignUid.class,
			AddAssignGridCell.class,
		},
		description="add a 'assign' operators")
class AssignCommands extends SubCommand<MarmotRuntime> {
	@Override
	public void run(MarmotRuntime marmot) throws Exception {
		getCommandLine().usage(System.out, Ansi.OFF);
	}
	
	@Command(name="uid", description="add a 'assign_uid' operator")
	public static class AddAssignUid extends AbstractAddOperatorCommand {
		@Option(names={"-column"}, paramLabel="col_name", description={"output column name)"})
		private String m_uidCol;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			return builder.assignUid(m_uidCol);
		}
	}
	
	@Command(name="grid_cell", aliases= {"gridcell", "cell"}, description="add a 'assign_gridcell' operator")
	public static class AddAssignGridCell extends AbstractAddOperatorCommand {
		@Parameters(paramLabel="geom_col", index="0",
					description={"column name for the input geometry data"})
		private String m_geomCol;

		@Parameters(paramLabel="grid_expr", index="1",
					description={"grid-cell information"})
		private String m_gridExpr;
		
		@Option(names={"-assign_outside"},
				description={"assign grid info for the record that is out of the grid"})
		private boolean m_outside;

		@Override
		public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
			SquareGrid grid = SquareGrid.parseString(m_gridExpr);
			return builder.assignGridCell(m_geomCol, grid, m_outside);
		}
	}
}
