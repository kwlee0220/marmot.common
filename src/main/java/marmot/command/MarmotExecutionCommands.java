package marmot.command;

import java.util.ArrayList;
import java.util.List;

import marmot.MarmotRuntime;
import marmot.command.PicocliCommands.SubCommand;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotAnalysis.Type;
import marmot.exec.MarmotAnalysisExecution;
import marmot.exec.MarmotExecution;
import marmot.exec.MarmotExecution.State;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotExecutionCommands {
	@Command(name="show", description="show a MarmotExecution")
	public static class Show extends SubCommand {
		@Parameters(paramLabel="id", index="0", arity = "1..1", description={"execution id"})
		private String m_id;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			MarmotExecution exec = marmot.getMarmotExecution(m_id);
			System.out.println(exec);
		}
	}
	
	@Command(name="list", description="list MarmotExecutions")
	public static class ListExecs extends SubCommand {
		@Option(names={"-s", "-state"}, paramLabel="state",
				description="list all executions in a particular state")
		private String m_stateStr;
		
		@Option(names={"-a", "-analysis"}, paramLabel="type",
				description="list all executions of a particular analysis type (NONE for non-analysis executions)")
		private String m_analTypeStr;

		@Option(names={"-l"}, description="list in detail")
		private boolean m_details;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			List<MarmotExecution> execList = marmot.getMarmotExecutionAll();
			
			if ( m_stateStr != null ) {
				State state = State.valueOf(m_stateStr.toUpperCase());
				if ( state == null ) {
					throw new IllegalArgumentException("invalid '-state' option value: " + m_stateStr);
				}
				
				List<MarmotExecution> matcheds = new ArrayList<>();
				for ( MarmotExecution exec: execList ) {
					if ( state == exec.getState() ) {
						matcheds.add(exec);
					}
				}
				execList = matcheds;
			}
			
			if ( m_analTypeStr != null ) {
				if ( "NONE".equals(m_analTypeStr.toUpperCase()) ) {
					List<MarmotExecution> matcheds = new ArrayList<>();
					for ( MarmotExecution exec: execList ) {
						if ( !(exec instanceof MarmotAnalysisExecution) ) {
							matcheds.add(exec);
						}
					}
					execList = matcheds;
				}
				else if ( "ALL".equals(m_analTypeStr.toUpperCase()) ) {
					List<MarmotExecution> matcheds = new ArrayList<>();
					for ( MarmotExecution exec: execList ) {
						if ( exec instanceof MarmotAnalysisExecution ) {
							matcheds.add(exec);
						}
					}
					execList = matcheds;
				}
				else {
					Type type = Type.valueOf(m_analTypeStr.toUpperCase());
					if ( type == null ) {
						throw new IllegalArgumentException("invalid '-analysis' option value: " + m_analTypeStr);
					}
					
					List<MarmotExecution> matcheds = new ArrayList<>();
					for ( MarmotExecution exec: execList ) {
						if ( exec instanceof MarmotAnalysisExecution ) {
							MarmotAnalysis anal = ((MarmotAnalysisExecution)exec).getMarmotAnalysis();
							if ( type == anal.getType() ) {
								matcheds.add(exec);
							}
						}
					}
					execList = matcheds;
				}
			}
			
			for ( MarmotExecution exec: execList ) {
				if ( m_details ) {
					System.out.println(exec);
				}
				else {
					System.out.println(exec.getId());
				}
			}
		}
	}

	@Command(name="cancel", description="cancel a running MarmotExecution")
	public static class Cancel extends SubCommand {
		@Parameters(paramLabel="id", arity = "1..1", description={"execution id"})
		private String m_id;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			MarmotExecution exec = marmot.getMarmotExecution(m_id);
			exec.cancel();
		}
	}
}
