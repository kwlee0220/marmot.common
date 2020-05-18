package marmot.command;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import marmot.MarmotRuntime;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotAnalysis.Type;
import marmot.exec.MarmotExecution;
import marmot.exec.MarmotExecution.State;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.PicocliSubCommand;
import utils.UnitUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotExecutionCommands {
	@Command(name="show", description="show a MarmotExecution")
	public static class Show extends PicocliSubCommand<MarmotRuntime> {
		@Parameters(paramLabel="id", index="0", arity = "1..1", description={"execution id"})
		private String m_id;
		
		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			MarmotExecution exec = initialContext.getMarmotExecution(m_id);
			System.out.println(exec);
		}
	}
	
	@Command(name="list", description="list MarmotExecutions")
	public static class ListExecs extends PicocliSubCommand<MarmotRuntime> {
		@Option(names={"-s", "-state"}, paramLabel="state",
				description="list all executions in a particular state")
		private String m_stateStr;
		
		@Option(names={"-a", "-analysis"}, paramLabel="type",
				description="list all executions of a particular analysis type (NONE for non-analysis executions)")
		private String m_analTypeStr;

		@Option(names={"-t", "-time"}, paramLabel="duration", description="extinct time after finish")
		private String m_timeSpanStr;

		@Option(names={"-r", "-recur"}, paramLabel="duration", description="recurring duration")
		private String m_recurPeriod;

		@Option(names={"-l"}, description="list in detail")
		private boolean m_details;
		
		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			if ( m_recurPeriod != null ) {
				long period = UnitUtils.parseDuration(m_recurPeriod);
				ScheduledExecutorService exector = Executors.newSingleThreadScheduledExecutor();
				exector.scheduleAtFixedRate(()-> {
					show(initialContext);
					System.out.println("-----------------------------------------------------------------------------------------");
				}, 0, period, TimeUnit.MILLISECONDS);
			}
			else {
				show(initialContext);
			}
		}
		
		private void show(MarmotRuntime marmot) {
			List<MarmotExecution> execList = marmot.getMarmotExecutionAll();
			if ( m_timeSpanStr != null ) {
				long dur = UnitUtils.parseDuration(m_timeSpanStr);
				long now = System.currentTimeMillis();
				execList = FStream.from(execList)
									.filter(exec -> exec.isRunning() || (now - exec.getFinishedTime()) < dur)
									.toList();
			}
			
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
						exec.getMarmotAnalysis().ifAbsent(() -> matcheds.add(exec));
					}
					execList = matcheds;
				}
				else if ( "ALL".equals(m_analTypeStr.toUpperCase()) ) {
					execList = FStream.from(execList)
										.filter(exec -> exec.getMarmotAnalysis().isPresent())
										.toList();
				}
				else {
					Type type = Type.valueOf(m_analTypeStr.toUpperCase());
					if ( type == null ) {
						throw new IllegalArgumentException("invalid '-analysis' option value: " + m_analTypeStr);
					}
					
					execList = FStream.from(execList)
									.filter(exec -> type == exec.getMarmotAnalysis()
																.map(MarmotAnalysis::getType)
																.getOrNull())
									.toList();
				}
			}
			
			int index = 1;
			for ( MarmotExecution exec: execList ) {
				if ( m_details ) {
					System.out.printf("%03d: %s%n", index, exec);
				}
				else {
					System.out.printf("%03d: %11s%s%n", index, exec.getId(),
										exec.getMarmotAnalysis()
											.map(anal -> String.format(", %s", anal.getId()))
											.getOrElse(""));
				}
				
				++index;
			}
		}
	}

	@Command(name="cancel", description="cancel a running MarmotExecution")
	public static class Cancel extends PicocliSubCommand<MarmotRuntime> {
		@Parameters(paramLabel="id", arity = "1..1", description={"execution id"})
		private String m_id;
		
		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			MarmotExecution exec = initialContext.getMarmotExecution(m_id);
			exec.cancel();
		}
	}
}
