package marmot.command;

import marmot.MarmotRuntime;
import picocli.CommandLine;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.Spec;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PicocliCommands {
	public interface PicocliCommand extends Runnable {
		public MarmotRuntime getMarmotRuntime();
	}
	
	public abstract static class SubCommand implements PicocliCommand {
		@ParentCommand private PicocliCommand m_parent;
		@Spec private CommandSpec m_spec;
		@Mixin private UsageHelp m_help;
		
		abstract protected void run(MarmotRuntime marmot) throws Exception;
		
		@Override
		public MarmotRuntime getMarmotRuntime() {
			return m_parent.getMarmotRuntime();
		}
		
		public PicocliCommand getParent() {
			return m_parent;
		}
		
		public CommandLine getCommandLine() {
			return m_spec.commandLine();
		}
		
		@Override
		public void run() {
			ParseResult sub = m_spec.commandLine().getParseResult().subcommand();
			try {
				if ( sub != null ) {
					PicocliCommand subC = (PicocliCommand)sub.commandSpec().userObject();
					subC.run();
				}
				else {
					run(m_parent.getMarmotRuntime());
				}
			}
			catch ( Exception e ) {
				System.err.printf("failed: %s%n%n", e);
				
//				m_spec.commandLine().usage(System.out, Ansi.OFF);
			}
		}
	}

}
