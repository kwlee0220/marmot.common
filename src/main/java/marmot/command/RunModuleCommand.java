package marmot.command;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RunModuleCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private UsageHelp m_help;

	@Parameters(paramLabel="module_id", index="0", arity="1..1",
				description="module id")
	private String m_moduleId;
	
	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		marmot.executeModule(m_moduleId);
	}
}
