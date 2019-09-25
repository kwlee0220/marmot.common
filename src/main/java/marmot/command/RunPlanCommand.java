package marmot.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import marmot.Plan;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.io.FileUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RunPlanCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private UsageHelp m_help;

	@Parameters(paramLabel="plan_file", index="0", arity="1..1",
				description="plan file to run")
	private String m_filePath;
	
	@Option(names={"-s", "-schema"}, description={"print schema info only"})
	private boolean m_schemaOnly;
	
	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		File planFile = new File(m_filePath);
		String ext = FileUtils.getExtension(planFile).toLowerCase();
		
		Plan plan = null;
		switch ( ext ) {
			case "json":
				plan = loadJsonPlan(marmot, planFile);
				break;
			default:
				throw new UnsupportedOperationException("invalid file extenstion: " + ext);
		}
		
		if ( m_schemaOnly ) {
			System.out.println(marmot.getOutputRecordSchema(plan));
		}
		else {
			marmot.execute(plan);
		}
	}
	
	private static Plan loadJsonPlan(MarmotRuntime marmot, File planFile)
		throws FileNotFoundException, IOException {
		try ( Reader reader = new BufferedReader(new FileReader(planFile)) ) {
			return Plan.parseJson(reader);
		}
	}
}
