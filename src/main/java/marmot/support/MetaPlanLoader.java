package marmot.support;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Plan;
import marmot.plan.STScriptPlanLoader;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MetaPlanLoader {
	private static final Logger s_logger = LoggerFactory.getLogger(MetaPlanLoader.class);
	
	private static final String ST_PLAN_SUFFIX = "meta.st";
	private static final String JSON_PLAN_SUFFIX = "meta.json";
	
	public static FOption<Plan> load(File start) throws IOException {
		FOption<Plan> plan = tryToLoadTemplatePlan(start);
		if ( plan.isAbsent() ) {
			plan = tryToLoadJsonPlan(start);
		}
		
		return plan;
	}
	
	private static FOption<Plan> tryToLoadTemplatePlan(File start) throws IOException {
		File metaFile = getMetaPlanFile(start, ST_PLAN_SUFFIX);
		if ( metaFile.exists() ) {
			return FOption.of(STScriptPlanLoader.load(metaFile));
		}
		else {
			s_logger.warn("fails to find StringTemplate plan file=" + start);
			return FOption.empty();
		}
	}
	
	private static FOption<Plan> tryToLoadJsonPlan(File start) throws IOException {
		File metaFile = getMetaPlanFile(start, JSON_PLAN_SUFFIX);
		if ( metaFile.exists() ) {
			try ( Reader reader = new InputStreamReader(new FileInputStream(metaFile),
														StandardCharsets.UTF_8) ) {
				return FOption.of(Plan.parseJson(reader));
			}
		}
		else {
			return FOption.empty();
		}
	}
	
	private static File getMetaPlanFile(File start, String suffix) {
		if ( start.isDirectory() ) {
			return new File(start, "_" + suffix);
		}
		else if ( start.getName().endsWith(suffix) ) {
			return start;
		}
		else {
			String fileName = start.getAbsolutePath();
			fileName = String.format("%s%s.%s", FilenameUtils.getFullPath(fileName),
									FilenameUtils.getBaseName(fileName), suffix);
			return new File(fileName);
		}
	}
}