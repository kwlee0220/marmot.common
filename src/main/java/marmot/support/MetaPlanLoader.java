package marmot.support;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FilenameUtils;

import io.vavr.control.Option;
import marmot.Plan;
import marmot.plan.STScriptPlanLoader;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MetaPlanLoader {
	private static final String ST_PLAN_SUFFIX = "meta.st";
	private static final String JSON_PLAN_SUFFIX = "meta.json";
	
	public static Option<Plan> load(File start) throws IOException {
		Option<Plan> plan = tryToLoadTemplatePlan(start);
		if ( plan.isEmpty() ) {
			plan = tryToLoadJsonPlan(start);
		}
		
		return plan;
	}
	
	private static Option<Plan> tryToLoadTemplatePlan(File start) throws IOException {
		File metaFile = getMetaPlanFile(start, ST_PLAN_SUFFIX);
		if ( metaFile.exists() ) {
			return Option.some(STScriptPlanLoader.load(metaFile));
		}
		else {
			return Option.none();
		}
	}
	
	private static Option<Plan> tryToLoadJsonPlan(File start) throws IOException {
		File metaFile = getMetaPlanFile(start, JSON_PLAN_SUFFIX);
		if ( metaFile.exists() ) {
			try ( Reader reader = new InputStreamReader(new FileInputStream(metaFile),
														StandardCharsets.UTF_8) ) {
				return Option.some(Plan.parseJson(reader));
			}
		}
		else {
			return Option.none();
		}
	}
	
	private static File getMetaPlanFile(File start, String suffix) {
		if ( start.isDirectory() ) {
			return new File(start, "_" + suffix);
		}
		else {
			String fileName = start.getAbsolutePath();
			fileName = String.format("%s%s.%s", FilenameUtils.getFullPath(fileName),
									FilenameUtils.getBaseName(fileName), suffix);
			return new File(fileName);
		}
	}
}