package marmot.externio;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import org.apache.commons.io.FilenameUtils;

import io.vavr.control.Option;
import marmot.Plan;
import utils.Throwables;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportPlanSupplier implements Supplier<Option<Plan>> {
	private static final String PLAN_FILE_NAME = "_meta.json";
	
	private final File m_planFile;
	
	public static ImportPlanSupplier from(File from) {
		File metaFile;
		if ( from.isDirectory() ) {
			metaFile = new File(from, PLAN_FILE_NAME);
		}
		else {
			String fileName = from.getAbsolutePath();
			fileName = String.format("%s%s%s", FilenameUtils.getFullPath(fileName),
									FilenameUtils.getBaseName(fileName), PLAN_FILE_NAME);
			metaFile = new File(fileName);
		}
		
		return new ImportPlanSupplier(metaFile);
	}
	
	private ImportPlanSupplier(File planFile) {
		m_planFile = planFile;
	}

	@Override
	public Option<Plan> get() {
		try {
			return m_planFile.exists() ? Option.some(load()) : Option.none();
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}

	private Plan load() throws FileNotFoundException, IOException {
		try ( Reader reader = new InputStreamReader(new FileInputStream(m_planFile),
													StandardCharsets.UTF_8) ) {
			return Plan.parseJson(reader);
		}
	}
}
