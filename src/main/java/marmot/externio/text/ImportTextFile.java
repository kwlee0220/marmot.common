package marmot.externio.text;

import java.io.File;

import io.vavr.control.Option;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.ImportParameters;
import marmot.externio.ImportPlanSupplier;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportTextFile extends ImportIntoDataSet {
	private final File m_start;
	private final TextLineParameters m_txtParams;

	public ImportTextFile(File start, TextLineParameters txtParams, ImportParameters importParams) {
		super(importParams);
		
		m_start = start;
		m_txtParams = txtParams;
	}

	@Override
	protected RecordSet loadRecordSet(MarmotRuntime marmot) {
		return new TextLineFileRecordSet(m_start, m_txtParams);
	}

	@Override
	protected Option<Plan> loadImportPlan(MarmotRuntime marmot) {
		return ImportPlanSupplier.from(m_start).get();
	}
}
