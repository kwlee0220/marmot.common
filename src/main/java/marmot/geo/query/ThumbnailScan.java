package marmot.geo.query;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.InsufficientThumbnailException;
import marmot.RecordSet;
import marmot.ThumbnailNotFoundException;
import utils.LoggerSettable;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ThumbnailScan implements LoggerSettable {
	private final DataSet m_ds;
	private final Envelope m_range;
	private final int m_sampleCount;
	private Logger m_logger;
	
	public static ThumbnailScan on(DataSet ds, Envelope range, long sampleCount) {
		return new ThumbnailScan(ds, range, sampleCount);
	}
	
	private ThumbnailScan(DataSet ds, Envelope range, long sampleCount) {
		Utilities.checkNotNullArgument(ds, "DataSet");
		
		m_ds = ds;
		m_range = range;
		m_sampleCount = (int)sampleCount;
		
		m_logger = LoggerFactory.getLogger(ThumbnailScan.class);
	}

	public RecordSet run() throws ThumbnailNotFoundException, InsufficientThumbnailException, IOException {
		return m_ds.readThumbnail(m_range, m_sampleCount);
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
}
