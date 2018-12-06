package marmot.externio.geojson;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.geo.geotools.GeoToolsUtils;
import marmot.geo.geotools.SimpleFeatureRecordSet;
import marmot.rset.ConcatedRecordSet;
import utils.Unchecked;
import utils.io.FileUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiFileGeoJsonRecordSet extends ConcatedRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(MultiFileGeoJsonRecordSet.class);
	
	private final FStream<File> m_files;
	private final Charset m_charset;
	private SimpleFeatureRecordSet m_first;
	private final RecordSchema m_schema;
	
	public MultiFileGeoJsonRecordSet(File start, Charset charset) {
		setLogger(s_logger);
		
		try {
			List<File> files = FileUtils.walk(start, "**/*.geojson").toList();
			if ( files.isEmpty() ) {
				throw new IllegalArgumentException("no GeoJson files to read: path=" + start);
			}
			
			getLogger().info("loading GeoJsonFile: from={}, nfiles={}", start, files.size());

			m_files = FStream.of(files);
			m_charset = charset;
			
			m_first = parseGeoJson(m_files.next().get(), m_charset);
			m_schema = m_first.getRecordSchema();
		}
		catch ( IOException e ) {
			throw new RecordSetException("fails to parse GeoJSON, cause=" + e);
		}
	}

	@Override
	protected void closeInGuard() {
		if ( m_first != null ) {
			m_first.closeQuietly();
			m_first = null;
		}
		m_files.closeQuietly();
		
		super.closeInGuard();
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	protected RecordSet loadNext() {
		if ( m_first != null ) {
			RecordSet rset = m_first;
			m_first = null;
			
			return rset;
		}
		else {
			return m_files.next()
							.map(file -> Unchecked.supplyRTE(() -> parseGeoJson(file, m_charset)))
							.getOrNull();
		}
	}
	
	public static SimpleFeatureRecordSet parseGeoJson(BufferedReader reader) throws IOException {
        FeatureJSON fjson = new FeatureJSON(new GeometryJSON());

		FeatureIterator<SimpleFeature> iter = fjson.streamFeatureCollection(reader);
		return GeoToolsUtils.toRecordSet(iter);
	}
	
	public static SimpleFeatureRecordSet parseGeoJson(File file, Charset charset)
		throws IOException {
		BufferedReader reader = Files.newBufferedReader(file.toPath(), charset);
		return parseGeoJson(reader);
	}
}
