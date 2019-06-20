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
import marmot.geo.geotools.SimpleFeatures;
import marmot.rset.ConcatedRecordSet;
import utils.func.FOption;
import utils.io.FileUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiFileGeoJsonRecordSet extends ConcatedRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(MultiFileGeoJsonRecordSet.class);
	private static final String GEOJSON_GEOM_COLNAME = "geometry";
	
	private final FStream<File> m_files;
	private final String m_geomColName;
	private final Charset m_charset;
	private RecordSet m_first;
	private final RecordSchema m_schema;
	
	public MultiFileGeoJsonRecordSet(File start, String geomColName, Charset charset) {
		setLogger(s_logger);
		
		try {
			List<File> files = FileUtils.walk(start, "**/*.geojson").toList();
			if ( files.isEmpty() ) {
				throw new IllegalArgumentException("no GeoJson files to read: path=" + start);
			}
			
			getLogger().info("loading GeoJsonFile: from={}, nfiles={}", start, files.size());

			m_files = FStream.from(files);
			m_geomColName = geomColName;
			m_charset = charset;
			
			m_first = parseGeoJson(m_files.next().get(), m_geomColName, m_charset);
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
			FOption<File> next = m_files.next();
			if ( next.isPresent() ) {
				try {
					return parseGeoJson(next.getUnchecked(), m_geomColName, m_charset);
				}
				catch ( IOException e ) {
					String msg = String.format("fails to load GeoJSON file: path=%s, details=%s",
												next.getUnchecked(), e);
					throw new RecordSetException(msg);
				}
			}
			else {
				return null;
			}
		}
	}
	
	public static RecordSet parseGeoJson(BufferedReader reader, String geomColName)
		throws IOException {
        FeatureJSON fjson = new FeatureJSON(new GeometryJSON());

		FeatureIterator<SimpleFeature> iter = fjson.streamFeatureCollection(reader);
		RecordSet rset = SimpleFeatures.toRecordSet(iter);
		if ( !GEOJSON_GEOM_COLNAME.equals(geomColName) ) {
			rset = rset.renameColumn(GEOJSON_GEOM_COLNAME, geomColName);
		}
		return rset;
	}
	
	private static RecordSet parseGeoJson(File file, String geomColName, Charset charset)
		throws IOException {
		BufferedReader reader = Files.newBufferedReader(file.toPath(), charset);
		return parseGeoJson(reader, geomColName);
	}
}
