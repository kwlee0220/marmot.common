package marmot.geo.geotools;

import java.util.Map;

import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Geometry;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.geo.CRSUtils;
import marmot.rset.AbstractRecordSet;
import marmot.support.DataUtils;
import marmot.type.DataType;
import utils.Utilities;
import utils.func.Try;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SimpleFeatureRecordSet extends AbstractRecordSet {
	private final FeatureIterator<SimpleFeature> m_reader;
	private final RecordSchema m_schema;
	private final DataType[] m_types;
	
	private SimpleFeature m_first = null;
	private CoordinateReferenceSystem m_crs;
	private String m_srid;
	
	public SimpleFeatureRecordSet(FeatureIterator<SimpleFeature> iter)
		throws FactoryException {
		Preconditions.checkArgument(iter != null && iter.hasNext());

		m_first = iter.next();
		SimpleFeatureType sfType = m_first.getFeatureType();
		
		try {
			m_schema = SimpleFeatures.toRecordSchema(sfType);
			m_types = m_schema.getColumns().stream()
								.map(Column::type)
								.toArray(sz -> new DataType[sz]);
			
			m_crs = sfType.getCoordinateReferenceSystem();
			if ( m_crs != null ) {
				String srid = CRS.lookupIdentifier(m_crs, true);
				if ( srid == null ) {
					srid = CRSUtils.toEPSG(CRS.toSRS(m_crs));
				}
				m_srid = srid;
			}
			else {
				m_srid = null;
			}
		}
		catch ( Throwable e ) {
			throw new RecordSetException("fails to read SimpleFeatureCollection: type="
											+ sfType, e);
		}

		m_reader = iter;
		setLogger(LoggerFactory.getLogger(SimpleFeatureRecordSet.class));
	}
	
	public SimpleFeatureRecordSet(SimpleFeatureType sfType, FeatureIterator<SimpleFeature> iter)
		throws FactoryException {
		try {
			m_schema = SimpleFeatures.toRecordSchema(sfType);
			m_types = m_schema.getColumns().stream()
								.map(Column::type)
								.toArray(sz -> new DataType[sz]);
			
			m_crs = sfType.getCoordinateReferenceSystem();
			if ( m_crs != null ) {
				String srid = CRS.lookupIdentifier(m_crs, true);
				if ( srid == null ) {
					srid = CRSUtils.toEPSG(CRS.toSRS(m_crs));
				}
				m_srid = srid;
			}
			else {
				m_srid = null;
			}
		}
		catch ( Throwable e ) {
			throw new RecordSetException("fails to read SimpleFeatureCollection: type="
											+ sfType, e);
		}

		m_reader = iter;
		setLogger(LoggerFactory.getLogger(SimpleFeatureRecordSet.class));
	}
	
	public SimpleFeatureRecordSet(FeatureCollection<SimpleFeatureType,SimpleFeature> sfColl)
		throws FactoryException  {
		this(sfColl.getSchema(), sfColl.features());
	}
	
	@Override
	protected void closeInGuard() {
		Try.run(m_reader::close);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public String getSRID() {
		return m_srid;
	}
	
	public void setSRID(String srid) {
		Utilities.checkNotNullArgument(srid, "SRID");
		
		m_srid = srid;
		m_crs = CRSUtils.toCRS(m_srid);
	}
	
	public CoordinateReferenceSystem getCRS() {
		return m_crs;
	}
	
	public void setCRS(CoordinateReferenceSystem crs) {
		Utilities.checkNotNullArgument(crs, "CoordinateReferenceSystem");
		
		m_crs = crs;
		m_srid = CRSUtils.toEPSG(CRS.toSRS(m_crs));
	}
	
	@Override
	public boolean next(Record output) throws RecordSetException {
		checkNotClosed();
		
		if ( !m_reader.hasNext() ) {
			return false;
		}
		
		SimpleFeature feature;
		if ( m_first != null ) {
			feature = m_first;
			m_first = null;
		}
		else {
			feature = m_reader.next();
		}
		
		for ( int i =0; i < getRecordSchema().length(); ++i ) {
			Object value = feature.getAttribute(i);
			
			value = DataUtils.cast(value, m_types[i]);
			output.set(i, value);
		}
		
		return true;
	}
	
	private String buildNonGeomColumnsString(SimpleFeature feature) {
		Map<String,Object> attrs = Maps.newLinkedHashMap();
		
		for ( int idx =0; idx < feature.getAttributeCount(); ++idx ) {
			Object attr = feature.getAttribute(idx);
			if ( !(attr instanceof Geometry) ) {
				String name = feature.getFeatureType().getDescriptor(idx).getLocalName();
				attrs.put(name, attr);
			}
		}
		
		return attrs.toString();
	}
}