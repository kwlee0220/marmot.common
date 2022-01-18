package marmot.optor.geo;

import java.util.Map;

import org.locationtech.jts.geom.Envelope;

import marmot.MarmotRuntime;
import marmot.dataset.DataSet;
import marmot.geo.GeoClientUtils;
import marmot.proto.Size2dProto;
import marmot.proto.optor.SquareGridProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.CSV;
import utils.Size2d;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.Either;
import utils.func.KeyValue;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SquareGrid implements PBSerializable<SquareGridProto> {
	private final Either<String, Envelope> m_gridBounds;
	private final Size2d m_cellSize;
	private double m_margin = 0;
	
	public SquareGrid(String dsId, Size2d cellSize) {
		Utilities.checkNotNullArgument(dsId, "dataset id should not be null");
		Utilities.checkNotNullArgument(cellSize, "Grid cell size should not be null");

		m_gridBounds = Either.left(dsId);
		m_cellSize = cellSize;
	}
	
	public SquareGrid(Envelope bounds, Size2d cellSize) {
		Utilities.checkNotNullArgument(bounds, "Universe Envelope should not be null");
		Utilities.checkNotNullArgument(cellSize, "Grid cell size should not be null");
		
		m_gridBounds = Either.right(bounds);
		m_cellSize = cellSize;
	}
	
	private SquareGrid(Either<String, Envelope> bounds, Size2d cellSize, double margin) {
		m_gridBounds = bounds;
		m_cellSize = cellSize;
		m_margin = margin;
	}
	
	public double margin() {
		return m_margin;
	}
	
	public SquareGrid margin(double margin) {
		return new SquareGrid(m_gridBounds, m_cellSize, margin);
	}
	
	public Either<String, Envelope> getGridBounds() {
		return m_gridBounds;
	}
	
	public Envelope getGridBounds(MarmotRuntime marmot) {
		Envelope bounds = null;
		if ( m_gridBounds.isRight() ) {
			bounds = m_gridBounds.right().get();
		}
		else {
			DataSet ds = marmot.getDataSet(m_gridBounds.getLeft());
			bounds = ds.getBounds();
		}
		
		if ( m_margin > 0 ) {
			bounds.expandBy(m_margin);
		}
		
		return bounds;
	}
	
	public Size2d getCellSize() {
		return m_cellSize;
	}
	
	@Override
	public String toString() {
		String marginStr = (m_margin > 0) ? String.format(",margin=%.3f", m_margin) : "";
		
		if ( m_gridBounds.isLeft() ) {
			return String.format("dataset=%s;cell=%s%s",
								m_gridBounds.getLeft(), toString(m_cellSize), marginStr);
		}
		else {
			Envelope bounds = m_gridBounds.right().get();
			return String.format("bounds=%s;cell=%s%s",
								GeoClientUtils.toString(bounds), toString(m_cellSize), marginStr);
		}
	}
	
	private String toString(Size2d size) {
		return String.format("%sx%s", UnitUtils.toMeterString(size.getWidth()),
										UnitUtils.toMeterString(size.getHeight()));
	}
	
	public static SquareGrid parseString(String expr) {
		Utilities.checkNotNullArgument(expr, "SquareGrid string is null");
	
		Map<String,String> kvMap = CSV.parseCsv(expr, ';')
										.map(KeyValue::parse)
										.toMap(KeyValue::key, KeyValue::value);
		
		String cellExpr = kvMap.get("cell");
		if ( cellExpr == null ) {
			throw new IllegalArgumentException("cell is absent: expr=" + expr);
		}
		Size2d cell = Size2d.fromString(cellExpr);
		
		double margin = 0;
		String marginExpr = kvMap.get("margin");
		if ( marginExpr != null ) {
			margin = UnitUtils.parseLengthInMeter(marginExpr);
		}
		
		String boundsExpr = kvMap.get("bounds");
		if ( boundsExpr != null ) {
			return new SquareGrid(GeoClientUtils.parseEnvelope(boundsExpr).get(), cell).margin(margin);
		}
		
		String dsId = kvMap.get("dataset");
		if ( dsId != null ) {
			return new SquareGrid(dsId, cell).margin(margin);
		}
		
		throw new IllegalArgumentException("invalid SquareGrid string: " + expr);
	}

	public static SquareGrid fromProto(SquareGridProto proto) {
		Size2dProto sizeProto = proto.getCellSize();
		Size2d cellSize = new Size2d(sizeProto.getWidth(), sizeProto.getHeight());
		
		double margin = 0;
		switch ( proto.getOptionalMarginCase() ) {
			case MARGIN:
				margin = proto.getMargin();
				break;
			case OPTIONALMARGIN_NOT_SET:
				margin = 0;
				break;
		}
		
		switch ( proto.getGridBoundsCase() ) {
			case DATASET:
				return new SquareGrid(proto.getDataset(), cellSize).margin(margin);
			case BOUNDS:
				return new SquareGrid(PBUtils.fromProto(proto.getBounds()), cellSize).margin(margin);
			default:
				throw new AssertionError();
		}
	}

	@Override
	public SquareGridProto toProto() {
		if ( m_gridBounds.isLeft() ) {
			return SquareGridProto.newBuilder()
									.setDataset(m_gridBounds.getLeft())
									.setCellSize(PBUtils.toProto(m_cellSize))
									.setMargin(m_margin)
									.build();
		}
		else {
			return SquareGridProto.newBuilder()
									.setBounds(PBUtils.toProto(m_gridBounds.right().get()))
									.setCellSize(PBUtils.toProto(m_cellSize))
									.setMargin(m_margin)
									.build();
		}
	}
}
