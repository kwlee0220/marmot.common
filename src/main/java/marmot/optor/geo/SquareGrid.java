package marmot.optor.geo;

import java.util.List;
import java.util.Map;

import com.vividsolutions.jts.geom.Envelope;

import io.vavr.control.Either;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.geo.GeoClientUtils;
import marmot.plan.Group;
import marmot.proto.Size2dProto;
import marmot.proto.optor.SquareGridProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.CSV;
import utils.KeyValue;
import utils.Size2d;
import utils.UnitUtils;
import utils.Utilities;
import utils.stream.KVFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SquareGrid implements PBSerializable<SquareGridProto> {
	private final Either<String, Envelope> m_gridBounds;
	private final Size2d m_cellSize;
	
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
	
	public Either<String, Envelope> getGridBounds() {
		return m_gridBounds;
	}
	
	public Envelope getGridBounds(MarmotRuntime marmot) {
		if ( m_gridBounds.isRight() ) {
			return m_gridBounds.right().get();
		}
		else {
			DataSet ds = marmot.getDataSet(m_gridBounds.getLeft());
			return ds.getBounds();
		}
	}
	
	public Size2d getCellSize() {
		return m_cellSize;
	}
	
	@Override
	public String toString() {
		if ( m_gridBounds.isLeft() ) {
			return String.format("dataset=%s;cell=%s",
								m_gridBounds.getLeft(), toString(m_cellSize));
		}
		else {
			Envelope bounds = m_gridBounds.right().get();
			return String.format("bounds=%s;cell=%s",
								GeoClientUtils.toString(bounds), toString(m_cellSize));
		}
	}
	
	private String toString(Size2d size) {
		return String.format("%sx%s", UnitUtils.toMeterString(size.getWidth()),
										UnitUtils.toMeterString(size.getHeight()));
	}
	
	public static SquareGrid parseString(String expr) {
		Utilities.checkNotNullArgument(expr, "SquareGrid string is null");
	
		Map<String,String> kvMap = CSV.parseCsv(expr, ';')
										.map(SquareGrid::parseKeyValue)
										.toMap(KeyValue::key, KeyValue::value);
		
		String cellExpr = kvMap.get("cell");
		if ( cellExpr == null ) {
			throw new IllegalArgumentException("cell is absent: expr=" + expr);
		}
		Size2d cell = Size2d.fromString(cellExpr);
		
		String boundsExpr = kvMap.get("bounds");
		if ( boundsExpr != null ) {
			return new SquareGrid(GeoClientUtils.parseEnvelope(boundsExpr).get(), cell);
		}
		
		String dsId = kvMap.get("dataset");
		if ( dsId != null ) {
			return new SquareGrid(dsId, cell);
		}
		
		throw new IllegalArgumentException("invalid SquareGrid string: " + expr);
	}
	
	private static KeyValue<String,String> parseKeyValue(String expr) {
		List<String> parts = CSV.parseCsv(expr, '=')
								.map(String::trim)
								.toList();
		if ( parts.size() != 2 ) {
			throw new IllegalArgumentException("invalid key-value: " + expr);
		}
		
		return KeyValue.of(parts.get(0), parts.get(1));
	}

	public static SquareGrid fromProto(SquareGridProto proto) {
		Size2dProto sizeProto = proto.getCellSize();
		Size2d cellSize = new Size2d(sizeProto.getWidth(), sizeProto.getHeight());
		
		switch ( proto.getGridBoundsCase() ) {
			case DATASET:
				return new SquareGrid(proto.getDataset(), cellSize);
			case BOUNDS:
				return new SquareGrid(PBUtils.fromProto(proto.getBounds()), cellSize);
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
									.build();
		}
		else {
			return SquareGridProto.newBuilder()
									.setBounds(PBUtils.toProto(m_gridBounds.right().get()))
									.setCellSize(PBUtils.toProto(m_cellSize))
									.build();
		}
	}
}
