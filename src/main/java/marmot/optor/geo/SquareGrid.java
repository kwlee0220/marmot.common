package marmot.optor.geo;

import com.vividsolutions.jts.geom.Envelope;

import io.vavr.control.Either;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.proto.Size2dProto;
import marmot.proto.optor.SquareGridProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.Size2d;
import utils.Utilities;

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
