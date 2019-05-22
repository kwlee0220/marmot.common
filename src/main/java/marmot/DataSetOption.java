package marmot;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import marmot.proto.service.DataSetOptionsProto;
import marmot.protobuf.PBUtils;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class DataSetOption {
	public static final ForceOption FORCE = new ForceOption();
	public static final AppendOption APPEND = new AppendOption();
	public static final CompressOption COMPRESS = new CompressOption();
	
	public abstract void set(DataSetOptionsProto.Builder builder);
	
	public static BlockSizeOption BLOCK_SIZE(long size) {
		return new BlockSizeOption(size);
	}
	public static BlockSizeOption BLOCK_SIZE(String size) {
		Utilities.checkNotNullArgument(size, "block-size is null");
		
		return new BlockSizeOption(UnitUtils.parseByteSize(size));
	}
	
	public static GeomColumnInfoOption GEOMETRY(GeometryColumnInfo info) {
		Utilities.checkNotNullArgument(info, "GeometryColumnInfo is null");
		
		return new GeomColumnInfoOption(info);
	}
	public static GeomColumnInfoOption GEOMETRY(String geomCol, String srid) {
		Utilities.checkNotNullArgument(geomCol, "Geometry column name is null");
		Utilities.checkNotNullArgument(srid, "Geometry SRID is null");
		
		return new GeomColumnInfoOption(geomCol, srid);
	}
	
	public static MetadataOption META_DATA(Map<String,String> metadata) {
		return new MetadataOption(metadata);
	}
	
	public static boolean hasForce(List<DataSetOption> opts) {
		return FStream.from(opts)
						.castSafely(ForceOption.class)
						.next()
						.isPresent();
	}
	
	public static boolean hasAppend(List<DataSetOption> opts) {
		return FStream.from(opts)
						.castSafely(AppendOption.class)
						.next()
						.isPresent();
	}
	
	public static boolean hasCompression(List<DataSetOption> opts) {
		return FStream.from(opts)
						.castSafely(CompressOption.class)
						.next()
						.isPresent();
	}
	
	public static FOption<GeometryColumnInfo> getGeometryColumnInfo(List<DataSetOption> opts) {
		return FStream.from(opts)
						.castSafely(GeomColumnInfoOption.class)
						.next()
						.map(GeomColumnInfoOption::get);
	}
	
	public static FOption<Long> getBlockSize(List<DataSetOption> opts) {
		return FStream.from(opts)
						.castSafely(BlockSizeOption.class)
						.next()
						.map(BlockSizeOption::get);
	}

	public static List<DataSetOption> fromProto(DataSetOptionsProto proto) {
		List<DataSetOption> opts = Lists.newArrayList();

		switch ( proto.getOptionalGeomColInfoCase() ) {
			case GEOM_COL_INFO:
				opts.add(GEOMETRY(GeometryColumnInfo.fromProto(proto.getGeomColInfo())));
				break;
			default:
		}
		switch ( proto.getOptionalForceCase() ) {
			case FORCE:
				opts.add(FORCE);
				break;
			default:
		}
		switch ( proto.getOptionalAppendCase()) {
			case APPEND:
				opts.add(APPEND);
				break;
			default:
		}
		switch ( proto.getOptionalBlockSizeCase() ) {
			case BLOCK_SIZE:
				opts.add(BLOCK_SIZE(proto.getBlockSize()));
				break;
			default:
		}
		switch ( proto.getOptionalCompressCase() ) {
			case COMPRESS:
				opts.add(COMPRESS);
				break;
			default:
		}
		switch ( proto.getOptionalMetadataCase() ) {
			case METADATA:
				Map<String,String> metadata = PBUtils.fromProto(proto.getMetadata());
				opts.add(META_DATA(metadata));
				break;
			default:
		}
		
		return opts;
	}
	
	public static DataSetOptionsProto toProto(List<? extends DataSetOption> opts) {
		Utilities.checkNotNullArgument(opts, "GeomOpOption list is null");
		
		List<DataSetOption> matcheds = FStream.from(opts)
											.castSafely(DataSetOption.class)
											.toList();
		return FStream.from(matcheds)
					.collectLeft(DataSetOptionsProto.newBuilder(), (b,o) -> o.set(b))
					.build();
	}
	
	public static class ForceOption extends DataSetOption {
		@Override
		public void set(DataSetOptionsProto.Builder builder) {
			builder.setForce(true);
		}
		
		@Override
		public String toString() {
			return "force";
		}
	}
	
	public static class AppendOption extends DataSetOption {
		@Override
		public void set(DataSetOptionsProto.Builder builder) {
			builder.setAppend(true);
		}
		
		@Override
		public String toString() {
			return "append";
		}
	}
	
	public static class BlockSizeOption extends DataSetOption {
		private final long m_size;
		
		private BlockSizeOption(long count) {
			m_size = count;
		}
		
		public long get() {
			return m_size;
		}
		
		public void set(DataSetOptionsProto.Builder builder) {
			builder.setBlockSize(m_size);
		}
		
		@Override
		public String toString() {
			return String.format("block_size=%s", UnitUtils.toByteSizeString(m_size));
		}
	}
	
	public static class GeomColumnInfoOption extends DataSetOption {
		private final GeometryColumnInfo m_geomInfo;
		
		private GeomColumnInfoOption(String geomCol, String srid) {
			Utilities.checkNotNullArgument(geomCol, "Geometry column name is null");
			Utilities.checkNotNullArgument(srid, "SRID is null");
			
			m_geomInfo = new GeometryColumnInfo(geomCol, srid);
		}
		
		private GeomColumnInfoOption(GeometryColumnInfo geomInfo) {
			Utilities.checkNotNullArgument(geomInfo, "GeometryColumnInfo is null");
			
			m_geomInfo = geomInfo;
		}
		
		public GeometryColumnInfo get() {
			return m_geomInfo;
		}
		
		@Override
		public void set(DataSetOptionsProto.Builder builder) {
			builder.setGeomColInfo(m_geomInfo.toProto());
		}
		
		@Override
		public String toString() {
			return String.format("geom_col=%s", m_geomInfo);
		}
	}
	
	public static class CompressOption extends DataSetOption {
		public void set(DataSetOptionsProto.Builder builder) {
			builder.setCompress(true);
		}
		
		@Override
		public String toString() {
			return "compressed";
		}
	}
	
	public static class MetadataOption extends DataSetOption {
		private final Map<String,String> m_metadata;
		
		private MetadataOption(Map<String,String> metadata) {
			Utilities.checkNotNullArgument(metadata, "metadata is null");
			Preconditions.checkArgument(metadata.size() > 0, "metadata is empty");
			
			m_metadata = Maps.newHashMap(metadata);
		}
		
		public Map<String,String> get() {
			return m_metadata;
		}
		
		public void set(DataSetOptionsProto.Builder builder) {
			builder.setMetadata(PBUtils.toProto(m_metadata));
		}
		
		@Override
		public String toString() {
			return String.format("meta=%s", m_metadata);
		}
	}
}
