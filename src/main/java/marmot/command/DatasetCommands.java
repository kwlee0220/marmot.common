package marmot.command;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import marmot.BindDataSetOptions;
import marmot.Column;
import marmot.DataSet;
import marmot.DataSetType;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.command.PicocliCommands.SubCommand;
import marmot.externio.ExternIoUtils;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.csv.CsvParameters;
import marmot.externio.csv.ExportAsCsv;
import marmot.externio.csv.ImportCsv;
import marmot.externio.geojson.ExportAsGeoJson;
import marmot.externio.geojson.GeoJsonParameters;
import marmot.externio.geojson.ImportGeoJson;
import marmot.externio.jdbc.ImportJdbcTable;
import marmot.externio.jdbc.JdbcParameters;
import marmot.externio.shp.ExportDataSetAsShapefile;
import marmot.externio.shp.ExportRecordSetAsShapefile;
import marmot.externio.shp.ExportShapefileParameters;
import marmot.externio.shp.ImportShapefile;
import marmot.externio.shp.ShapefileParameters;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.ClusterDataSetOptions;
import marmot.optor.AggregateFunction;
import marmot.support.DefaultRecord;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.StopWatch;
import utils.UnitUtils;
import utils.Utilities;
import utils.async.ProgressiveExecution;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DatasetCommands {
	@Command(name="list", description="list datasets")
	public static class List extends SubCommand {
		@Parameters(paramLabel="path", index="0", arity="0..1", description={"dataset folder path"})
		private String m_start;

		@Option(names={"-r"}, description="list all descendant datasets")
		private boolean m_recursive;

		@Option(names={"-l"}, description="list in detail")
		private boolean m_details;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			java.util.List<DataSet> dsList;
			if ( m_start != null ) {
				dsList = marmot.getDataSetAllInDir(m_start, m_recursive);
			}
			else {
				dsList = marmot.getDataSetAll();
			}
			
			for ( DataSet ds: dsList ) {
				System.out.print(ds.getId());
				
				if ( m_details ) {
					System.out.printf(" %s %s", ds.getType(), ds.getHdfsPath());
					if ( ds.hasGeometryColumn() ) {
						System.out.printf(" %s", ds.getGeometryColumnInfo());
						
						if ( ds.isSpatiallyClustered() ) {
							System.out.printf("(clustered)");
						}
					}
				}
				System.out.println();
			}
		}
	}

	@Command(name="show", description="print records of the dataset")
	public static class Show extends SubCommand {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id to print"})
		private String m_dsId;

		@Option(names={"-t", "-type"}, paramLabel="type",
				description="target type: dataset (default), file, thumbnail")
		private String m_type = "dataset";

		@Option(names={"-project"}, paramLabel="column_list", description="selected columns (optional)")
		private String m_cols = null;
		
		@Option(names={"-limit"}, paramLabel="count", description="limit count (optional)")
		private int m_limit = -1;

		@Option(names={"-csv"}, description="display csv format")
		private boolean m_asCsv;

		@Option(names={"-delim"}, paramLabel="character", description="csv delimiter (default: ',')")
		private String m_delim = ",";

		@Option(names={"-g", "-geom"}, description="display geometry columns")
		private boolean m_displayGeom;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			PlanBuilder builder = marmot.planBuilder("list_records");
			switch ( m_type.toLowerCase() ) {
				case "dataset":
					builder = builder.load(m_dsId);
					break;
				case "thumbnail":
					builder = builder.loadMarmotFile("database/thumbnails/" + m_dsId);
					break;
				case "file":
					builder = builder.loadMarmotFile(m_dsId);
					break;
				default:
					throw new IllegalArgumentException("invalid type: '" + m_type + "'");
			}
			
			if ( m_limit > 0 ) {
				builder = builder.take(m_limit);
			}
			if ( m_cols != null ) {
				builder = builder.project(m_cols);
			}
		
			if ( !m_displayGeom ) {
				Plan tmp = builder.build();
				RecordSchema schema = marmot.getOutputRecordSchema(tmp);
				String cols = schema.streamColumns()
									.filter(col -> col.type().isGeometryType())
									.map(Column::name)
									.join(",");
				if ( cols.length() > 0 ) {
					builder.project(String.format("*-{%s}", cols));
				}
			}
			
			try ( RecordSet rset = marmot.executeLocally(builder.build()) ) {
				Record record = DefaultRecord.of(rset.getRecordSchema());
				while ( rset.next(record) ) {
					Map<String,Object> values = record.toMap();
					
					if ( m_asCsv ) {
						System.out.println(toCsv(values.values(), m_delim));
					}
					else {
						System.out.println(values);
					}
				}
			}
		}
		
		private static String toCsv(Collection<?> values, String delim) {
			return values.stream()
						.map(o -> {
							String str = ""+o;
							if ( str.contains(" ") || str.contains(delim) ) {
								str = "\"" + str + "\"";
							}
							return str;
						})
						.collect(Collectors.joining(delim));
		}
	}

	@Command(name="schema", description="print the RecordSchema of the dataset")
	public static class Schema extends SubCommand {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			DataSet info = marmot.getDataSet(m_dsId);

			System.out.println("TYPE         : " + info.getType());
			if ( info.getRecordCount() > 0 ) {
				System.out.println("COUNT        : " + info.getRecordCount());
			}
			else {
				System.out.println("COUNT        : unknown");
			}
			System.out.println("SIZE         : " + UnitUtils.toByteSizeString(info.length()));
			if ( info.hasGeometryColumn() ) {
				System.out.println("GEOMETRY     : " + info.getGeometryColumnInfo().name());
				System.out.println("SRID         : " + info.getGeometryColumnInfo().srid());
			}
			System.out.println("HDFS PATH    : " + info.getHdfsPath());
			System.out.println("COMPRESSION  : " + info.getCompressionCodecName().getOrElse("none"));
			SpatialIndexInfo idxInfo = info.getDefaultSpatialIndexInfo().getOrNull();
			System.out.printf("SPATIAL INDEX: %s%n", (idxInfo != null)
														? idxInfo.getHdfsFilePath() : "none");
			System.out.println("COLUMNS      :");
			info.getRecordSchema().getColumns()
					.stream()
					.forEach(c -> System.out.println("\t" + c));
		}
	}

	@Command(name="move", description="move a dataset to another directory")
	public static class Move extends SubCommand {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"id for the source dataset"})
		private String m_src;
		
		@Parameters(paramLabel="path", description={"path to the destination path"})
		private String m_dest;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			DataSet srcDs = marmot.getDataSet(m_src);
			marmot.moveDataSet(srcDs.getId(), m_dest);
		}
	}

	@Command(name="set_geometry", description="set Geometry column info for a dataset")
	public static class SetGcInfo extends SubCommand {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;
		
		@Parameters(paramLabel="col_name", index="1",
					description={"name for default geometry column"})
		private String m_column;
		
		@Parameters(paramLabel="EPSG_code", index="2",
					description={"EPSG code for default geometry"})
		private String m_srid;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			DataSet ds = marmot.getDataSet(m_dsId);
			
			GeometryColumnInfo gcInfo = new GeometryColumnInfo(m_column, m_srid);
			ds.updateGeometryColumnInfo(FOption.ofNullable(gcInfo));
		}
	}

	@Command(name="count", description="count records of the dataset")
	public static class Count extends SubCommand {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			Plan plan = marmot.planBuilder("count records")
								.load(m_dsId)
								.aggregate(AggregateFunction.COUNT())
								.build();
			System.out.println(marmot.executeToLong(plan).get());
		}
	}

	@Command(name="cluster",
			subcommands= {
				CreateCluster.class,
				DeleteCluster.class,
				ShowCluster.class,
				DrawCluster.class,
			},
			description="spatial-cluster related commands")
	public static class Cluster extends SubCommand {
		@Override
		public void run(MarmotRuntime marmot) throws Exception { }
	}

	@Command(name="create", description="cluster the dataset")
	public static class CreateCluster extends SubCommand {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;
		
		@Option(names="-sample_ratio", paramLabel="ratio", description="sampling ratio (0:1]")
		private double m_sampleRatio;
		
		@Option(names="-fill_ratio", paramLabel="ratio", description="max block-fill ratio")
		private double m_fillRatio;

		@Option(names= {"-b", "-block_size"}, paramLabel="nbytes", description="cluster size (eg: '64mb')")
		private void setBlockSize(String blkSizeStr) {
			m_blkSize = UnitUtils.parseByteSize(blkSizeStr);
		}
		private long m_blkSize = -1;
		
		@Option(names="-workers", paramLabel="count", description="reduce task count")
		private int m_nworkers = -1;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			ClusterDataSetOptions options = ClusterDataSetOptions.DEFAULT();
			if ( m_fillRatio > 0 ) {
				options = options.blockFillRatio(m_fillRatio);
			}
			if ( m_sampleRatio > 0 ) {
				options = options.sampleRatio(m_sampleRatio);
			}
			if ( m_blkSize > 0 ) {
				options = options.blockSize(m_blkSize);
			}
			if ( m_nworkers > 0 ) {
				options = options.workerCount(m_nworkers);
			}
			
			DataSet ds = marmot.getDataSet(m_dsId);
			ds.cluster(options);
		}
	}

	@Command(name="delete", description="delete the cluster of the dataset")
	public static class DeleteCluster extends SubCommand {
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
					description={"dataset id to cluster"})
		private String m_dsId;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			DataSet ds = marmot.getDataSet(m_dsId);
			ds.deleteSpatialCluster();
		}
	}

	@Command(name="show", description="display spatial cluster information for a dataset")
	public static class ShowCluster extends SubCommand {
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
					description={"dataset id to cluster"})
		private String m_dsId;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			Plan plan;
			plan = marmot.planBuilder("list_spatial_clusters")
						.loadSpatialClusterIndexFile(m_dsId)
						.project("*-{bounds,value_envelope}")
						.build();
			try ( RecordSet rset = marmot.executeLocally(plan) ) {
				rset.forEach(r -> printIndexEntry(r));
			}
		}
		
		private static final void printIndexEntry(Record record) {
			String packId = record.getString("pack_id");
			int blockNo = record.getInt("block_no");
			String quadKey = record.getString("quad_key");
			long count = record.getLong("count");
			long ownedCount = record.getLong("owned_count");
			String start = UnitUtils.toByteSizeString(record.getLong("start"));
			String len = UnitUtils.toByteSizeString(record.getLong("length"));
			
			System.out.printf("pack_id=%s, block_no=%02d, quad_key=%s, count=%d(%d), start=%s, length=%s%n",
								packId, blockNo, quadKey, count, ownedCount, start, len);
		}
	}

	@Command(name="draw", description="create a shapefile for spatial cluster tiles of a dataset")
	public static class DrawCluster extends SubCommand {
		@Mixin private ShapefileParameters m_shpParams;
		
		@Parameters(paramLabel="dataset-id", index="0", arity="1..1", description={"id of the target dataset"})
		private String m_dsId;
		
		@Option(names={"-o", "-output_dir"}, paramLabel="output-directory", required=true,
				description={"directory path for the output"})
		private String m_output;
		
		@Option(names={"-f"}, description="force to create a new output directory")
		private boolean m_force;
		
		@Option(names={"-v", "-value"}, description="draw value envelope")
		private boolean m_drawValue;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			String toGeom = (m_drawValue) ? "ST_GeomFromEnvelope(data_bounds)"
											: "ST_GeomFromEnvelope(tile_bounds)";
			
			Plan plan = marmot.planBuilder("read_cluster_index")
								.loadSpatialClusterIndexFile(m_dsId)
								.defineColumn("the_geom:polygon", toGeom)
								.project("the_geom,pack_id,quad_key,count,length")
								.build();
			
			try ( RecordSet rset = marmot.executeLocally(plan) ) {
				ExportShapefileParameters params = ExportShapefileParameters.create()
															.charset(m_shpParams.charset());
				ExportRecordSetAsShapefile exporter = new ExportRecordSetAsShapefile(rset, "EPSG:4326",
																					m_output, params);
				exporter.setForce(m_force);
				exporter.start().waitForDone();
			}
		}
	}

	@Command(name="bind", description="bind the existing file(s) as a dataset")
	public static class Bind extends SubCommand {
		@Parameters(paramLabel="path", index="0", arity="1..1",
					description={"source file-path (or source dataset-id) to bind"})
		private String m_path;
		
		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
				description={"dataset id to bind into"})
		private String m_dataset;
		
		@Option(names={"-t", "-type"}, paramLabel="type", required=true,
				description={"source type ('text', 'file', or 'dataset)"})
		private String m_type;

		private GeometryColumnInfo m_gcInfo;
		@Option(names={"-geom_col"}, paramLabel="column_name(EPSG code)",
				description="default Geometry column info")
		public void setGeometryColumnInfo(String gcInfoStr) {
			m_gcInfo = GeometryColumnInfo.fromString(gcInfoStr);
		}
		
		@Option(names={"-f", "-force"}, description="force to bind to a new dataset")
		private boolean m_force;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			DataSetType type;
			switch ( m_type ) {
				case "text":
					type = DataSetType.TEXT;
					break;
				case "file":
					type = DataSetType.FILE;
					break;
				case "dataset":
					DataSet srcDs = marmot.getDataSet(m_path);
					if ( m_gcInfo == null && srcDs.hasGeometryColumn() ) {
						m_gcInfo = srcDs.getGeometryColumnInfo();
					}
					m_path = srcDs.getHdfsPath();
					type = DataSetType.LINK;
					break;
				default:
					throw new IllegalArgumentException("invalid dataset type: " + m_type);
			}
			
			BindDataSetOptions opts = BindDataSetOptions.FORCE(m_force);
			if ( m_gcInfo != null ) {
				opts = opts.geometryColumnInfo(m_gcInfo);
			}
			marmot.bindExternalDataSet(m_dataset, m_path, type, opts);
		}
	}

	@Command(name="delete", description="delete the dataset(s)")
	public static class Delete extends SubCommand {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Option(names={"-r"}, description="list all descendant datasets")
		private boolean m_recursive;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			if ( m_recursive ) {
				marmot.deleteDir(m_dsId);
			}
			else {
				marmot.deleteDataSet(m_dsId);
			}
		}
	}

	@Command(name="import",
			subcommands= {
				ImportCsvCmd.class,
				ImportShapefileCmd.class,
				ImportGeoJsonCmd.class,
				ImportJdbcCmd.class
			},
			description="import into the dataset")
	public static class Import extends SubCommand {
		@Override
		public void run(MarmotRuntime marmot) throws Exception { }
	}

	@Command(name="csv", description="import CSV file into the dataset")
	public static class ImportCsvCmd extends SubCommand {
		@Mixin private CsvParameters m_csvParams;
		@Mixin private ImportParameters m_params;
		
		@Parameters(paramLabel="file_path", index="0", arity="1..1",
					description={"path to the target csv file"})
		private String m_start;
		
		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
					description={"dataset id to import onto"})
		public void setDataSetId(String id) {
			Utilities.checkNotNullArgument(id, "dataset id is null");
			m_params.setDataSetId(id);
		}
		
		@Option(names={"-glob"}, paramLabel="expr", description="glob expression for import files")
		private String m_glob = "**/*.csv";

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			StopWatch watch = StopWatch.start();
			
			File csvFilePath = new File(m_start);
			ImportIntoDataSet importFile = ImportCsv.from(csvFilePath, m_csvParams, m_params, m_glob);
			importFile.getProgressObservable()
						.subscribe(report -> {
							double velo = report / watch.getElapsedInFloatingSeconds();
							System.out.printf("imported: count=%d, elapsed=%s, velo=%.0f/s%n",
											report, watch.getElapsedMillisString(), velo);
						});
			long count = importFile.run(marmot);
			
			double velo = count / watch.getElapsedInFloatingSeconds();
			System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
							m_params.getDataSetId(), count, watch.getElapsedMillisString(), velo);
		}
	}

	@Command(name="shp", aliases={"shapefile"}, description="import shapefile(s) into the dataset")
	public static class ImportShapefileCmd extends SubCommand {
		@Mixin private ImportParameters m_params;
		@Mixin private ShapefileParameters m_shpParams;
		
		@Parameters(paramLabel="shp_file", index="0", arity="1..1",
					description={"path to the target shapefile (or directory)"})
		private String m_shpPath;
		
		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
				description={"dataset id to import onto"})
		public void setDataSetId(String id) {
			Utilities.checkNotNullArgument(id, "dataset id is null");
			m_params.setDataSetId(id);
		}

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			StopWatch watch = StopWatch.start();
			
			if ( m_params.getGeometryColumnInfo().isAbsent() ) {
				throw new IllegalArgumentException("Option '-geom_col' is missing");
			}
			
			File shpFile = new File(m_shpPath);
			ImportShapefile importFile = ImportShapefile.from(shpFile, m_shpParams, m_params);
			importFile.getProgressObservable()
						.subscribe(report -> {
							double velo = report / watch.getElapsedInFloatingSeconds();
							System.out.printf("imported: count=%d, elapsed=%s, velo=%.1f/s%n",
											report, watch.getElapsedMillisString(), velo);
						});
			long count = importFile.run(marmot);
			
			double velo = count / watch.getElapsedInFloatingSeconds();
			System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
								m_params.getDataSetId(), count, watch.getElapsedMillisString(),
								velo);
		}
	}

	@Command(name="geojson", description="import geojson file into the dataset")
	public static class ImportGeoJsonCmd extends SubCommand {
		@Mixin private GeoJsonParameters m_gjsonParams;
		@Mixin private ImportParameters m_importParams;
		
		@Parameters(paramLabel="path", index="0", arity="1..1",
					description={"path to the target geojson files (or directories)"})
		private String m_path;

		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
				description={"dataset id to import onto"})
		public void setDataSetId(String id) {
			Utilities.checkNotNullArgument(id, "dataset id is null");
			
			m_importParams.setDataSetId(id);
		}

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			StopWatch watch = StopWatch.start();
			
			if ( m_importParams.getGeometryColumnInfo().isAbsent() ) {
				throw new IllegalArgumentException("Option '-geom_col' is missing");
			}
			
			File gjsonFile = new File(m_path);
			ImportGeoJson importFile = ImportGeoJson.from(gjsonFile, m_gjsonParams, m_importParams);
			importFile.getProgressObservable()
						.subscribe(report -> {
							double velo = report / watch.getElapsedInFloatingSeconds();
							System.out.printf("imported: count=%d, elapsed=%s, velo=%.1f/s%n",
											report, watch.getElapsedMillisString(), velo);
						});
			long count = importFile.run(marmot);
			
			double velo = count / watch.getElapsedInFloatingSeconds();
			System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
								m_importParams.getDataSetId(), count, watch.getElapsedMillisString(), velo);
		}
	}

	@Command(name="jdbc", description="import a JDBC-connected table into a dataset")
	public static class ImportJdbcCmd extends SubCommand {
		@Mixin private JdbcParameters m_jdbcParams;
		@Mixin private ImportParameters m_importParams;

		@Parameters(paramLabel="table_name", index="0", arity="1..1",
					description={"JDBC table name"})
		private String m_tableName;
		
		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
				description={"dataset id to import onto"})
		public void setDataSetId(String id) {
			Utilities.checkNotNullArgument(id, "dataset id is null");
			m_importParams.setDataSetId(id);
		}

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			StopWatch watch = StopWatch.start();
			
			ImportJdbcTable importFile = ImportJdbcTable.from(m_tableName, m_jdbcParams,
																m_importParams);
			importFile.getProgressObservable()
						.subscribe(report -> {
							double velo = report / watch.getElapsedInFloatingSeconds();
							System.out.printf("imported: count=%d, elapsed=%s, velo=%.1f/s%n",
											report, watch.getElapsedMillisString(), velo);
						});
			long count = importFile.run(marmot);
			
			double velo = count / watch.getElapsedInFloatingSeconds();
			System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
								m_importParams.getDataSetId(), count, watch.getElapsedMillisString(), velo);
		}
	}
	
	@Command(name="export",
			subcommands= {
				ExportCsv.class,
				ExportShapefile.class,
				ExportGeoJson.class,
			},
			description="export a dataset")
	public static class Export extends SubCommand {
		@Override
		public void run(MarmotRuntime marmot) throws Exception { }
	}
	
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

	@Command(name="csv", description="export a dataset in CSV format")
	public static class ExportCsv extends SubCommand {
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
					description={"dataset id to export"})
		private String m_dsId;

		@Parameters(paramLabel="file_path", index="1", arity="0..1",
					description={"file path for exported CSV file"})
		private String m_output;

		@Mixin private CsvParameters m_csvParams;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			m_csvParams.charset().ifAbsent(() -> m_csvParams.charset(DEFAULT_CHARSET));
			
			FOption<String> output = FOption.ofNullable(m_output);
			BufferedWriter writer = ExternIoUtils.toWriter(output, m_csvParams.charset().get());
			new ExportAsCsv(m_dsId, m_csvParams).run(marmot, writer);
		}
	}

	@Command(name="shp", description="export the dataset in Shapefile format")
	public static class ExportShapefile extends SubCommand {
		@Mixin private ExportShapefileParameters m_shpParams;
		
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
					description={"dataset id to export"})
		private String m_dsId;

		@Parameters(paramLabel="output_dir", index="1", arity="1..1",
					description={"directory path for the output shapefiles"})
		private String m_output;
		
		@Option(names={"-f", "-force"}, description="force to create a new output directory")
		private boolean m_force;
		
		@Option(names={"-report_interval"}, paramLabel="record count",
				description="progress report interval")
		private int m_interval = -1;
		
		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			ExportDataSetAsShapefile export = new ExportDataSetAsShapefile(m_dsId, m_output,
																			m_shpParams);
			export.setForce(m_force);
			FOption.when(m_interval > 0, m_interval)
					.ifPresent(export::setProgressInterval);
			
			ProgressiveExecution<Long, Long> act = export.start(marmot);
			act.get();
		}
	}

	@Command(name="geojson", description="export a dataset in GeoJSON format")
	public static class ExportGeoJson extends SubCommand {
		@Mixin private GeoJsonParameters m_gjsonParams;
		
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
					description={"dataset id to export"})
		private String m_dsId;

		@Parameters(paramLabel="file_path", index="1", arity="0..1",
					description={"file path for exported GeoJson file"})
		private String m_output;
		
		@Option(names={"-p", "-pretty"}, description={"path to the output CSV file"})
		private boolean m_pretty;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			ExportAsGeoJson export = new ExportAsGeoJson(m_dsId)
										.printPrinter(m_pretty);
			m_gjsonParams.geoJsonSrid().ifPresent(export::setGeoJSONSrid);
			
			FOption<String> output = FOption.ofNullable(m_output);
			BufferedWriter writer = ExternIoUtils.toWriter(output, m_gjsonParams.charset());
			long count = export.run(marmot, writer);
			
			System.out.printf("done: %d records%n", count);
		}
	}

	@Command(name="thumbnail",
			subcommands= {
				CreateThumbnail.class,
			},
			description="thumbnail related commands")
	public static class Thumbnail extends SubCommand {
		@Override
		public void run(MarmotRuntime marmot) throws Exception { }
	}

	@Command(name="create", description="create a thumbnail for a dataset")
	public static class CreateThumbnail extends SubCommand {
		@Parameters(paramLabel="dataset", index="0", arity="1..1",
					description={"dataset id for thumbnail"})
		private String m_dsId;

		@Parameters(paramLabel="sample_count", index="1", arity="1..1",
					description={"sample count"})
		private long m_sampleCount;

		@Override
		public void run(MarmotRuntime marmot) throws Exception {
			StopWatch watch = StopWatch.start();
			
			DataSet ds = marmot.getDataSet(m_dsId);
			ds.createThumbnail((int)m_sampleCount);
			
			System.out.printf("nsmaples=%,d, elapsed time: %s%n",
							m_sampleCount, watch.stopAndGetElpasedTimeString());
		}
	}
}
