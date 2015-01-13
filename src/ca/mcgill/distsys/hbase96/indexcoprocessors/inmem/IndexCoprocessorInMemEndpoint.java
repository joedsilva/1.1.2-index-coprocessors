package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem;

import ca.mcgill.distsys.hbase96.indexcommons.SecondaryIndexConstants;
import ca.mcgill.distsys.hbase96.indexcommons.Util;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.IndexedColumnQuery;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorCreateRequest;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorCreateResponse;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorDeleteRequest;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorDeleteResponse;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorInMemService;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexedQueryRequest;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexedQueryResponse;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.ProtoResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class IndexCoprocessorInMemEndpoint extends IndexCoprocessorInMemService
		implements CoprocessorService, Coprocessor {

	private static final Log LOG =
			LogFactory.getLog(IndexCoprocessorInMemEndpoint.class);

	private boolean doNotRun = false;
	private Configuration configuration;
	private HRegion region;

	@Override
	public void createIndex(RpcController controller,
			IndexCoprocessorCreateRequest request,
			RpcCallback<IndexCoprocessorCreateResponse> done) {

		IndexCoprocessorCreateResponse.Builder builder =
				IndexCoprocessorCreateResponse.newBuilder();
		builder.setSuccess(true);

		if (!doNotRun) {
			// TODO check request inputs (family & qualifier)
			try {
				RegionIndexMap rim = RegionIndexMap.getInstance();
				RegionIndex regionIndex = rim.get(region.getRegionNameAsString());

				// If region index does not exist, create it
				if (regionIndex == null) {
					int maxTreeSize = configuration.getInt(
							SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
							SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
					regionIndex = new RegionIndex(maxTreeSize);
					rim.add(region.getRegionNameAsString(), regionIndex);
				}

				List<Column> colList = Util.buildColumnList(request.getColumnList());
				String idxColKey = Util.concatColumnsToString(colList);

				// If given column index does not exist, create it
				if (!regionIndex.getIndexedColumns().contains(idxColKey)) {
					// modified by Cong
					// Get in-memory index type and arguments for constructor
					String indexType = request.getIndexType();
					List<ByteString> resultArguments = request.getArgumentsList();
					Object[] objectArguments = new Object[resultArguments.size()];
					for (int i = 0; i < resultArguments.size(); i++) {
						try {
							objectArguments[i] = (Util.deserialize(
									resultArguments.get(i).toByteArray()));
						} catch (ClassNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					regionIndex.add(colList, region, indexType, objectArguments);

				} else {
					LOG.info("INDEX: requested index already exists.");
				}

			} catch (Exception e) {
				LOG.error("INDEX: Failed to create index.", e);
				builder.setSuccess(false);
				ResponseConverter.setControllerException(controller,
						new IOException(e));
			}
		}

		done.run(builder.build());
	}

	@Override
	public void deleteIndex(RpcController controller,
			IndexCoprocessorDeleteRequest request,
			RpcCallback<IndexCoprocessorDeleteResponse> done) {

		IndexCoprocessorDeleteResponse.Builder builder =
				IndexCoprocessorDeleteResponse.newBuilder();
		builder.setSuccess(true);

		if (!doNotRun) {
			// TODO check request inputs (family & qualifier)
			RegionIndexMap rim = RegionIndexMap.getInstance();
			RegionIndex regionIndex = rim.get(region.getRegionNameAsString());

			List<Column> colList = Util.buildColumnList(request.getColumnList());
			//String idxColKey = Util.concatColumnsToString(colList);

			try {
				if (regionIndex != null) {
					regionIndex.remove(colList);
				}
			} catch (Exception e) {
				builder.setSuccess(false);
				ResponseConverter.setControllerException(controller,
						new IOException(e));
			}
		}
		done.run(builder.build());
	}

	public void start(CoprocessorEnvironment environment) throws IOException {
		// make sure we are on a region server
		if (!(environment instanceof RegionCoprocessorEnvironment)) {
			throw new IllegalArgumentException(
					"Indexes only act on regions - " +
					"started in an environment that was not a region");
		}

		RegionCoprocessorEnvironment env =
				(RegionCoprocessorEnvironment) environment;
		this.region = env.getRegion();
		TableName tableName = region.getTableDesc().getTableName();

		if (tableName.isSystemTable() || tableName.getNameAsString().equals(
				SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME)) {
			doNotRun = true;
		} else {
			configuration = HBaseConfiguration.create();
		}

	}

	@Override
	public void execIndexedQuery(RpcController controller,
			IndexedQueryRequest request,
			RpcCallback<IndexedQueryResponse> done) {

		RegionIndexMap rim = RegionIndexMap.getInstance();
		RegionIndex regionIndex = rim.get(region.getRegionNameAsString());

		IndexedQueryResponse.Builder builder = IndexedQueryResponse.newBuilder();

		if (doNotRun || regionIndex == null) {
			// No index exists for this region, exception
			ResponseConverter.setControllerException(controller,
					new IOException("No index exists for table ["
							+ region.getTableDesc().getNameAsString() + "]."));
			done.run(builder.build());
			return;
		}

		IndexedColumnQuery query = Util.buildQuery(request);

		if (query.getCriteria().isEmpty()) {
			// The query must contain at least one criterion
			IOException e = new IOException("An indexed query must contain " +
					"at least one criterion.");
			ResponseConverter.setControllerException(controller, e);
			LOG.error(e);
			done.run(builder.build());
			return;
		}

		// Yousuf
		//
		// Check if there exists a single- or multi-column index that
		// covers all criteria columns in the same order.
		// If yes, then use this index.
		// Otherwise, check which criteria columns have corresponding
		// single-column indices available, and separate them from
		// those that do not.  Then iterate over each indexed criterion,
		// successively filtering (if MUST_PASS_ALL) or appending (if
		// MUST_PASS_ONE) their respective result sets (of primary
		// key references).  Then fetch these rows from the region,
		// along with applying non-indexed criterion, filtering as
		// required (MUST_PASS_ALL or MUST_PASS_ONE).  Note: This
		// means that for MUST_PASS_ONE criteria (A OR B OR C OR D) -
		// where single-column indices exist for A and B - becomes
		// ((A OR B) AND (C OR D)) because we apply any non-indexed
		// column criteria on top of any indexed column critera; i.e.,
		// we do not scan the entire region for (C OR D), rather
		// applying it only on those rows that first satisfied (A OR B).
		//

		List<Criterion<?>> selectCriteria = query.getCriteria();
		List<Column> projectColumns = query.getColumnList();

		// Step 1:
		// Check if there exists a single- or multi-column index that
		// covers all criteria columns in the same order.

		List<Column> criteriaColumns = new ArrayList<Column>();
		for (Criterion<?> criterion : selectCriteria) {
			criteriaColumns.add(criterion.getCompareColumn());
		}
		String idxColKey = Util.concatColumnsToString(criteriaColumns);

		if (regionIndex.getIndexedColumns().contains(idxColKey)) {
			// Case 1:
			// If yes, then use this index.

			try {
				List<ProtoResult> filteredRows = regionIndex.
						filterRowsFromCriteria(idxColKey, selectCriteria,
								projectColumns, region);
				builder.addAllResult(filteredRows);

			} catch (IOException e) {
				ResponseConverter.setControllerException(controller,
						new IOException(e));
				LOG.error("Couldn't fetch results", e);
			}
			done.run(builder.build());
			return;
		}

		// Case 2:
		// Otherwise, check which criteria columns have corresponding
		// single-column indices available, and separate them from
		// those that do not.

		List<Criterion<?>> criteriaOnNonIndexedColumns =
				new ArrayList<Criterion<?>>();
		List<Criterion<?>> criteriaOnIndexedColumns =
				new ArrayList<Criterion<?>>();
		splitCriteriaByTarget(criteriaOnNonIndexedColumns,
				criteriaOnIndexedColumns, selectCriteria, regionIndex);

		if (criteriaOnIndexedColumns.isEmpty()) {
			// At least one criterion must apply to an indexed column
			IOException e = new IOException("An indexed query must contain " +
					"at least one criterion that applies to an indexed column.");
			ResponseConverter.setControllerException(controller, e);
			LOG.error(e);
			done.run(builder.build());
			return;
		}

		// Step 2:
		// Then iterate over each indexed criterion,
		// successively filtering (if MUST_PASS_ALL) or appending (if
		// MUST_PASS_ONE) their respective result sets (of primary
		// key references).  Then fetch these rows from the region,
		// along with applying non-indexed criterion, filtering as
		// required (MUST_PASS_ALL or MUST_PASS_ONE).

		try {
			List<ProtoResult> filteredRows = regionIndex.
					filterRowsFromCriteria(criteriaOnIndexedColumns,
							criteriaOnNonIndexedColumns, query, region);
			builder.addAllResult(filteredRows);

		} catch (Exception e) {
			ResponseConverter.setControllerException(controller,
					new IOException(e));
			LOG.error("Couldn't fetch results", e);
		}
		done.run(builder.build());
		return;
	}

	private void splitCriteriaByTarget(
			List<Criterion<?>> criteriaOnNonIndexedColumns,
			List<Criterion<?>> criteriaOnIndexColumns,
			List<Criterion<?>> criteria, RegionIndex regionIndex) {

		Set<String> indexedColumns = regionIndex.getIndexedColumns();

		for (Criterion<?> criterion : criteria) {
			Column column = criterion.getCompareColumn();
			if (indexedColumns.contains(column.toString())) {
				criteriaOnIndexColumns.add(criterion);
			} else {
				criteriaOnNonIndexedColumns.add(criterion);
			}
		}
	}

	public void stop(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub
	}

	public Service getService() {
		return this;
	}
}
