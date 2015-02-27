package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem;

import ca.mcgill.distsys.hbase96.indexcommons.IndexedColumn;
import ca.mcgill.distsys.hbase96.indexcommons.SecondaryIndexConstants;
import ca.mcgill.distsys.hbase96.indexcommons.Util;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommons.proto.ColumnValue;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.AbstractPluggableIndex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HTableIndexCoprocessor extends BaseRegionObserver {

	private static final Log LOG = LogFactory
			.getLog(HTableIndexCoprocessor.class);

	private boolean doNotRun = false;
	private Configuration configuration;
	private TableName tableName;
	private HRegion region;
	private String regionName;

	@Override
	public void start(CoprocessorEnvironment environment) throws IOException {

		// Make sure we are on a region server
		if (!(environment instanceof RegionCoprocessorEnvironment)) {
			throw new IllegalArgumentException(
					"INDEX: Indexes only act on regions - "
							+ "started in an environment that was not a region");
		}

		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) environment;
		region = env.getRegion();
		HTableDescriptor desc = region.getTableDesc();
		tableName = desc.getTableName();
		LOG.debug("Table name: " + tableName);

		// MOdified by Cong
		// MASTER_INDEX_TABLE_NAME
		// Name of the master index table that contains which columns are
		// indexed for which tables.
		// if (Arrays.equals(tableName, HConstants.ROOT_TABLE_NAME)||
		// Arrays.equals(tableName, HConstants.META_TABLE_NAME)
		// || new
		// String(tableName).equals(SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME))
		// {
		// if (desc.isRootRegion() || desc.isMetaTable()
		// || Arrays.equals(tableName, HConstants.META_TABLE_NAME)
		// || Bytes.toString(tableName)
		// .equals(SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME)) {

		if (tableName.isSystemTable()
				|| tableName.getNameAsString().equals(
						SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME)) {
			doNotRun = true;
		} else {
			configuration = HBaseConfiguration.create(env.getConfiguration());
			regionName = region.getRegionNameAsString();
			LOG.info("INDEX: Starting HTableIndexCoprocessor on region " + "["
					+ regionName + "]");
		}
	}

	private void loadRegionIndexes() throws IOException, NoSuchMethodException {
		try {
			loadIndexFromFS();
			updateRegionIndexes();

		} catch (FileNotFoundException FNFe) {
			LOG.info("INDEX: No file index for region [" + regionName
					+ "] found; "
					+ "checking master table in case the file was deleted "
					+ "and rebuilding if required.");
			try {
				rebuildRegionIndexes();

			} catch (ClassNotFoundException e) {
				LOG.fatal("INDEX: Failed to create index for [" + regionName
						+ "]", e);
				throw new RuntimeException(e);
			}

		} catch (Exception e) {
			LOG.warn(
					"INDEX: Failed to read index for region ["
							+ regionName
							+ "] "
							+ "from FS or update it. Rebuilding entire region's index. "
							+ "This may take a while.", e);
			try {
				rebuildRegionIndexes();

			} catch (ClassNotFoundException e1) {
				LOG.fatal(
						"INDEX: Could not rebuild entire index for region "
								+ "["
								+ regionName
								+ "] from scanning the region. Indexing for "
								+ "this region will be unavailable until this problem is fixed.",
						e1);
				throw new RuntimeException(e1);
			}
		}
	}

	private void loadIndexFromFS() throws IOException, ClassNotFoundException {
		// Modified by Cong
		// Path tablePath =
		// FSUtils.getTablePath(FSUtils.getRootDir(configuration),
		// region.getTableDesc().getName());

		Path tablePath = FSUtils.getTableDir(FSUtils.getRootDir(configuration),
				tableName);
		Path regionIndexPath = new Path(tablePath, regionName + "__index");
		LOG.debug("loadIndexFromFS: " + regionIndexPath.toString());

		FileSystem fs = FileSystem.get(configuration);

		LOG.info("INDEX: Opening index for region [" + regionName
				+ "] from file.");

		if (!fs.exists(regionIndexPath)) {

			LOG.info("INDEX: No file index for region ["
					+ regionName
					+ "] found; "
					+ "checking if it has been left an index by its splitting parent.");

			regionIndexPath = new Path(tablePath, "postSplit,"
					+ Bytes.toString(region.getStartKey()) + ".__index");
			LOG.debug("loadIndexFromFS: " + regionIndexPath.toString());

			if (!fs.exists(regionIndexPath)) {
				throw new FileNotFoundException(regionIndexPath.toString());
			}

			LOG.info("INDEX: Opening postSplit index for region [" + regionName
					+ "].");

		} else {
			LOG.info("INDEX: Loading index for region [" + regionName
					+ "] from file.");
		}

		FSDataInputStream in = fs.open(regionIndexPath);
		SnappyInputStream sis = new SnappyInputStream(in);
		ObjectInputStream ois = new ObjectInputStream(sis);

		RegionIndex regionIndex = (RegionIndex) ois.readObject();
		RegionIndexMap.getInstance().add(regionName, regionIndex);
		ois.close();

		try {
			fs.delete(regionIndexPath, false);
		} catch (IOException e) {
			LOG.warn("INDEX: Failed to delete index file " + "["
					+ regionIndexPath.toString() + "] after loading it.", e);
		}
	}

	private void rebuildRegionIndexes() throws IOException,
			ClassNotFoundException, NoSuchMethodException {

		List<IndexedColumn> regionIndexedColumns = getIndexedColumns();

		if (!regionIndexedColumns.isEmpty()) {

			int maxTreeSize = configuration.getInt(
					SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
					SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
			RegionIndex regionIndex = new RegionIndex(maxTreeSize);

			for (IndexedColumn idxCol : regionIndexedColumns) {
				LOG.info("INDEX: Building index for region [" + region + "]; "
						+ "column [" + idxCol.toString() + "].");

				regionIndex.add(idxCol.getColumnList(), region,
						idxCol.getIndexType(), idxCol.getArguments());

				LOG.info("INDEX: Finished building index for region " + "["
						+ region + "]; column [" + idxCol.toString() + "].");
			}

			RegionIndexMap.getInstance().add(regionName, regionIndex);
		}
	}

	private List<IndexedColumn> getIndexedColumns() throws IOException {

		HBaseAdmin admin = null;
		List<IndexedColumn> result = new ArrayList<IndexedColumn>();

		try {
			admin = new HBaseAdmin(configuration);
			if (!admin
					.tableExists(SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME)) {
				return result;
			}
		} finally {
			if (admin != null) {
				admin.close();
			}
		}

		HTableInterface masterIdxTable = null;
		try {
			masterIdxTable = new HTable(configuration,
					SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME);
			Get get = new Get(tableName.getName());
			Result rs = masterIdxTable.get(get);
			for (KeyValue kv : rs.raw()) {
				byte[] serializedIndexedColumn = kv.getValue();
				if (serializedIndexedColumn != null) {
					ByteArrayInputStream bais = new ByteArrayInputStream(
							serializedIndexedColumn);
					ObjectInputStream ois = new ObjectInputStream(bais);
					try {
						result.add((IndexedColumn) ois.readObject());
					} catch (ClassNotFoundException e) {
						LOG.error(
								"INDEX: Invalid entry in master index table for "
										+ "indexed table ["
										+ tableName.toString() + "].", e);
					} finally {
						ois.close();
					}
				}
			}
		} finally {
			if (masterIdxTable != null) {
				masterIdxTable.close();
			}
		}

		return result;
	}

	public synchronized void updateRegionIndexes() throws IOException,
			ClassNotFoundException, NoSuchMethodException {

		List<IndexedColumn> metaRegionIndexedColumns = getIndexedColumns();
		Set<String> regionIndexedColumns;
		RegionIndex regionIndex = RegionIndexMap.getInstance().get(regionName);

		// Remove if deleted
		if (regionIndex != null) {
			regionIndexedColumns = regionIndex.getIndexedColumns();
			for (String idxCol : regionIndexedColumns) {
				boolean found = false;
				for (IndexedColumn metaIdxCol : metaRegionIndexedColumns) {
					String idxColKey = metaIdxCol.toString();
					if (idxColKey.equals(idxCol)) {
						found = true;
						break;
					}
				}
				if (!found) {
					regionIndex.remove(idxCol);
				}
			}
		}

		// Add new indexed columns
		if (metaRegionIndexedColumns.size() > 0) {
			if (regionIndex == null) {
				int maxTreeSize = configuration
						.getInt(SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
								SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
				regionIndex = new RegionIndex(maxTreeSize);
			}

			for (IndexedColumn idxCol : metaRegionIndexedColumns) {
				String idxColKey = idxCol.toString();
				if (!regionIndex.getIndexedColumns().contains(idxColKey)) {
					// Need to be done by Cong
					regionIndex.add(idxCol.getColumnList(), region,
							idxCol.getIndexType(), idxCol.getArguments());
				}
			}
		}

		// This is because we create new regionIndex if regionIndex == null
		if (regionIndex != null
				&& RegionIndexMap.getInstance().get(regionName) == null) {
			RegionIndexMap.getInstance().add(regionName, regionIndex);
		}
	}

	@Override
	public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
		if (!doNotRun) {
			try {
				LOG.info("INDEX: Region [" + regionName + "] postOpen; "
						+ "initializing indexes.");
				try {
					loadRegionIndexes();
				} catch (NoSuchMethodException e1) {
					LOG.fatal(
							"INDEX: Failed to load the region's index for table "
									+ "[" + tableName.toString() + "]", e1);
					e1.printStackTrace();
				}
			} catch (IOException IOe) {
				LOG.fatal("INDEX: Failed to load the region's index for table "
						+ "[" + tableName.toString() + "]", IOe);
				// TODO close the region??
				throw new RuntimeException(IOe);
			}
		}
	}

	// private void updateTableIndexes(List<KeyValue> kVListToIndex,
	// Result currentRow, RegionIndex regionIndex) throws IOException {
	//
	// for (KeyValue kv : kVListToIndex) {
	// try {
	// KeyValue currentValue = currentRow.getColumnLatest(
	// kv.getFamily(), kv.getQualifier());
	//
	// if (currentValue != null
	// && !Arrays.equals(currentValue.getValue(),
	// kv.getValue())) {
	// // There is a current value for the column but it is
	// // different from the one to be added
	// // => update current value's index to remove the reference
	//
	// // (1) This is will do the update operation
	// // -1: remove the current value
	// removeCurrentValueRef(kv.getRow(), currentValue,
	// regionIndex);
	// // -2: add the new value
	// addNewValueRef(kv, regionIndex);
	// } else if (currentValue == null) {
	// // (2) This will do the put operation
	// addNewValueRef(kv, regionIndex);
	// } else {
	// // Nothing to do, new value is the same as the old value
	// }
	// } catch (IOException IOe) {
	// LOG.error(
	// "INDEX: PUT: Failed to add to index for " + "table ["
	// + tableName.toString() + "], " + "column ["
	// + Bytes.toString(kv.getFamily()) + ":"
	// + Bytes.toString(kv.getQualifier()) + "]", IOe);
	// throw IOe;
	// } catch (ClassNotFoundException CNFe) {
	// LOG.error(
	// "INDEX: PUT: Failed to add to index for " + "table ["
	// + tableName.toString() + "], " + "column ["
	// + Bytes.toString(kv.getFamily()) + ":"
	// + Bytes.toString(kv.getQualifier()) + "]", CNFe);
	// throw new IOException(CNFe);
	// }
	// }
	// }

	// changed
	// private void updateTableIndexes(List<KeyValue> kVListToIndex,
	// HashMap<String, Set<IndexedColumn>> singleMappedIndex,
	// Result currentRow, RegionIndex regionIndex) throws IOException {
	//
	// Set<IndexedColumn> changedIndexColumnSet;
	// Set<IndexedColumn> alreadyChangedIndexColumnSet = new
	// HashSet<IndexedColumn>();
	// KeyValue nextColumn = null;
	// boolean isIndexed = false;
	// byte[] concatValue;
	// for (KeyValue kv : kVListToIndex) {
	// try {
	// KeyValue currentValue = currentRow.getColumnLatest(
	// kv.getFamily(), kv.getQualifier());
	//
	// if (currentValue != null
	// && !Arrays.equals(currentValue.getValue(),
	// kv.getValue())) {
	// // There is a current value for the column but it is
	// // different from the one to be added
	// // => update current value's index to remove the reference
	//
	// // (1) This is will do the update operation
	//
	// changedIndexColumnSet = singleMappedIndex.get(new Column(kv
	// .getFamily()).setQualifier(kv.getQualifier())
	// .toString());
	// for (IndexedColumn changedIndexColumn : changedIndexColumnSet) {
	// if (alreadyChangedIndexColumnSet
	// .contains(changedIndexColumn)) {
	// continue;
	// } else {
	// alreadyChangedIndexColumnSet
	// .add(changedIndexColumn);
	// isIndexed = true;
	// concatValue = null;
	// for (Column column : changedIndexColumn
	// .getColumnList()) {
	// if (Bytes.equals(Util.concatByteArray(
	// column.getFamily(),
	// column.getQualifier()), Util
	// .concatByteArray(kv.getFamily(),
	// kv.getQualifier())) == true) {
	// concatValue = Util.concatByteArray(
	// concatValue, kv.getValue());
	// } else {
	// nextColumn = currentRow.getColumnLatest(
	// column.getFamily(),
	// column.getQualifier());
	// if (nextColumn != null) {
	// concatValue = Util.concatByteArray(
	// concatValue,
	// nextColumn.getValue());
	// } else {
	// isIndexed = false;
	// break;
	// }
	// }
	// }
	// if (isIndexed) {
	// // <1>: remove the current value
	// removeCurrentValueRefFromIndex(kv.getRow(),
	// changedIndexColumn.toString(),
	// concatValue, regionIndex);
	// // <2>: add the new value
	//
	// LOG.error("Coprocessor: Remove: row: "
	// + Bytes.toString(kv.getRow())
	// + " concatValue: "
	// + Bytes.toString(concatValue));
	//
	// addNewValueRefToIndex(kv.getRow(),
	// changedIndexColumn.toString(),
	// concatValue, regionIndex);
	// }
	//
	// }
	//
	// }
	//
	// } else if (currentValue == null) {
	//
	// changedIndexColumnSet = singleMappedIndex.get(new Column(kv
	// .getFamily()).setQualifier(kv.getQualifier())
	// .toString());
	// for (IndexedColumn changedIndexColumn : changedIndexColumnSet) {
	//
	// if (alreadyChangedIndexColumnSet
	// .contains(changedIndexColumn)) {
	// continue;
	// } else {
	// alreadyChangedIndexColumnSet
	// .add(changedIndexColumn);
	// isIndexed = true;
	// concatValue = null;
	// for (Column column : changedIndexColumn
	// .getColumnList()) {
	// if (Bytes.equals(Util.concatByteArray(
	// column.getFamily(),
	// column.getQualifier()), Util
	// .concatByteArray(kv.getFamily(),
	// kv.getQualifier())) == true) {
	// concatValue = Util.concatByteArray(
	// concatValue, kv.getValue());
	// } else {
	// nextColumn = currentRow.getColumnLatest(
	// column.getFamily(),
	// column.getQualifier());
	// if (nextColumn != null) {
	// concatValue = Util.concatByteArray(
	// concatValue,
	// currentRow.getValue(
	// column.getFamily(),
	// column.getQualifier()));
	// } else {
	// isIndexed = false;
	// break;
	// }
	// }
	// }
	// // <1>: add the new value
	//
	// LOG.error("Coprocessor: addNew: row: "
	// + Bytes.toString(kv.getRow())
	// + " concatValue: "
	// + Bytes.toString(concatValue));
	//
	// addNewValueRefToIndex(kv.getRow(),
	// changedIndexColumn.toString(), concatValue,
	// regionIndex);
	//
	// }
	//
	// }
	// } else {
	// // Nothing to do, new value is the same as the old value
	// }
	// } catch (IOException IOe) {
	// LOG.error(
	// "INDEX: PUT: Failed to add to index for " + "table ["
	// + tableName.toString() + "], " + "column ["
	// + Bytes.toString(kv.getFamily()) + ":"
	// + Bytes.toString(kv.getQualifier()) + "]", IOe);
	// throw IOe;
	// } catch (ClassNotFoundException CNFe) {
	// LOG.error(
	// "INDEX: PUT: Failed to add to index for " + "table ["
	// + tableName.toString() + "], " + "column ["
	// + Bytes.toString(kv.getFamily()) + ":"
	// + Bytes.toString(kv.getQualifier()) + "]", CNFe);
	// throw new IOException(CNFe);
	// }
	// }
	// }

	private void updateTableIndexes(List<KeyValue> kVListToIndex,
			HashMap<String, Set<IndexedColumn>> singleMappedIndex,
			Result currentRow, RegionIndex regionIndex, byte[] rowKey) throws IOException {

		Set<IndexedColumn> changedIndexColumnSet;
		Set<IndexedColumn> allChangedIndexColumnSet = new HashSet<IndexedColumn>();
		List<ColumnValue> existColumns = new ArrayList<ColumnValue>();
		List<ColumnValue> newColumns = new ArrayList<ColumnValue>();
		ColumnValue tempColumnValue;
		KeyValue nextColumn = null;
		int index;
		boolean isIndexed = true;
		boolean isNew = false;
		byte[] concatOldValue;
		byte[] concatNewValue;
		// return null??
		//byte[] rowKey = currentRow.getRow();
		for (KeyValue kv : kVListToIndex) {
			KeyValue currentValue = currentRow.getColumnLatest(kv.getFamily(),
					kv.getQualifier());

			if (currentValue != null
					&& !Arrays.equals(currentValue.getValue(), kv.getValue())) {

				// There is a current value for the column but it is
				// different from the one to be added
				// => update current value's index to remove the reference
				tempColumnValue = new ColumnValue(kv);
				changedIndexColumnSet = singleMappedIndex.get(tempColumnValue
						.toString());
				allChangedIndexColumnSet.addAll(changedIndexColumnSet);
				existColumns.add(tempColumnValue);
				//LOG.debug("Hello1: tempColumnValue: " + tempColumnValue.toStringFull());

			} else if (currentValue == null) {

				// new column added
				tempColumnValue = new ColumnValue(kv);
				changedIndexColumnSet = singleMappedIndex.get(tempColumnValue
						.toString());
				allChangedIndexColumnSet.addAll(changedIndexColumnSet);
				newColumns.add(tempColumnValue);
        //LOG.debug("Hello2: tempColumnValue: " + tempColumnValue.toStringFull());
			} else {
				// Nothing to do, new value is the same as the old value
			}
		}

		for (IndexedColumn indexedColumn : allChangedIndexColumnSet) {
			isIndexed = true;
			isNew = false;
			concatOldValue = null;
			concatNewValue = null;

			for (Column column : indexedColumn.getColumnList()) {
				tempColumnValue = new ColumnValue(column);
				nextColumn = currentRow.getColumnLatest(column.getFamily(),
						column.getQualifier());
					
				if ((index = newColumns.indexOf(tempColumnValue)) != -1) {
					isNew = true;
					concatNewValue = Util.concatByteArray(concatNewValue,
							newColumns.get(index).getValue());
				} else {
					if (nextColumn == null) {
						isIndexed = false;
						break;
					}  else {
						if ((index = existColumns.indexOf(tempColumnValue)) != -1) {
							if (isNew == true) {
								concatNewValue = Util.concatByteArray(
										concatNewValue, existColumns.get(index).getValue());
							} else {
								concatNewValue = Util.concatByteArray(
										concatNewValue, existColumns.get(index).getValue());
								concatOldValue = Util.concatByteArray(
										concatOldValue, nextColumn.getValue());
							}
						} else {
							if (isNew == true) {
								concatNewValue = Util.concatByteArray(
										concatNewValue, nextColumn.getValue());
							} else {
								concatNewValue = Util.concatByteArray(
										concatNewValue, nextColumn.getValue());
								concatOldValue = Util.concatByteArray(
										concatOldValue, nextColumn.getValue());
							}
						}
					}
				}
			}

			if (isIndexed) {
				try {
					if (isNew) {
						//LOG.debug("HelloFinal1: rowKey: " + Bytes.toString(rowKey) +
            //    ", indexedColumn: " + indexedColumn.toString() +
            //    ", concatNewValue: " + Bytes.toString(concatNewValue));
						addNewValueRefToIndex(rowKey,
                indexedColumn.toString(), concatNewValue, regionIndex);
					} else {
						
						//LOG.debug("HelloFinal1: rowKey: " + Bytes.toString(rowKey) +
            //    ", indexedColumn: " + indexedColumn.toString() +
            //    ", concatOldValue: " + Bytes.toString(concatOldValue));
						//LOG.debug("HelloFinal2: rowKey: " + Bytes.toString(rowKey) +
            //    ", indexedColumn: " + indexedColumn.toString() +
            //    ", concatNewValue: " + Bytes.toString(concatNewValue));
						removeCurrentValueRefFromIndex(rowKey,
								indexedColumn.toString(), concatOldValue, regionIndex);
						
						addNewValueRefToIndex(rowKey,
								indexedColumn.toString(), concatNewValue, regionIndex);
						
					}
				} catch (IOException IOe) {
					LOG.error(
							"INDEX: PUT: Failed to add to index for " + "table ["
									+ tableName.toString() + "], " + "column ["
									+ indexedColumn.toString() + "]", IOe);
					throw IOe;
				} catch (ClassNotFoundException CNFe) {
					LOG.error(
							"INDEX: PUT: Failed to add to index for " + "table ["
									+ tableName.toString() + "], " + "column ["
									+ indexedColumn.toString() + "]", CNFe);
					throw new IOException(CNFe);
				}
				
			}
		}

	}

	private void addNewValueRef(KeyValue newValue, RegionIndex regionIndex)
			throws IOException, ClassNotFoundException {
		// Changed by Cong
		Column column = new Column(newValue.getFamily(),
				newValue.getQualifier());
		AbstractPluggableIndex rci = regionIndex.get(column.toString());
		rci.add(newValue.getValue(), newValue.getRow());
	}

	private void addNewValueRefToIndex(byte[] row, String abstractIndexKey,
			byte[] currentValue, RegionIndex regionIndex) throws IOException,
			ClassNotFoundException {
		// Changed by Cong
		AbstractPluggableIndex rci = regionIndex.get(abstractIndexKey);
		rci.add(currentValue, row);
	}

	private void removeCurrentValueRef(byte[] row, KeyValue currentValue,
			RegionIndex regionIndex) throws IOException, ClassNotFoundException {
		// Changed by Cong
		Column column = new Column(currentValue.getFamily(),
				currentValue.getQualifier());
		AbstractPluggableIndex rci = regionIndex.get(column.toString());
		rci.remove(currentValue.getValue(), row);
	}

	private void removeCurrentValueRefFromIndex(byte[] row,
			String abstractIndexKey, byte[] currentValue,
			RegionIndex regionIndex) throws IOException, ClassNotFoundException {
		// Changed by Cong
		AbstractPluggableIndex rci = regionIndex.get(abstractIndexKey);
		rci.remove(currentValue, row);
	}

	// @SuppressWarnings("unchecked")
	// private void getValueToIndex(Put put, List<KeyValue> kVListToIndex,
	// Set<String> indexedColumns) {
	// for (byte[] family : put.getFamilyMap().keySet()) {
	// for (KeyValue kv : (List<KeyValue>) put.getFamilyMap().get(family)) {
	// String colName = Bytes.toString(Util.concatByteArray(family,
	// kv.getQualifier()));
	// if (indexedColumns.contains(colName)) {
	// kVListToIndex.add(kv);
	// }
	// }
	// }
	// }

	@SuppressWarnings("unchecked")
	private void getValueToIndex(Put put, List<KeyValue> kVListToIndex,
			Set<String> indexedColumns) {
		for (byte[] family : put.getFamilyMap().keySet()) {
			for (KeyValue kv : (List<KeyValue>) put.getFamilyMap().get(family)) {
				Column column = new Column(family, kv.getQualifier());
				if (indexedColumns.contains(column.toString())) {
					kVListToIndex.add(kv);
				}
			}
		}
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		LOG.info("INDEX: HTableIndexCoprocessor stop for region " + "["
				+ regionName + "] postOpen; initializing indexes.");
	}

	private void persistIndexToFS() throws IOException {

		RegionIndex regionIndex = RegionIndexMap.getInstance().get(regionName);

		// Modified by Cong
		// Path tablePath =
		// FSUtils.getTablePath(FSUtils.getRootDir(configuration),
		// region.getTableDesc().getName());

		Path tablePath = FSUtils.getTableDir(FSUtils.getRootDir(configuration),
				tableName);

		if (regionIndex != null) {
			Path regionIndexPath = new Path(tablePath, regionName + "__index");
			LOG.debug("persistIndexToFS: " + regionIndexPath.toString());

			FileSystem fs = FileSystem.get(configuration);
			FsPermission perms = FSUtils.getFilePermissions(fs, configuration,
					HConstants.DATA_FILE_UMASK_KEY);

			FSDataOutputStream out = FileSystem.create(fs, regionIndexPath,
					perms);
			SnappyOutputStream sos = new SnappyOutputStream(out);
			ObjectOutputStream oos = new ObjectOutputStream(sos);

			oos.writeObject(regionIndex);
			oos.flush();
			oos.close();
		}
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public byte[] getTableName() {
		return tableName.getName();
	}

	@Override
	public void postClose(ObserverContext<RegionCoprocessorEnvironment> e,
			boolean abortRequested) {
		if (!doNotRun) {
			try {
				LOG.info("INDEX: Region [" + regionName + "] postClose; "
						+ "persisting region indexes.");
				persistIndexToFS();
			} catch (IOException e1) {
				LOG.error(
						"INDEX: Failed to persist index to filesystem for region "
								+ "[" + regionName + "].", e1);
			}
			RegionIndexMap.getInstance().remove(regionName);
		}
	}

	@Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c,
			byte[] splitRow) throws IOException {
		// we remove the ref to the memory index, we don't want it persisted
		// when the region closes due to a split;
		// the index will be rebuilt when the new regions open.
		LOG.info("INDEX: Region [" + regionName + "] preSplit; "
				+ "removing in memory indexes.");
		splitAndPersistIndex(splitRow);
		RegionIndexMap.getInstance().remove(regionName);
	}

	private void splitAndPersistIndex(byte[] splitRow) {

		RegionIndex regionIndex = RegionIndexMap.getInstance().get(regionName);
		regionIndex.setSplitting(true);

		if (regionIndex != null) {
			int maxTreeSize = configuration.getInt(
					SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
					SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
			RegionIndex daughterRegionAIndex = new RegionIndex(maxTreeSize);
			RegionIndex daughterRegionBIndex = new RegionIndex(maxTreeSize);

			try {
				regionIndex.split(daughterRegionAIndex, daughterRegionBIndex,
						splitRow);

				String daughterRegionAIndexFilename = "postSplit,"
						+ Bytes.toString(region.getStartKey()) + ".__index";
				String daughterRegionBIndexFilename = "postSplit,"
						+ Bytes.toString(splitRow) + ".__index";

				if (!daughterRegionAIndex.isEmpty()) {
					persistDaughterRegionIndex(daughterRegionAIndex,
							daughterRegionAIndexFilename);
				}
				if (!daughterRegionBIndex.isEmpty()) {
					persistDaughterRegionIndex(daughterRegionBIndex,
							daughterRegionBIndexFilename);
				}
			} catch (Exception e) {
				LOG.warn(
						"INDEX: Failed to save the daugher region indexes post split "
								+ "for region ["
								+ regionName
								+ "]. Full rebuild of daughter "
								+ "region indexes may be required and take some time.",
						e);
			}
		}
	}

	private void persistDaughterRegionIndex(RegionIndex daughterRegionAIndex,
			String daughterRegionAIndexFilename) throws IOException {

		// Path tablePath =
		// FSUtils.getTablePath(FSUtils.getRootDir(configuration),
		// region.getTableDesc().getName());
		Path tablePath = FSUtils.getTableDir(FSUtils.getRootDir(configuration),
				tableName);
		Path regionIndexPath = new Path(tablePath, daughterRegionAIndexFilename);
		LOG.debug("persistDaughterRegionIndex: " + regionIndexPath.toString());

		FileSystem fs = FileSystem.get(configuration);

		FsPermission perms = FSUtils.getFilePermissions(fs, configuration,
				HConstants.DATA_FILE_UMASK_KEY);

		FSDataOutputStream out = FileSystem.create(fs, regionIndexPath, perms);
		SnappyOutputStream sos = new SnappyOutputStream(out);
		ObjectOutputStream oos = new ObjectOutputStream(sos);

		oos.writeObject(daughterRegionAIndex);
		oos.flush();
		oos.close();
	}

	// public void prePut(ObserverContext<RegionCoprocessorEnvironment> env,
	// Put put, WALEdit edit, Durability durability) throws IOException {
	// if (!doNotRun) {
	// RegionIndex regionIndex = RegionIndexMap.getInstance().get(
	// regionName);
	//
	// if (regionIndex != null) {
	// Set<String> indexedColumns = regionIndex.getIndexedColumns();
	//
	// if (!indexedColumns.isEmpty()) {
	// List<KeyValue> kVListToIndex = new ArrayList<KeyValue>();
	// getValueToIndex(put, kVListToIndex, indexedColumns);
	//
	// if (!kVListToIndex.isEmpty()) {
	// Get get = new Get(put.getRow());
	// Result result = null;
	//
	// try {
	// result = env.getEnvironment().getRegion().get(get);
	// } catch (IOException IOe) {
	// LOG.error(
	// "INDEX: PUT: Failed to retrieve the current row. "
	// + "This is required for index update. The index may be in an "
	// + "invalid state if the put succeeds and affects an already "
	// + "indexed column value.", IOe);
	// throw IOe;
	// }
	// updateTableIndexes(kVListToIndex, result, regionIndex);
	// }
	// }
	// }
	// }
	// }

	// changed
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> env,
			Put put, WALEdit edit, Durability durability) throws IOException {
		if (!doNotRun) {

      long startTime = System.nanoTime();

			RegionIndex regionIndex = RegionIndexMap.getInstance().get(
					regionName);

			if (regionIndex != null) {

				// added
				HashMap<String, Set<IndexedColumn>> singleMappedIndex = regionIndex
						.getSingleMappedIndex();
				Set<String> singleMappedIndexKeySet = singleMappedIndex
						.keySet();

				// Set<String> indexedColumns = regionIndex.getIndexedColumns();

				if (!singleMappedIndexKeySet.isEmpty()) {
					// Contains cf:qualifier=value from put request
					List<KeyValue> kVListToIndex = new ArrayList<KeyValue>();
					// Contains all the singleMapped key: Set<cf:a,cf:b>
					getValueToIndex(put, kVListToIndex, singleMappedIndexKeySet);

					if (!kVListToIndex.isEmpty()) {
						Get get = new Get(put.getRow());
						Result result = null;

						try {
							result = env.getEnvironment().getRegion().get(get);
						} catch (IOException IOe) {
							LOG.error(
									"INDEX: PUT: Failed to retrieve the current row. "
											+ "This is required for index update. The index may be in an "
											+ "invalid state if the put succeeds and affects an already "
											+ "indexed column value.", IOe);
							throw IOe;
						}

						// changed
						if( result != null) {
							updateTableIndexes(kVListToIndex, singleMappedIndex,
									result, regionIndex, put.getRow());
						}

						// updateTableIndexes(kVListToIndex, result,
						// regionIndex);
					}
				}
			}

      long duration = (System.nanoTime() - startTime)/1000;
      LOG.trace(tableName.getNameAsString() + ": prePut (inmem): " + duration + " us");
		}
	}

	// @Override
	// public void preDelete(ObserverContext<RegionCoprocessorEnvironment> env,
	// Delete delete, WALEdit edit, Durability durability)
	// throws IOException {
	// if (!doNotRun) {
	// RegionIndex regionIndex = RegionIndexMap.getInstance().get(regionName);
	//
	// if (regionIndex != null) {
	//
	// // added
	// HashMap<String, Set<IndexedColumn>> singleMappedIndex =
	// regionIndex.getSingleMappedIndex();
	// Set<String> indexedColumns = regionIndex.getIndexedColumns();
	//
	// if (!indexedColumns.isEmpty()) {
	//
	// Get get = new Get(delete.getRow());
	// Result result = null;
	//
	// try {
	// result = env.getEnvironment().getRegion().get(get);
	// } catch (IOException IOe) {
	// LOG.error(
	// "INDEX: PUT: Failed to retrieve the current row. "
	// + "This is required for index update. The index may be in an "
	// + "invalid state if the put succeeds and affects an already "
	// + "indexed column value.", IOe);
	// throw IOe;
	// }
	// updateTableIndexesForDelete(result, regionIndex,
	// indexedColumns, delete);
	// }
	// }
	// }
	// }

	// changed
	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> env,
			Delete delete, WALEdit edit, Durability durability)
			throws IOException {
		if (!doNotRun) {
			RegionIndex regionIndex = RegionIndexMap.getInstance().get(
					regionName);

			if (regionIndex != null) {

				// added
				HashMap<String, Set<IndexedColumn>> singleMappedIndex = regionIndex
						.getSingleMappedIndex();
				Set<String> indexedColumns = regionIndex.getIndexedColumns();

				if (!indexedColumns.isEmpty()) {

					Get get = new Get(delete.getRow());
					Result result = null;

					try {
						result = env.getEnvironment().getRegion().get(get);
					} catch (IOException IOe) {
						LOG.error(
								"INDEX: PUT: Failed to retrieve the current row. "
										+ "This is required for index update. The index may be in an "
										+ "invalid state if the put succeeds and affects an already "
										+ "indexed column value.", IOe);
						throw IOe;
					}
					if(result != null) {
						updateTableIndexesForDelete(result, singleMappedIndex,
								regionIndex, indexedColumns, delete);
					}
				}
			}
		}
	}

	private void updateTableIndexesForDelete(Result currentRow,
			RegionIndex regionIndex, Set<String> indexedColumns, Delete delete)
			throws IOException {

		List<KeyValue> kVListToDelete = makeKVListForDeletion(delete,
				indexedColumns, currentRow);

		for (KeyValue kv : kVListToDelete) {
			try {
				KeyValue currentValue = currentRow.getColumnLatest(
						kv.getFamily(), kv.getQualifier());

				if (currentValue != null) {
					// There is a current value for the column but it is
					// different from the one to be added
					// => update current value's index to remove the reference
					removeCurrentValueRef(kv.getRow(), currentValue,
							regionIndex);
				} else if (currentValue == null) {
					// Nothing to do, there is nothing to delete in that cell
				}
			} catch (IOException IOe) {
				LOG.error(
						"INDEX: DELETE: Failed to remove from index for "
								+ "table [" + tableName.toString() + "], "
								+ "column [" + Bytes.toString(kv.getFamily())
								+ ":" + Bytes.toString(kv.getQualifier()) + "]",
						IOe);
				throw IOe;
			} catch (ClassNotFoundException CNFe) {
				LOG.error(
						"INDEX: DELETE: Failed to remove from index for "
								+ "table [" + tableName.toString() + "], "
								+ "column [" + Bytes.toString(kv.getFamily())
								+ ":" + Bytes.toString(kv.getQualifier()) + "]",
						CNFe);
				throw new IOException(CNFe);
			}
		}
	}

	// changed
	private void updateTableIndexesForDelete(Result currentRow,
			HashMap<String, Set<IndexedColumn>> singleMappedIndex,
			RegionIndex regionIndex, Set<String> indexedColumns, Delete delete)
			throws IOException {

		// changed
		List<KeyValue> kVListToDelete = makeKVListForDeletion(delete,
				singleMappedIndex.keySet(), currentRow);

		Set<IndexedColumn> changedIndexColumnSet;
		Set<IndexedColumn> alreadyChangedIndexColumnSet = new HashSet<IndexedColumn>();
		KeyValue nextColumn = null;
		boolean isIndexed = false;
		byte[] concatValue;

		for (KeyValue kv : kVListToDelete) {
			try {
				KeyValue currentValue = currentRow.getColumnLatest(
						kv.getFamily(), kv.getQualifier());

				if (currentValue != null) {
					// There is a current value for the column

					// removeCurrentValueRef(kv.getRow(), currentValue,
					// regionIndex);

					changedIndexColumnSet = singleMappedIndex.get(new Column(kv
							.getFamily()).setQualifier(kv.getQualifier())
							.toString());
					for (IndexedColumn changedIndexColumn : changedIndexColumnSet) {
						if (alreadyChangedIndexColumnSet
								.contains(changedIndexColumn)) {
							continue;
						} else {
							alreadyChangedIndexColumnSet
									.add(changedIndexColumn);
							isIndexed = true;
							concatValue = null;
							for (Column column : changedIndexColumn
									.getColumnList()) {

								nextColumn = currentRow.getColumnLatest(
										column.getFamily(),
										column.getQualifier());
								if (nextColumn != null) {
									concatValue = Util.concatByteArray(
											concatValue, nextColumn.getValue());
								} else {
									// the column doesn't exist
									isIndexed = false;
									break;
								}

							}
							if (isIndexed) {
								// <1>: remove the current value
								removeCurrentValueRefFromIndex(kv.getRow(),
										changedIndexColumn.toString(),
										concatValue, regionIndex);
							}

						}

					}

				} else if (currentValue == null) {
					// Nothing to do, there is nothing to delete in that cell
				}
			} catch (IOException IOe) {
				LOG.error(
						"INDEX: DELETE: Failed to remove from index for "
								+ "table [" + tableName.toString() + "], "
								+ "column [" + Bytes.toString(kv.getFamily())
								+ ":" + Bytes.toString(kv.getQualifier()) + "]",
						IOe);
				throw IOe;
			} catch (ClassNotFoundException CNFe) {
				LOG.error(
						"INDEX: DELETE: Failed to remove from index for "
								+ "table [" + tableName.toString() + "], "
								+ "column [" + Bytes.toString(kv.getFamily())
								+ ":" + Bytes.toString(kv.getQualifier()) + "]",
						CNFe);
				throw new IOException(CNFe);
			}
		}
	}

	// changed the indexedColumns to singleMappedIndexKeySet
	private List<KeyValue> makeKVListForDeletion(Delete delete,
			Set<String> singleMappedIndexKeySet, Result result) {
		List<KeyValue> indexedKVList = new ArrayList<KeyValue>();
		// method to check if the familyMap is empty
		if (delete.isEmpty()) {
			// DELETE ROW
			indexedKVList.addAll(getIndexedKV(Arrays.asList(result.raw()),
					singleMappedIndexKeySet));
		} else {
			for (byte[] family : delete.getFamilyMap().keySet()) {
				List<KeyValue> columnsToDelete = getColumsToDeleteForFamily(
						delete, family);
				// if no qualifier is found, DELETING ENTIRE COLUMN FAMILY
				if (columnsToDelete.isEmpty()) {
					List<KeyValue> columnsToDeleteCurrentValueList = new ArrayList<KeyValue>();
					for (KeyValue kv : result.raw()) {
						if (Arrays.equals(kv.getFamily(), family)) {
							columnsToDeleteCurrentValueList.add(kv);
							indexedKVList.addAll(getIndexedKV(
									columnsToDeleteCurrentValueList,
									singleMappedIndexKeySet));
						}
					}
					// DELETE FOUND QUALIFIER OF COLUMN FAMILY
				} else {
					List<KeyValue> columnsToDeleteCurrentValueList = new ArrayList<KeyValue>();
					for (KeyValue ctd : columnsToDelete) {
						for (KeyValue kv : result.raw()) {
							if (Arrays.equals(ctd.getQualifier(),
									kv.getQualifier())) {
								columnsToDeleteCurrentValueList.add(kv);
								break;
							}
						}
					}
					indexedKVList.addAll(getIndexedKV(
							columnsToDeleteCurrentValueList,
							singleMappedIndexKeySet));
				}

			}
		}
		return indexedKVList;
	}

	private List<KeyValue> getIndexedKV(List<KeyValue> columnsToDelete,
			Set<String> indexedColumns) {
		List<KeyValue> result = new ArrayList<KeyValue>();
		for (KeyValue kv : columnsToDelete) {
			Column column = new Column(kv.getFamily(), kv.getQualifier());
			if (indexedColumns.contains(column.toString())) {
				result.add(kv);
			}
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private List<KeyValue> getColumsToDeleteForFamily(Delete del, byte[] family) {
		List<KeyValue> result = new ArrayList<KeyValue>();
		for (KeyValue kv : (List<KeyValue>) del.getFamilyMap().get(family)) {
			if (kv.getQualifierLength() > 0) {
				result.add(kv);
			}
		}
		return result;
	}
}
