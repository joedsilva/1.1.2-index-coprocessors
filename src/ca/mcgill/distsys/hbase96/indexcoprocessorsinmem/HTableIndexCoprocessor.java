package ca.mcgill.distsys.hbase96.indexcoprocessorsinmem;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

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
import org.apache.hadoop.hbase.util.FSUtils;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.IndexedColumn;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.SecondaryIndexConstants;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.Util;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex.AbstractPluggableIndex;

public class HTableIndexCoprocessor extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(HTableIndexCoprocessor.class);
    private byte[] tableName;
    private boolean doNotRun = false;
    private Configuration configuration;
    private HRegion region;
    private String regionName;

    @Override
    public void start(CoprocessorEnvironment environment) throws IOException {
        // make sure we are on a region server
        if (!(environment instanceof RegionCoprocessorEnvironment)) {
            throw new IllegalArgumentException("INDEX: Indexes only act on regions - started in an environment that was not a region");
        }

        RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) environment;
        region = env.getRegion();
        HTableDescriptor desc = region.getTableDesc();

        tableName = desc.getName();
       
        // MOdified by Cong
        // MASTER_INDEX_TABLE_NAME
        // Name of the master index table that contains which columns are indexed for which tables.
//        if (Arrays.equals(tableName, HConstants.ROOT_TABLE_NAME)|| Arrays.equals(tableName, HConstants.META_TABLE_NAME)
//                || new String(tableName).equals(SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME)) {
        if (desc.isRootRegion() || Arrays.equals(tableName, HConstants.META_TABLE_NAME)
                || new String(tableName).equals(SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME)) {
            doNotRun = true;
        } else {
            configuration = HBaseConfiguration.create();
            // configuration = env.getConfiguration();
            regionName = region.getRegionNameAsString();
            LOG.info("INDEX: Starting HTableIndexCoprocessor on region [" + regionName + "].");
        }
    }

    private void loadRegionIndexes() throws IOException {

        try {
            loadIndexFromFS();
            updateRegionIndexes();
        } catch (FileNotFoundException FNFe) {
            LOG.info("INDEX: No file index for region [" + new String(regionName)
                    + "] found; checking master table in case the file was deleted and rebuilding if required.");
            try {
                rebuildRegionIndexes();
            } catch (ClassNotFoundException e) {
                LOG.fatal("INDEX: Failed to create index for " + new String(regionName) + "]", e);
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            LOG.warn("INDEX: Failed to read index for region [" + new String(regionName)
                    + "] from FS or update it. Rebuilding entire region's index. this may take a while.", e);
            try {
                rebuildRegionIndexes();
            } catch (ClassNotFoundException e1) {
                LOG.fatal("INDEX: Could not rebuild entire index for region [" + new String(regionName)
                        + "] from scanning the region. Indexing for this region will be unavailable until this problem is fixed.", e1);
                throw new RuntimeException(e1);
            }
        }

    }

    private void loadIndexFromFS() throws IOException, ClassNotFoundException {
    	// Modified by Cong
//        Path tablePath = FSUtils.getTablePath(FSUtils.getRootDir(configuration), region.getTableDesc().getName());
        Path tablePath = FSUtils.getTableDir(FSUtils.getRootDir(configuration), region.getTableDesc().getTableName());
    	Path regionIndexPath = new Path(tablePath, regionName + "__index");
        FileSystem fs = FileSystem.get(configuration);

        LOG.info("INDEX: Opening index for region [" + new String(regionName) + "] from file.");
        if (!fs.exists(regionIndexPath)) {
            LOG.info("INDEX: No file index for region [" + new String(regionName)
                    + "] found; checking if it has been left an index by its splitting parent.");

            regionIndexPath = new Path(tablePath, "postSplit," + new String(region.getStartKey()) + ".__index");
            if (!fs.exists(regionIndexPath)) {
                throw new FileNotFoundException(regionIndexPath.toString());
            }
            LOG.info("INDEX: Opening postSplit index for region [" + new String(regionName) + "].");
        }

        FSDataInputStream in = fs.open(regionIndexPath);
        SnappyInputStream sis = new SnappyInputStream(in);
        ObjectInputStream ois = new ObjectInputStream(sis);

        RegionIndex regionIndex = (RegionIndex) ois.readObject();
        RegionIndexMap.getInstance().add(regionName, regionIndex);
        ois.close();

        try {
            fs.delete(regionIndexPath, false);
        } catch (IOException IOe) {
            LOG.warn("INDEX: Failed to delete index file [" + regionIndexPath.toString() + "] after loading it.", IOe);
        }

    }

    private void rebuildRegionIndexes() throws IOException, ClassNotFoundException {
        List<IndexedColumn> regionIndexedColumns = getIndexedColumns();

        if (!regionIndexedColumns.isEmpty()) {
            int maxTreeSize = configuration.getInt(SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
                    SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
            RegionIndex regionIndex = new RegionIndex(maxTreeSize);
            for (IndexedColumn column : regionIndexedColumns) {
                LOG.info("INDEX: Building index for region [" + region.getRegionNameAsString() + "; column [" + new String(column.getColumnFamily())
                        + ":" + new String(column.getQualifier()) + "].");
                // Need to be done by Cong
                
//                regionIndex.add(column.getColumnFamily(), column.getQualifier(), region);
                
                LOG.info("INDEX: Finished building index for region [" + region.getRegionNameAsString() + "; column ["
                        + new String(column.getColumnFamily()) + ":" + new String(column.getQualifier()) + "].");
            }
            RegionIndexMap.getInstance().add(regionName, regionIndex);
        }
    }

    private List<IndexedColumn> getIndexedColumns() throws IOException {
        HBaseAdmin admin = null;
        List<IndexedColumn> result = new ArrayList<IndexedColumn>();
        try {
            admin = new HBaseAdmin(configuration);

            if (!admin.tableExists(SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME)) {
                return result;
            }
        } finally {
            if (admin != null) {
                admin.close();
            }
        }

        HTableInterface masterIdxTable = null;
        try {
            masterIdxTable = new HTable(configuration, SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME);
            Get get = new Get(tableName);
            Result rs = masterIdxTable.get(get);
            for (KeyValue kv : rs.raw()) {
                byte[] serializedIndexedColumn = kv.getValue();
                if (serializedIndexedColumn != null) {
                    ByteArrayInputStream bais = new ByteArrayInputStream(serializedIndexedColumn);
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    try {
                        result.add((IndexedColumn) ois.readObject());
                    } catch (ClassNotFoundException e) {
                        LOG.error("INDEX: Invalid entry in master index table for indexed table [" + new String(tableName) + "].", e);
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

    public synchronized void updateRegionIndexes() throws IOException, ClassNotFoundException {
        List<IndexedColumn> meta_RegionIndexedColumns = getIndexedColumns();
        Set<String> regionindexedColumns;
        RegionIndex regionIndex = RegionIndexMap.getInstance().get(regionName);

        // Remove if deleted
        if (regionIndex != null) {
            regionindexedColumns = regionIndex.getIndexedColumns();
            for (String idxCol : regionindexedColumns) {
                boolean found = false;
                for (IndexedColumn meta_idxCol : meta_RegionIndexedColumns) {
                    String idxColKey = new String(Util.concatByteArray(meta_idxCol.getColumnFamily(), meta_idxCol.getQualifier()));
                    if (new String(idxColKey).equals(idxCol)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    regionIndex.removeKey(idxCol);
                }
            }
        }

        // Add new indexed columns
        if (meta_RegionIndexedColumns.size() > 0) {
            if (regionIndex == null) {
                int maxTreeSize = configuration.getInt(SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
                        SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
                regionIndex = new RegionIndex(maxTreeSize);
            }

            for (IndexedColumn idxCol : meta_RegionIndexedColumns) {
                String idxColKey = new String(Util.concatByteArray(idxCol.getColumnFamily(), idxCol.getQualifier()));
                if (!regionIndex.getIndexedColumns().contains(idxColKey)) {
                	// Need to be done by Cong
                	
//                    regionIndex.add(idxCol.getColumnFamily(), idxCol.getQualifier(), region);
                
                }
            }
        }

        if (regionIndex != null && RegionIndexMap.getInstance().get(regionName) == null) {
            RegionIndexMap.getInstance().add(regionName, regionIndex);
        }
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        if (!doNotRun) {
            try {
                LOG.info("INDEX: Region [" + regionName + "] postOpen; initializing indexes.");
                loadRegionIndexes();
            } catch (IOException IOe) {
                LOG.fatal("INDEX: Failed to load the region's index for table table [" + new String(tableName) + "]", IOe);
                // TODO close the region??
                throw new RuntimeException(IOe);
            }
        }
    }

    private void updateTableIndexes(List<KeyValue> kVListToIndex, Result currentRow, RegionIndex regionIndex) throws IOException {

        for (KeyValue kv : kVListToIndex) {
            try {
                KeyValue currentValue = currentRow.getColumnLatest(kv.getFamily(), kv.getQualifier());

                if (currentValue != null && !Arrays.equals(currentValue.getValue(), kv.getValue())) {
                    // There is a current value for the column but it is
                    // different from the one to be added
                    // => update current value's index to remove the reference
                    removeCurrentValueRef(kv.getRow(), currentValue, regionIndex);
                    addNewValueRef(kv, regionIndex);
                } else if (currentValue == null) {
                    // There is no current value, just add to the index
                    addNewValueRef(kv, regionIndex);
                } else {
                    // Nothing to do, new value is the same as the old value
                }
            } catch (IOException IOe) {
                LOG.error("INDEX: PUT: Failed to add to index for table [" + new String(tableName) + "], column [" + new String(kv.getFamily()) + ":"
                        + new String(kv.getQualifier() + "]"), IOe);
                throw IOe;
            } catch (ClassNotFoundException CNFe) {
                LOG.error("INDEX: PUT: Failed to add to index for table [" + new String(tableName) + "], column [" + new String(kv.getFamily()) + ":"
                        + new String(kv.getQualifier() + "]"), CNFe);
                throw new IOException(CNFe);
            }
        }
    }

    private void addNewValueRef(KeyValue kv, RegionIndex regionIndex) throws IOException, ClassNotFoundException {
        // Changed by Cong
//    	RegionColumnIndex rci = regionIndex.get(kv.getFamily(), kv.getQualifier());
    	AbstractPluggableIndex rci = regionIndex.get(kv.getFamily(), kv.getQualifier());
        rci.add(kv.getValue(), kv.getRow());

    }

    private void removeCurrentValueRef(byte[] row, KeyValue currentValue, RegionIndex regionIndex) throws IOException, ClassNotFoundException {
        // Changed by Cong
//    	RegionColumnIndex rci = regionIndex.get(currentValue.getFamily(), currentValue.getQualifier());
    	AbstractPluggableIndex rci = regionIndex.get(currentValue.getFamily(), currentValue.getQualifier());
    	rci.removeValueFromIdx(currentValue.getValue(), row);
    }

    @SuppressWarnings("unchecked")
    private void getValueToIndex(Put put, List<KeyValue> kVListToIndex, Set<String> indexedColumns) {
        for (byte[] family : put.getFamilyMap().keySet()) {
            for (KeyValue kv : (List<KeyValue>) put.getFamilyMap().get(family)) {
                String colName = new String(Util.concatByteArray(family, kv.getQualifier()));
                if (indexedColumns.contains(colName)) {
                    kVListToIndex.add(kv);
                }
            }
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        LOG.info("INDEX: HTableIndexCoprocessor stop for Region [" + regionName + "] postOpen; initializing indexes.");
    }

    private void persistIndexToFS() throws IOException {
        RegionIndex regionIndex = RegionIndexMap.getInstance().get(regionName);
        
        // Modified by Cong
//        Path tablePath = FSUtils.getTablePath(FSUtils.getRootDir(configuration), region.getTableDesc().getName());
        Path tablePath = FSUtils.getTableDir(FSUtils.getRootDir(configuration), region.getTableDesc().getTableName());
        
        if (regionIndex != null) {
            Path regionIndexPath = new Path(tablePath, region.getRegionNameAsString() + "__index");
            FileSystem fs = FileSystem.get(configuration);

            FsPermission perms = FSUtils.getFilePermissions(fs, configuration, HConstants.DATA_FILE_UMASK_KEY);

            FSDataOutputStream out = FileSystem.create(fs, regionIndexPath, perms);
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
        return tableName;
    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
        if (!doNotRun) {
            try {
                LOG.info("INDEX: Region [" + regionName + "] postClose; persisting region indexes.");
                persistIndexToFS();
            } catch (IOException e1) {
                LOG.error("INDEX: Failed to persist index to filesystem for region [" + region.getRegionNameAsString() + "].", e1);
            }
            RegionIndexMap.getInstance().remove(regionName);
        }
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c, byte[] splitRow) throws IOException {
        // we remove the ref to the memory index, we don't want it persisted
        // when the region closes due to a split;
        // the index will be rebuilt when the new regions open.
        LOG.info("INDEX: Region [" + regionName + "] preSplit; removing in memory indexes.");
        splitAndPersistIndex(splitRow);
        RegionIndexMap.getInstance().remove(regionName);
    }

    private void splitAndPersistIndex(byte[] splitRow) {
        RegionIndex regionIndex = RegionIndexMap.getInstance().get(regionName);
        regionIndex.setSplitting(true);
        if (regionIndex != null) {
            int maxTreeSize = configuration.getInt(SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
                    SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
            RegionIndex daughterRegionAIndex = new RegionIndex(maxTreeSize);
            RegionIndex daughterRegionBIndex = new RegionIndex(maxTreeSize);

            try {
                regionIndex.split(daughterRegionAIndex, daughterRegionBIndex, splitRow);

                String daughterRegionAIndexFilename = "postSplit," + new String(region.getStartKey()) + ".__index";
                String daughterRegionBIndexFilename = "postSplit," + new String(splitRow) + ".__index";

                if (!daughterRegionAIndex.isEmpty()) {
                    persistDaughterRegionIndex(daughterRegionAIndex, daughterRegionAIndexFilename);
                }
                if (!daughterRegionBIndex.isEmpty()) {
                    persistDaughterRegionIndex(daughterRegionBIndex, daughterRegionBIndexFilename);
                }
            } catch (Exception e) {
                LOG.warn("INDEX: Failed to save the daugher region indexes post split for region [" + regionName
                        + "]. Full rebuild of daughter region indexes may be required and take some time.", e);
            }

        }
    }

    private void persistDaughterRegionIndex(RegionIndex daughterRegionAIndex, String daughterRegionAIndexFilename) throws IOException {

//        Path tablePath = FSUtils.getTablePath(FSUtils.getRootDir(configuration), region.getTableDesc().getName());
        Path tablePath = FSUtils.getTableDir(FSUtils.getRootDir(configuration), region.getTableDesc().getTableName());

    	Path regionIndexPath = new Path(tablePath, daughterRegionAIndexFilename);
        FileSystem fs = FileSystem.get(configuration);

        FsPermission perms = FSUtils.getFilePermissions(fs, configuration, HConstants.DATA_FILE_UMASK_KEY);

        FSDataOutputStream out = FileSystem.create(fs, regionIndexPath, perms);
        SnappyOutputStream sos = new SnappyOutputStream(out);
        ObjectOutputStream oos = new ObjectOutputStream(sos);
        oos.writeObject(daughterRegionAIndex);
        oos.flush();
        oos.close();

    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> env, Put put, WALEdit edit, Durability durability) throws IOException {
        if (!doNotRun) {
            RegionIndex regionIndex = RegionIndexMap.getInstance().get(regionName);

            if (regionIndex != null) {
                Set<String> indexedColumns = regionIndex.getIndexedColumns();

                if (!indexedColumns.isEmpty()) {
                    List<KeyValue> kVListToIndex = new ArrayList<KeyValue>();
                    getValueToIndex(put, kVListToIndex, indexedColumns);

                    if (!kVListToIndex.isEmpty()) {
                        Get get = new Get(put.getRow());
                        Result result = null;

                        try {
                            result = env.getEnvironment().getRegion().get(get);
                        } catch (IOException e) {
                            LOG.error(
                                    "INDEX: PUT: Failed to retrieve the current row. This is required for index update. The index may be in an invalid state if the put succeeds and affects an already indexed column value.",
                                    e);
                            throw e;
                        }
                        updateTableIndexes(kVListToIndex, result, regionIndex);
                    }
                }
            }
        }
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> env, Delete delete, WALEdit edit, Durability durability) throws IOException {
        if (!doNotRun) {
            RegionIndex regionIndex = RegionIndexMap.getInstance().get(regionName);

            if (regionIndex != null) {
                Set<String> indexedColumns = regionIndex.getIndexedColumns();

                if (!indexedColumns.isEmpty()) {

                    Get get = new Get(delete.getRow());
                    Result result = null;

                    try {
                        result = env.getEnvironment().getRegion().get(get);
                    } catch (IOException e) {
                        LOG.error(
                                "INDEX: PUT: Failed to retrieve the current row. This is required for index update. The index may be in an invalid state if the put succeeds and affects an already indexed column value.",
                                e);
                        throw e;
                    }
                    updateTableIndexesForDelete(result, regionIndex, indexedColumns, delete);

                }
            }
        }
    }

    private void updateTableIndexesForDelete(Result currentRow, RegionIndex regionIndex, Set<String> indexedColumns, Delete delete)
            throws IOException {
        
        List<KeyValue> kVListToDelete = makeKVListForDeletion(delete, indexedColumns, currentRow);
        
        for (KeyValue kv : kVListToDelete) {
            try {
                KeyValue currentValue = currentRow.getColumnLatest(kv.getFamily(), kv.getQualifier());

                if (currentValue != null) {
                    // There is a current value for the column but it is
                    // different from the one to be added
                    // => update current value's index to remove the reference
                    removeCurrentValueRef(kv.getRow(), currentValue, regionIndex);
                } else if (currentValue == null) {
                    // Nothing to do, there is nothing to delete in that cell
                }
            } catch (IOException IOe) {
                LOG.error(
                        "INDEX: DELETE: Failed to remove from index for table [" + new String(tableName) + "], column [" + new String(kv.getFamily())
                                + ":" + new String(kv.getQualifier() + "]"), IOe);
                throw IOe;
            } catch (ClassNotFoundException CNFe) {
                LOG.error(
                        "INDEX: DELETE: Failed to remove from index for table [" + new String(tableName) + "], column [" + new String(kv.getFamily())
                                + ":" + new String(kv.getQualifier() + "]"), CNFe);
                throw new IOException(CNFe);
            }
        }
    }

    private List<KeyValue> makeKVListForDeletion(Delete delete, Set<String> indexedColumns, Result result) {
        List<KeyValue> indexedKVList = new ArrayList<KeyValue>();
        if (delete.isEmpty()) {
            // DELETE ROW
            indexedKVList.addAll(getIndexedKV(Arrays.asList(result.raw()), indexedColumns));
        } else {
            for (byte[] family : delete.getFamilyMap().keySet()) {
                List<KeyValue> columnsToDelete = getColumsToDeleteForFamily(delete, family);
                if (columnsToDelete.isEmpty()) {
                    // DELETE ENTIRE COLUMN FAMILY
                    List<KeyValue> columnsToDeleteCurrentValueList = new ArrayList<KeyValue>();
                    for (KeyValue kv : result.raw()) {
                        if (Arrays.equals(kv.getFamily(), family)) {
                            columnsToDeleteCurrentValueList.add(kv);
                            indexedKVList.addAll(getIndexedKV(columnsToDeleteCurrentValueList, indexedColumns));
                        }
                    }
                } else {
                    // DELETE FOUND COLUMS OF COLUMN FAMILY
                    List<KeyValue> columnsToDeleteCurrentValueList = new ArrayList<KeyValue>();
                    for (KeyValue ctd : columnsToDelete) {
                        for (KeyValue kv : result.raw()) {
                            if (Arrays.equals(ctd.getQualifier(), kv.getQualifier())) {
                                columnsToDeleteCurrentValueList.add(kv);
                                break;
                            }
                        }
                    }
                    indexedKVList.addAll(getIndexedKV(columnsToDeleteCurrentValueList, indexedColumns));
                }

            }
        }
        return indexedKVList;
    }

    private List<KeyValue> getIndexedKV(List<KeyValue> columnsToDelete, Set<String> indexedColumns) {
        List<KeyValue> result = new ArrayList<KeyValue>();
        for (KeyValue kv : columnsToDelete) {
            if (indexedColumns.contains(new String(Util.concatByteArray(kv.getFamily(), kv.getQualifier())))) {
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
