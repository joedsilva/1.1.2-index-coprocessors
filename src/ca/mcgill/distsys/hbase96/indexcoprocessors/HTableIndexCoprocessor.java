package ca.mcgill.distsys.hbase96.indexcoprocessors;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import ca.mcgill.distsys.hbase96.indexcommons.IndexedColumn;
import ca.mcgill.distsys.hbase96.indexcommons.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import ca.mcgill.distsys.hbase96.indexcoprocessors.jgroups.MasterIndexUpdateReceiver;
import ca.mcgill.distsys.hbase96.indexcommons.SecondaryIndexConstants;
import org.apache.hadoop.hbase.util.Bytes;

public class HTableIndexCoprocessor extends BaseRegionObserver {

    private static final Log LOG = LogFactory.getLog(HTableIndexCoprocessor.class);
    private Set<String> indexedColumns;
    private byte[] tableName;
    private String tableNameAsString;
    private Configuration configuration;
    private boolean doNotRun = false;
    private Object indexColumnsLock = new Object();
    private MasterIndexUpdateReceiver masterIndexUpdateReceiver;
    private boolean masterIndexBroadcast = true;
    private HBaseAdmin admin;
    private HTablePool hTablePool;
    private HashMap<String, Semaphore> tablePoolTableSemaphore;

    @Override
    public void start(CoprocessorEnvironment environment) {
        // make sure we are on a region server
        if (!(environment instanceof RegionCoprocessorEnvironment)) {
            throw new IllegalArgumentException("Indexes only act on regions - started in an environment that was not a region");
        }

        RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) environment;
        HTableDescriptor desc = env.getRegion().getTableDesc();

        tableName = desc.getName();
        tableNameAsString = desc.getNameAsString();

        //if (Bytes.toString(tableName).equals("-ROOT-") || Bytes.toString(tableName).equalsIgnoreCase(".META.")
        if (desc.getTableName().isSystemTable()
                || Bytes.toString(tableName).endsWith(
            SecondaryIndexConstants.INDEX_TABLE_SUFFIX)
                || Bytes.toString(tableName).equals(
            SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME)) {
            doNotRun = true;
            LOG.debug(
                "Not loading indexing coprocessor on " + desc.getNameAsString());
        } else {
            configuration = HBaseConfiguration.create();
            try {
                hTablePool = new HTablePool(configuration, 15);
                tablePoolTableSemaphore = new HashMap<String, Semaphore>();
                initIndexedColumnsForTable();
                if (masterIndexBroadcast) {
                    masterIndexUpdateReceiver = new MasterIndexUpdateReceiver();
                    masterIndexUpdateReceiver.start(this);
                }
            } catch (Exception e) {
                LOG.error("Failed to initialize Indexed Coprocessor for table [" + Bytes.toString(tableName) + "]", e);
                throw new RuntimeException(e);
            }

        }

    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        if (!doNotRun) {
            long startTime = System.nanoTime();
            HTableIndexPutHandler putHandler = new HTableIndexPutHandler(put, this, e);
            putHandler.processPut();
            long duration = (System.nanoTime() - startTime) / 1000;
            LOG.trace(tableNameAsString + ": prePut (HTable): " + duration + " us");
        }
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        if (!doNotRun) {
            HTableIndexDeleteHandler deleteHandler = new HTableIndexDeleteHandler(delete, this, e);
            deleteHandler.processDelete();
        }
    }

    @Override
    public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append, Result result) throws IOException {
        if (!doNotRun) {
            // TODO Auto-generated method stub

        }
        return super.postAppend(e, append, result);
    }

    public Set<String> getIndexedColumnsForTable() throws IOException {
        if (masterIndexBroadcast) {
            synchronized (indexColumnsLock) {
                if (indexedColumns != null)  {
                    return indexedColumns;
                }
            }
        }
        return initIndexedColumnsForTable();
    }

    public Set<String> initIndexedColumnsForTable() throws IOException {
        Set<String> result = new TreeSet<String>();

        try {
            if (admin == null) {
                admin = new HBaseAdmin(configuration);
            }
            if (!admin.tableExists(SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME)) {
                indexedColumns = result;
                return result;
            }
        } finally {
            if (admin != null) {
                admin.close();
            }
        }

        HTableInterface masterIdxTable = null;
        try {
            masterIdxTable = hTablePool.getTable(SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME);
            Get get = new Get(tableName);
            Result rs = masterIdxTable.get(get);
            for (KeyValue kv : rs.raw()) {
                try {
                    IndexedColumn ic = (IndexedColumn) Util.deserialize(kv.getValue());
                    result.add(Util.getSecondaryIndexTableName(tableNameAsString, ic));
                } catch (ClassNotFoundException ex) {
                    throw new IOException(ex);
                }
            }
        } finally {
            if (masterIdxTable != null) {
                masterIdxTable.close();
            }
        }
        if (masterIndexBroadcast) {
            synchronized (indexColumnsLock) {
                indexedColumns = result;
                updateTableLatches(result);
            }
        }
        return result;

    }

    private void updateTableLatches(Set<String> columns) {
        for (String col : columns) {
            Set<String> keySet = tablePoolTableSemaphore.keySet();
            if (!keySet.contains(col)) {
                tablePoolTableSemaphore.put(col, new Semaphore(10));
            }
        }
    }

    public void setIndexColumns(Set<String> idxCols) throws IOException {
        synchronized (indexColumnsLock) {
            indexedColumns = idxCols;
            updateTableLatches(idxCols);
        }
    }

    public byte[] getTableName() {
        return tableName;
    }
    public String getTableNameAsString() {
        return tableNameAsString;
    }


    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        if (masterIndexBroadcast && masterIndexUpdateReceiver != null) {
            masterIndexUpdateReceiver.stop();
        }

        if (hTablePool != null) {
            hTablePool.close();
        }
        super.stop(e);
    }

    public HTableInterface getHTableInterface(String tableName) {
        try {
            if (masterIndexBroadcast) {
                tablePoolTableSemaphore.get(tableName).acquire();
            }
        } catch (InterruptedException e) {
            LOG.error("Could not get table interface from table pool for " +
              tableName);
            throw new RuntimeException(e);
        }
        return hTablePool.getTable(tableName);
    }

    public void returnHTableInterface(HTableInterface table) throws IOException {
        String tableName = Bytes.toString(table.getTableName());
        table.close();
        if (masterIndexBroadcast) {
            tablePoolTableSemaphore.get(tableName).release();
        }
    }

}
