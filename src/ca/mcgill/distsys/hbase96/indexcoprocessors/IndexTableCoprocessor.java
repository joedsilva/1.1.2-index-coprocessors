package ca.mcgill.distsys.hbase96.indexcoprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import ca.mcgill.distsys.hbase96.indexcommons.ByteUtil;
import ca.mcgill.distsys.hbase96.indexcommons.SecondaryIndexConstants;
import ca.mcgill.distsys.hbase96.indexcommons.Util;

public class IndexTableCoprocessor extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(IndexTableCoprocessor.class);
    private byte[] tableName;
    private String tableNameAsString;
    private boolean doNotRun = true;

    private long localGetElapsed = 0;
    private int localGetCount = 0;
    private long remoteGetElapsed;
    private int remoteGetCount;
    private ArrayList<Long> localElapsed;
    private ArrayList<Long> remoteElapsed;

    @Override
    public void start(CoprocessorEnvironment environment) throws IOException {
        // make sure we are on a region server
        if (!(environment instanceof RegionCoprocessorEnvironment)) {
            throw new IllegalArgumentException("Indexes only act on regions - started in an environment that was not a region");
        }

        RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) environment;
        HTableDescriptor desc = env.getRegion().getTableDesc();

        tableName = desc.getName();
        tableNameAsString = desc.getNameAsString();

        if (tableNameAsString.endsWith(SecondaryIndexConstants.INDEX_TABLE_SUFFIX)) {
            doNotRun = false;
            localElapsed = new ArrayList<Long>();
            remoteElapsed = new ArrayList<Long>();
        }

    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        // TODO Auto-generated method stub
        super.stop(e);
    }

    private void processAdditionToIdx(ObserverContext<RegionCoprocessorEnvironment> env, Put put, byte[] value) throws IOException {
        byte[] idxCF = Bytes.toBytes(SecondaryIndexConstants.INDEX_TABLE_IDX_CF_NAME);
        byte[] idxC = Bytes.toBytes(SecondaryIndexConstants.INDEX_TABLE_IDX_C_NAME);
        Get get = new Get(put.getRow());
        Result rs = null;

        HRegion region = env.getEnvironment().getRegion();

        try {
            rs = region.get(get);
        } catch (IOException e) {
            LOG.error("PUT: Failed to retrieve the current row. This is " +
                "required for index update. The index may be in an " +
                "invalid state if the put succeeds and affects an " +
                "already indexed column value.", e);
            throw e;
        }

        TreeSet<byte[]> primaryRowKeys;
        if (!rs.isEmpty()) {
            try {
                primaryRowKeys = Util.deserializeIndex(rs.getValue(idxCF, idxC));
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        } else {
            primaryRowKeys = new TreeSet<byte[]>(ByteUtil.BYTES_COMPARATOR);
        }

        if (!primaryRowKeys.contains(value)) {
            primaryRowKeys.add(value);
            Put idxPut = new Put(put.getRow());
            idxPut.add(idxCF, idxC, Util.serializeIndex(primaryRowKeys));

            region.put(idxPut);
        }

    }

    private void processRemovalFromIdx(ObserverContext<RegionCoprocessorEnvironment> env, Put put, byte[] value) throws IOException {
        byte[] idxCF = Bytes.toBytes(SecondaryIndexConstants.INDEX_TABLE_IDX_CF_NAME);
        byte[] idxC = Bytes.toBytes(SecondaryIndexConstants.INDEX_TABLE_IDX_C_NAME);
        Get get = new Get(put.getRow());
        Result rs = null;

        try {

            rs = env.getEnvironment().getRegion().get(get);
        } catch (IOException e) {
            LOG.error(
                    "PUT: Failed to retrieve the current row. This is required for index update. The index may be in an invalid state if the put succeeds and affects an already indexed column value.",
                    e);
            throw e;
        }

        TreeSet<byte[]> primaryRowKeys;
        if (!rs.isEmpty()) {
            try {
                primaryRowKeys = Util.deserializeIndex(rs.getValue(idxCF, idxC));
                if (primaryRowKeys.contains(value)) {
                    primaryRowKeys.remove(value);
                    Put idxPut = new Put(put.getRow());
                    idxPut.add(idxCF, idxC, Util.serializeIndex(primaryRowKeys));
                    env.getEnvironment().getRegion().put(idxPut);
                }
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> env, Put put, WALEdit edit, Durability durability) throws IOException {
        if (!doNotRun) {
            long startTime = System.nanoTime();

            byte[] operationType = ((KeyValue) (put.getFamilyMap().get(Bytes.toBytes(SecondaryIndexConstants.INDEX_TABLE_IDX_CF_NAME)).get(0)))
                    .getQualifier();
            byte[] value;
            if (Arrays.equals(operationType, SecondaryIndexConstants.INDEX_TABLE_IDX_PUT_TO_IDX)) {
                // LOG.debug("PREPUT: Got an override to a normal PUT; insert value into index.");
                value = ((KeyValue) put.getFamilyMap().get(Bytes.toBytes(SecondaryIndexConstants.INDEX_TABLE_IDX_CF_NAME)).get(0)).getValue();
                processAdditionToIdx(env, put, value);
                env.bypass();
            } else if (Arrays.equals(operationType, SecondaryIndexConstants.INDEX_TABLE_IDX_DEL_FROM_IDX)) {
                value = ((KeyValue) put.getFamilyMap().get(Bytes.toBytes(SecondaryIndexConstants.INDEX_TABLE_IDX_CF_NAME)).get(0)).getValue();
                // LOG.debug("PREPUT: Got an override to a normal PUT; remove value from index.");
                processRemovalFromIdx(env, put, value);
                env.bypass();
            }

            long duration = (System.nanoTime() - startTime) / 1000;
            LOG.trace(tableNameAsString + ": prePut (HTable internal): " + duration + " us");
        }
    }

    public synchronized void incLocalGetElapsed(long elapsed) {
        localElapsed.add(elapsed);
        localGetElapsed += elapsed;
        localGetCount++;
        if (localGetCount % 5000 == 0) {
            Collections.sort(localElapsed);
            LOG.debug("ELAPSED-LOCAL-GET-IDX: Processed " + localGetCount + " local gets in " + localGetElapsed / 1000 + "ms; average:"
                    + (localGetElapsed / localGetCount) / 1000 + "ms; median: " + median(localElapsed) / 1000 + "ms.");
        }
    }

    public synchronized void incLocalPutElapsed(long elapsed) {
        remoteElapsed.add(elapsed);
        remoteGetElapsed += elapsed;
        remoteGetCount++;
        if (remoteGetCount % 5000 == 0) {
            Collections.sort(remoteElapsed);
            LOG.debug("ELAPSED-LOCAL-PUT_IDX: Processed " + remoteGetCount + " local put in " + remoteGetElapsed / 1000 + "ms; average: "
                    + (remoteGetElapsed / remoteGetCount) / 1000 + "ms; median: " + median(remoteElapsed) / 1000 + "ms.");
        }
    }

    public static double median(ArrayList<Long> m) {
        int middle = m.size() / 2;
        if (m.size() % 2 == 1) {
            return m.get(middle);
        } else {
            return (m.get(middle - 1) + m.get(middle)) / 2.0;
        }
    }

}
