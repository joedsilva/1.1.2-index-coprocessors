package ca.mcgill.distsys.hbase.indexcoprocessorsinmem;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

// modified by Cong
import org.apache.hadoop.hbase.Cell;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion;

public class RegionColumnIndex implements Serializable {
    private static final long serialVersionUID = -3578521804781335557L;
    private transient static Log LOG;

    private HashMap<String, RowIndex> rowIndexMap;
    private transient ReadWriteLock rwLock;
    private byte[] columnFamily;
    private byte[] qualifier;
    private int maxTreeSize;

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        rwLock = new ReentrantReadWriteLock(true);
        LOG = LogFactory.getLog(RegionColumnIndex.class);
    }

    public RegionColumnIndex(int maxTreeSize, byte[] columnFamily, byte[] qualifier) {
        LOG = LogFactory.getLog(RegionColumnIndex.class);
        rowIndexMap = new HashMap<String, RowIndex>(15000);
        rwLock = new ReentrantReadWriteLock(true);
        this.columnFamily = columnFamily;
        this.qualifier = qualifier;
        this.maxTreeSize = maxTreeSize;
    }

    public void add(byte[] key, byte[] value) throws IOException, ClassNotFoundException {
        rwLock.writeLock().lock();

        try {
            internalAdd(key, Arrays.copyOf(value, value.length));
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    void internalAdd(byte[] key, byte[] value) throws IOException, ClassNotFoundException {
        RowIndex rowIndex;
        boolean newPKRefTree = false;
        String keyString = new String(key);
        rowIndex = rowIndexMap.get(keyString);
        if (rowIndex == null) {
            rowIndex = new RowIndex();
            newPKRefTree = true;
        }

        rowIndex.add(value, maxTreeSize);

        if (newPKRefTree) {
            rowIndexMap.put(keyString, rowIndex);
        }
    }

    public byte[][] get(byte[] key) throws IOException, ClassNotFoundException {
        return get(new String(key));
    }

    public byte[][] get(String key) throws IOException, ClassNotFoundException {
        rwLock.readLock().lock();

        try {
            TreeSet<byte[]> pkRefs;

            byte[][] result = null;

            RowIndex rowIndex = rowIndexMap.get(key);

            if (rowIndex != null) {
                pkRefs = rowIndex.getPKRefs();
                if (pkRefs != null) {
                    result = pkRefs.toArray(new byte[pkRefs.size()][]);
                }
            }
            return result;
        } finally {
            rwLock.readLock().unlock();
        }

    }

    public void fullBuild(HRegion region) throws IOException, ClassNotFoundException {
        rwLock.writeLock().lock();

        try {
            Scan scan = new Scan();
            scan.addColumn(columnFamily, qualifier);
            scan.setCacheBlocks(false); // don't want to fill the cache uselessly and create churn
            RegionScanner scanner = region.getScanner(scan);
            MultiVersionConsistencyControl.setThreadReadPoint(scanner.getMvccReadPoint());
            region.startRegionOperation();
            try {
                synchronized (scanner) {
                	
                    // Modified by Cong
//                	List<KeyValue> values = new ArrayList<KeyValue>();
                	List<Cell> values = new ArrayList<Cell>();
                    rowIndexMap.clear();
                    
                    boolean more;
                    do {
                    	
                        more = scanner.nextRaw(values);
                        if (!values.isEmpty() && values.get(0) != null && values.get(0).getValue() != null) {
                            if (values.get(0).getRow() == null) {
                                LOG.error("NULL ROW for VALUE [" + values.get(0).getValue() + "] in column [" + new String(columnFamily) + ":"
                                        + new String(qualifier) + "]");
                            } else {
                                byte[] rowid = values.get(0).getRow(); 
                                try {                                  
                                    internalAdd(values.get(0).getValue(), Arrays.copyOf(rowid, rowid.length));
                                } catch (NullPointerException NPEe) {
                                    LOG.error("NPE for VALUE [" + new String(values.get(0).getValue()) + "] ROW ["
                                            + new String(rowid) + "] in column [" + new String(columnFamily) + ":"
                                            + new String(qualifier) + "]", NPEe);
                                    throw NPEe;
                                }
                            }
                        }
                        values.clear();
                    } while (more);
                    scanner.close();
                }
            } finally {
                region.closeRegionOperation();
            }
        } finally {
            rwLock.writeLock().unlock();
        }

    }

    public void removeValueFromIdx(byte[] key, byte[] value) throws IOException, ClassNotFoundException {
        rwLock.writeLock().lock();
        try {
            RowIndex rowIndex = rowIndexMap.get(new String(key));
            if (rowIndex != null) {
                rowIndex.remove(value, maxTreeSize);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public Set<byte[]> filterRowsFromCriteria(Criterion<?> criterion) throws IOException, ClassNotFoundException {
        rwLock.readLock().lock();

        try{
            Set<byte[]> rowKeys = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
             for(String value: criterion.getMatchingValueSetFromIndex(rowIndexMap.keySet())) {
                 rowKeys.addAll(rowIndexMap.get(value).getPKRefs());
             }
             
             return rowKeys;
        } finally {
            rwLock.readLock().unlock();
        }
    }
    
    Set<String> keySet() {
        return rowIndexMap.keySet();
    }

    byte[] getColumnFamily() {
        return columnFamily;
    }

    byte[] getQualifier() {
        return qualifier;
    }
}
