package ca.mcgill.distsys.hbase.indexcoprocessorsinmem;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.ByteUtil;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.Util;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.IndexedColumnQuery;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.ProtoResult;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

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

public class RegionIndex implements Serializable {
    private static final long serialVersionUID = 2883387553546148042L;
    private HashMap<String, RegionColumnIndex> colIndex;

    private transient ReadWriteLock rwLock;
    private int maxTreeSize;
    private boolean splitting;

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        rwLock = new ReentrantReadWriteLock(true);
    }

    public RegionIndex(int maxTreeSize) {
        colIndex = new HashMap<String, RegionColumnIndex>();
        rwLock = new ReentrantReadWriteLock(true);
        this.maxTreeSize = maxTreeSize;
    }

    public boolean isEmpty() {
        rwLock.readLock().lock();

        try {
            return colIndex.isEmpty();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void add(byte[] columnFamily, byte[] qualifier, HRegion region) throws IOException, ClassNotFoundException {
        rwLock.writeLock().lock();

        try {
            String key = Bytes.toString(Util.concatByteArray(columnFamily, qualifier));

            if (colIndex.get(key) == null) {
                RegionColumnIndex newColIdx = new RegionColumnIndex(maxTreeSize, columnFamily, qualifier);
                colIndex.put(key, newColIdx);
                newColIdx.fullBuild(region);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void remove(byte[] columnFamily, byte[] qualifier) {
        rwLock.writeLock().lock();

        try {
            colIndex.remove(Bytes.toString(Util.concatByteArray(columnFamily, qualifier)));
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void removeKey(String key) {
        rwLock.writeLock().lock();

        try {
            colIndex.remove(key);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public Set<String> getIndexedColumns() {
        return colIndex.keySet();
    }

    public RegionColumnIndex get(byte[] family, byte[] qualifier) throws IOException {
        rwLock.readLock().lock();
        if(splitting) {
            throw new IOException("The Region and Region Index are being split; no updates possible at this moment.");
        }
        try {
            return colIndex.get(Bytes.toString(Util.concatByteArray(family, qualifier)));
        } finally {
            rwLock.readLock().unlock();
        }

    }

    public void split(RegionIndex daughterRegionAIndex, RegionIndex daughterRegionBIndex, byte[] splitRow) throws IOException, ClassNotFoundException {
        rwLock.writeLock().lock();

        try {
            for (String column : colIndex.keySet()) {
                RegionColumnIndex rci = colIndex.get(column);
                RegionColumnIndex rciDaughterRegionA = new RegionColumnIndex(maxTreeSize, rci.getColumnFamily(), rci.getQualifier());
                RegionColumnIndex rciDaughterRegionB = new RegionColumnIndex(maxTreeSize, rci.getColumnFamily(), rci.getQualifier());
                for (String value : rci.keySet()) {
                    byte[][] sortedPKRefArray = rci.get(value);
                    int splitPoint = Arrays.binarySearch(sortedPKRefArray, splitRow, ByteUtil.BYTES_COMPARATOR);
                    for (int i = 0; i < sortedPKRefArray.length; i++) {
                        if ((splitPoint >= 0 && i < splitPoint) || (splitPoint < 0 && i < Math.abs(splitPoint + 1))) {
                            rciDaughterRegionA.internalAdd(Bytes.toBytes(value), sortedPKRefArray[i]);
                        } else {
                            rciDaughterRegionB.internalAdd(Bytes.toBytes(value), sortedPKRefArray[i]);
                        }
                    }
                }

                if (rciDaughterRegionA.keySet().size() > 0) {                    
                    daughterRegionAIndex.colIndex.put(column, rciDaughterRegionA);
                }
                if (rciDaughterRegionB.keySet().size() > 0) {
                    daughterRegionBIndex.colIndex.put(column, rciDaughterRegionB);
                }

            }
            

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public List<ProtoResult> filterRowsFromCriteria(List<Criterion<?>> criteriaOnIndexColumns, List<Criterion<?>> criteriaOnNonIndexedColumns,
            IndexedColumnQuery query, HRegion region) throws IOException, ClassNotFoundException {
        rwLock.readLock().lock();

        try {
            Set<byte[]> result = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
            boolean firstRows = true;
            boolean mustPassAll = query.isMustPassAllCriteria();
            /*
             * Filter from index
             */
            for (Criterion<?> criterion : criteriaOnIndexColumns) {
                String criterionColumn = Bytes.toString(criterion.getCompareColumn().getFamily())
                        + Bytes.toString(criterion.getCompareColumn().getQualifier());
    
                RegionColumnIndex rci = colIndex.get(criterionColumn);

                Set<byte[]> partialRows = rci.filterRowsFromCriteria(criterion);
                if (!partialRows.isEmpty()) {
                    if (firstRows || !mustPassAll) {
                        result.addAll(partialRows);
                    } else {
                        result.retainAll(partialRows);
                    }
                } else {
                    if (mustPassAll) {
                        // intersection of an empty set with any set yields an
                        // empty set.
                        return new ArrayList<ProtoResult>(0);
                    } else {
                        // continue
                    }
                }
                firstRows = false;
            }

            /*
             * Get the results from the region, further filtering by criteria on
             * non indexed columns.
             */
            FilterList filterList = buildFilterListFromCriteria(criteriaOnNonIndexedColumns, query.isMustPassAllCriteria());
            List<ProtoResult> filteredRows = prefilteredLocalMultiGet(result, filterList, query.getColumnList(), region);

            return filteredRows;
        } finally {
            rwLock.readLock().unlock();
        }

    }

    private static FilterList buildFilterListFromCriteria(List<Criterion<?>> criteriaOnNonIndexedColumns, boolean mustPassAllCriteria) {
        if (!criteriaOnNonIndexedColumns.isEmpty()) {
            FilterList filterList;
            if (mustPassAllCriteria) {
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            } else {
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            }

            for (Criterion<?> criterion : criteriaOnNonIndexedColumns) {
                filterList.addFilter(criterion.toFilter());
            }

            return filterList;
        }
        return null;
    }

    private List<ProtoResult> prefilteredLocalMultiGet(Set<byte[]> rows, FilterList filterList, List<Column> columnList, HRegion region)
            throws IOException {

        List<ProtoResult> resultList = new ArrayList<ProtoResult>(rows.size());

        for (byte[] row : rows) {
            Get get = new Get(row);
            for (Column col : columnList) {
                get.addColumn(col.getFamily(), col.getQualifier());
            }
            get.setFilter(filterList);

            Result result = region.get(get);
            if (result != null && !result.isEmpty()) {
                resultList.add(Util.toResult(result));
            }
        }

        return resultList;
    }

    public void setSplitting(boolean b) {
        splitting = true;
        
    }
}
