package ca.mcgill.distsys.hbase96.indexcoprocessorsinmem;

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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import ca.mcgill.distsys.hbase.indexcoprocessorsinmem.RegionColumnIndex;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.ByteUtil;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.Util;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.IndexedColumnQuery;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.ProtoResult;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex.AbstractPluggableIndex;

public class RegionIndex implements Serializable {
    private static final long serialVersionUID = 2883387553546148042L;
    // Modified by COng
    private HashMap<String, AbstractPluggableIndex> colIndex;

    private transient ReadWriteLock rwLock;
    private int maxTreeSize;
    private boolean splitting = false;

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        rwLock = new ReentrantReadWriteLock(true);
    }

    public RegionIndex(int maxTreeSize) {
        colIndex = new HashMap<String, AbstractPluggableIndex>();
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

    public void add(byte[] columnFamily, byte[] qualifier, HRegion region, String indexType, Object[] arguments) throws IOException, ClassNotFoundException {
        rwLock.writeLock().lock();

        try {
            String key = new String(Util.concatByteArray(columnFamily, qualifier));

            if (colIndex.get(key) == null) {
                // Modified by Cong
            	AbstractPluggableIndex newColIdx = AbstractPluggableIndex.getInstance(indexType, arguments);
                colIndex.put(key, newColIdx);
                // Modified by Cong
                newColIdx.fullBuild(region);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void remove(byte[] columnFamily, byte[] qualifier) {
        rwLock.writeLock().lock();

        try {
            colIndex.remove(new String(Util.concatByteArray(columnFamily, qualifier)));
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

    public AbstractPluggableIndex get(byte[] family, byte[] qualifier) throws IOException {
        rwLock.readLock().lock();
        if(splitting) {
            throw new IOException("The Region and Region Index are being split; no updates possible at this moment.");
        }
        try {
            return colIndex.get(new String(Util.concatByteArray(family, qualifier)));
        } finally {
            rwLock.readLock().unlock();
        }

    }

    // Not Implemented yet!!!!
    public void split(RegionIndex daughterRegionAIndex, RegionIndex daughterRegionBIndex, byte[] splitRow) throws IOException, ClassNotFoundException {
        rwLock.writeLock().lock();

        try {
            for (String column : colIndex.keySet()) {
            	AbstractPluggableIndex rci = colIndex.get(column);
            	String indexType = "";
            	Object[] arguments = new Object[1];
            	AbstractPluggableIndex rciDaughterRegionA = AbstractPluggableIndex.getInstance(indexType, arguments);
            	AbstractPluggableIndex rciDaughterRegionB = AbstractPluggableIndex.getInstance(indexType, arguments);
//                for (String value : rci.keySet()) {
//                    byte[][] sortedPKRefArray = rci.get(value);
//                    int splitPoint = Arrays.binarySearch(sortedPKRefArray, splitRow, ByteUtil.BYTES_COMPARATOR);
//                    for (int i = 0; i < sortedPKRefArray.length; i++) {
//                        if ((splitPoint >= 0 && i < splitPoint) || (splitPoint < 0 && i < Math.abs(splitPoint + 1))) {
//                            rciDaughterRegionA.internalAdd(Bytes.toBytes(value), sortedPKRefArray[i]);
//                        } else {
//                            rciDaughterRegionB.internalAdd(Bytes.toBytes(value), sortedPKRefArray[i]);
//                        }
//                    }
//                }
//
//                if (rciDaughterRegionA.keySet().size() > 0) {                    
//                    daughterRegionAIndex.colIndex.put(column, rciDaughterRegionA);
//                }
//                if (rciDaughterRegionB.keySet().size() > 0) {
//                    daughterRegionBIndex.colIndex.put(column, rciDaughterRegionB);
//                }

            }
            

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // Return type also changed: BY Cong
    // We could have only 
//    public Set<byte[]> filterRowsFromCriteria(List<Criterion<?>> criteriaOnIndexColumns, List<Criterion<?>> criteriaOnNonIndexedColumns,
//            IndexedColumnQuery query, HRegion region) throws IOException, ClassNotFoundException {
//        rwLock.readLock().lock();
//
//        try {
//            Set<byte[]> result = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
//            boolean firstRows = true;
//            boolean mustPassAll = query.isMustPassAllCriteria();
//            /*
//             * Filter from index
//             */
//            for (Criterion<?> criterion : criteriaOnIndexColumns) {
//                String criterionColumn = new String(criterion.getCompareColumn().getFamily())
//                        + new String(criterion.getCompareColumn().getQualifier());
//                AbstractPluggableIndex rci = colIndex.get(criterionColumn);
//
//                Set<byte[]> partialRows = rci.filterRowsFromCriteria(criterion);
//                if (!partialRows.isEmpty()) {
////                if (partialRows !=null) {
//                    if (firstRows || !mustPassAll ) {
//                        result.addAll(partialRows);
//                    } else {
//                        result.retainAll(partialRows);
//                        // Check if there are elements still in the set 
//                        // Modified by Cong 
//                        if(result.isEmpty()){
//                        	return null;
//                        }
//                    }
//                } else {
//                    if (mustPassAll) {
//                        // intersection of an empty set with any set yields an
//                        // empty set.
//                    	// Modified by Cong
//                        return null;
//                    } else {
//                        // continue
//                    }
//                }
//                firstRows = false;
//            }
//
//            /*
//             * Get the results from the region, further filtering by criteria on
//             * non indexed columns.
//             */
////            FilterList filterList = buildFilterListFromCriteria(criteriaOnNonIndexedColumns, query.isMustPassAllCriteria());
////            List<ProtoResult> filteredRows = prefilteredLocalMultiGet(result, filterList, query.getColumnList(), region);
//
//            // Change the return type as well
//            return result;
//        } finally {
//            rwLock.readLock().unlock();
//        }
//
//    }

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
                String criterionColumn = new String(criterion.getCompareColumn().getFamily())
                        + new String(criterion.getCompareColumn().getQualifier());
    
                AbstractPluggableIndex rci = colIndex.get(criterionColumn);

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
