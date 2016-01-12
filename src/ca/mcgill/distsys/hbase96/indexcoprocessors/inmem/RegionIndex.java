package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem;

import ca.mcgill.distsys.hbase96.indexcommons.IndexedColumn;
import ca.mcgill.distsys.hbase96.indexcommons.Util;
import ca.mcgill.distsys.hbase96.indexcommons.proto.ByteArrayCriterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.IndexedColumnQuery;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion.CompareType;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.AbstractPluggableIndex;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.ProtoResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.util.MultidimensionalCounter.Iterator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RegionIndex implements Serializable {
	private static final Log LOG = LogFactory.getLog(RegionIndex.class);
	private static final long serialVersionUID = 2883387553546148042L;
	// Modified by Cong
	private HashMap<String, AbstractPluggableIndex> colIndex;
	// Hashmap<"family:qualifier", Set<IndexedColumn>>
	private HashMap<String, Set<IndexedColumn>> singleMappedIndex;

	private transient ReadWriteLock rwLock;
	private int maxTreeSize;
	private boolean splitting = false;

	private void readObject(ObjectInputStream in) throws IOException,
	ClassNotFoundException {
		in.defaultReadObject();
		rwLock = new ReentrantReadWriteLock(true);
	}

	public RegionIndex(int maxTreeSize) {
		colIndex = new HashMap<String, AbstractPluggableIndex>();
		singleMappedIndex = new HashMap<String, Set<IndexedColumn>>();
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

	public HashMap<String, Set<IndexedColumn>> getSingleMappedIndex() {
		return singleMappedIndex;
	}

	/*
	// put the single column index
	public void singleMappedPut(String key, byte [] family, byte [] qualifier) {
		IndexedColumn indexedColumn = new IndexedColumn(family, qualifier);
		if(singleMappedIndex.get(key) == null) {
			HashSet<IndexedColumn> innerMappedIndex = new HashSet<IndexedColumn>();
			innerMappedIndex.add(indexedColumn);
			singleMappedIndex.put(key, innerMappedIndex);
		} else {
			singleMappedIndex.get(key).add(indexedColumn);
		}
	}
	*/

	// put the multi Column index
	public void singleMappedPut(List<Column> colList) {
		IndexedColumn indexedColumn = new IndexedColumn(colList);
		String singleColumn = null;
		if(colList.size() != 0) {
			for(Column col : colList) {
				singleColumn = col.toString();
				if(singleMappedIndex.get(singleColumn) == null){
					HashSet<IndexedColumn> innerMappedIndex = new HashSet<IndexedColumn>();
					innerMappedIndex.add(indexedColumn);
					singleMappedIndex.put(singleColumn, innerMappedIndex);
				} else {
					singleMappedIndex.get(singleColumn).add(indexedColumn);
				}
			}
		}
	}
	
	public void removeFromSingleMappedIndex(List<Column> colList) {
		IndexedColumn removeCol = new IndexedColumn(colList);
		String singleColumn = null;
		Set<IndexedColumn> set = null;
		for(Column col: colList) {
			singleColumn = col.toString();
			set = singleMappedIndex.get(singleColumn);
			if(set != null) {
				set.remove(removeCol);
				if(set.size() == 0) {
					singleMappedIndex.remove(singleColumn);
				}
			}
		}
	}

	/*
	public void removeFromSingleMappedIndex(String key, byte [] family, byte [] qualifier) {
		IndexedColumn removeCol = new IndexedColumn(family, qualifier);
		Set<IndexedColumn> set = singleMappedIndex.get(key);
		if(set != null) {
			System.out.println("size: " + set.size());
			set.remove(removeCol);
			
			if(set.size() == 0) {
				singleMappedIndex.remove(key);
			}
		}
		
	}

	// single column index add
	public void add(byte[] columnFamily, byte[] qualifier, HRegion region,
			String indexType, Object[] arguments, Class<?> [] argumentsClasses) throws IOException,
			ClassNotFoundException, NoSuchMethodException {
		rwLock.writeLock().lock();

		try {
			String key = Bytes.toString(Util.concatByteArray(columnFamily,
					qualifier));
			// No need to check colindex, because indexCoprocessorEndpoint already checked
			// Modified by Cong
			AbstractPluggableIndex newColIdx = AbstractPluggableIndex
					.getInstance(false, indexType, arguments, argumentsClasses);
			colIndex.put(key, newColIdx);
			
			// added at July 7th
			singleMappedPut(key, columnFamily, qualifier);
			
			// Modified by Cong
			if (region != null) {
				newColIdx.fullBuild(region);
			}

		} finally {
			rwLock.writeLock().unlock();
		}
	}
	*/

	public void add (List<Column> colList, HRegion region, String indexType,
			Object[]arguments)
			throws IOException, ClassNotFoundException, NoSuchMethodException {
		rwLock.writeLock().lock();
		try {
			// Modified by Cong
			AbstractPluggableIndex newColIdx = AbstractPluggableIndex
					.getInstance(indexType, arguments);
			String idxColKey = Util.concatColumnsToString(colList);
			colIndex.put(idxColKey, newColIdx);
			// added by July 7th
			singleMappedPut(colList);
			// Modified by Cong
			if (region != null) {
				newColIdx.fullBuild(region);
			}
		} finally {
			rwLock.writeLock().unlock();
		}
	}

	/*
	public void remove(byte[] columnFamily, byte[] qualifier) {
		rwLock.writeLock().lock();
		try {
			String idxColKey = Bytes.toString(Util.concatByteArray(columnFamily,
					qualifier));
			colIndex.remove(idxColKey);
			
			removeFromSingleMappedIndex(idxColKey, columnFamily, qualifier);
			
		} finally {
			rwLock.writeLock().unlock();
		}
	}
	*/

	public void remove(List<Column> colList) {
		rwLock.writeLock().lock();
		try {
			String idxColKey = Util.concatColumnsToString(colList);
			colIndex.remove(idxColKey);
			removeFromSingleMappedIndex(colList);
		} finally {
			rwLock.writeLock().unlock();
		}
	}

	// TODO: find a way to remove singleMappedIndex as well
	// Used by HTableIndexCoprocessor.updateRegionIndexes()
	public void remove(String colList) {
		rwLock.writeLock().lock();
		try {
			//String idxColKey = Util.concatColumnsToString(colList);
			colIndex.remove(colList);
			removeFromSingleMappedIndex(Util.buildColumnList(colList));
		} finally {
			rwLock.writeLock().unlock();
		}
	}

	public Set<String> getIndexedColumns() {
		return colIndex.keySet();
	}

	public AbstractPluggableIndex get(String key)
	throws IOException {
		rwLock.readLock().lock();
		if (splitting) {
			throw new IOException("The Region and Region Index are being split; "
					+ "no updates possible at this moment.");
		}
		try {
			return colIndex.get(key);
		} finally {
			rwLock.readLock().unlock();
		}

	}

	/*
	public AbstractPluggableIndex getValue(byte [] key) throws IOException{

		rwLock.readLock().lock();
		if (splitting) {
		throw new IOException (
				"The Region and Region Index are being split; no updates possible at this moment.");
		}
		try {
			return colIndex.get(Bytes.toString(key));
		} finally {
			rwLock.readLock().unlock();
		}
	}
	*/

	// Not implemented
	public void split(RegionIndex daughterRegionAIndex,
			RegionIndex daughterRegionBIndex, byte[] splitRow)
			throws IOException, ClassNotFoundException, NoSuchMethodException {
		rwLock.writeLock().lock();

		try {
			for (String column : colIndex.keySet()) {
				AbstractPluggableIndex rci = colIndex.get(column);
				String indexType = rci.getIndexType();
				Object[] arguments = rci.getArguments();
				//boolean isMultiColumn = rci.getIsMultiColumn();
				AbstractPluggableIndex rciDaughterRegionA =
						AbstractPluggableIndex
								.getInstance(indexType, arguments);
				AbstractPluggableIndex rciDaughterRegionB =
						AbstractPluggableIndex
								.getInstance(indexType, arguments);
				rci.split(rciDaughterRegionA, rciDaughterRegionB, splitRow);

				// To be Done: Need to check the size of the keyset?

				daughterRegionAIndex.colIndex.put(column, rciDaughterRegionA);

				daughterRegionBIndex.colIndex.put(column, rciDaughterRegionB);

			}

		} finally {
			rwLock.writeLock().unlock();
		}
	}

	public List<ProtoResult> filterRowsFromCriteria(
			List<Criterion<?>> criteriaOnIndexColumns,
			List<Criterion<?>> criteriaOnNonIndexedColumns,
			IndexedColumnQuery query, HRegion region) throws IOException,
			ClassNotFoundException {
		rwLock.readLock().lock();

		try {
			Set<byte[]> result = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
			boolean firstRows = true;
			boolean mustPassAll = query.isMustPassAllCriteria();
			/*
			 * Filter from index
			 */
			for (Criterion<?> criterion : criteriaOnIndexColumns) {
				Column column = criterion.getCompareColumn();

				AbstractPluggableIndex rci = colIndex.get(column.toString());

				Set<byte[]> partialRows = rci.filterRowsFromCriteria(criterion);

				if (partialRows != null && !partialRows.isEmpty()) {
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
			FilterList filterList = buildFilterListFromCriteria(
					criteriaOnNonIndexedColumns, query.isMustPassAllCriteria());
			List<ProtoResult> filteredRows = prefilteredLocalMultiGet(result,
					filterList, query.getColumnList(), region);

			return filteredRows;
		} finally {
			rwLock.readLock().unlock();
		}

	}

	private static FilterList buildFilterListFromCriteria(
			List<Criterion<?>> criteriaOnNonIndexedColumns,
			boolean mustPassAllCriteria) {
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

  private static final byte[] EMPTY = new byte[0];

	private List<ProtoResult> prefilteredLocalMultiGet(Set<byte[]> rows,
			FilterList filterList, List<Column> columnList, HRegion region)
	throws IOException {

    List<ProtoResult> resultList = new ArrayList<ProtoResult>(rows.size());

		for (byte[] row : rows) {
      // Check for empty projection
      if (columnList.isEmpty()) {
        Cell c = new KeyValue(row, EMPTY, EMPTY, EMPTY);
        Result r = Result.create(Arrays.asList(c));
        resultList.add(Util.toResult(r));
      }

      else {
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
		}

		return resultList;
	}

	public void setSplitting(boolean b) {
		splitting = true;
	}


	// Filter rows from index query
	public List<ProtoResult> filterRowsFromCriteria(String idxColKey,
			List<Criterion<?>> selectCriteria, List<Column> projectColumns,
			HRegion region)
	throws IOException {

		try {
			rwLock.readLock().lock();

			Criterion<?> selectCriterion = selectCriteria.get(0);

			if (selectCriteria.size() > 1) {
				// Use multi-column index
				byte[] concatValues = null;
				for (Criterion<?> criterion : selectCriteria) {
					concatValues = Util.concatByteArray(concatValues,
							(byte[]) criterion.getComparisonValue());
				}
				// Create concatenated criterion
				selectCriterion = new ByteArrayCriterion(concatValues);
			}

			AbstractPluggableIndex rci = colIndex.get(idxColKey);

			Set<byte[]> rowKeys = rci.filterRowsFromCriteria(selectCriterion);

			List<ProtoResult> resultList;
			if (rowKeys != null && !rowKeys.isEmpty()) {
				FilterList filterList = null;
				if (selectCriteria.size() > 1) {
					// Multi-column case:
					// Apply a MUST_PASS_ALL filter for the given criteria to
					// ensure that results that have the given concatenated
					// value for the given criteria are actually the same as
					// the value being queried.
					filterList = buildFilterListFromCriteria(selectCriteria,
							true);
				}
				resultList = prefilteredLocalMultiGet(rowKeys, filterList,
						projectColumns, region);
			} else {
				// no matching rows found
				resultList = new ArrayList<>(0);
			}

			return resultList;

		} finally {
			rwLock.readLock().unlock();
		}

	}
	
	
	 // just for test purpose
	public static void printSingleMappedIndex(HashMap<String, Set<IndexedColumn>> index) {
		for(String key: index.keySet()){
			System.out.println("Key: " + key);
			Set<IndexedColumn> cls = index.get(key);
			java.util.Iterator<IndexedColumn> iter = cls.iterator();
			while(iter.hasNext()) {
				IndexedColumn indexedCol = iter.next();
				//if(indexedCol.getMultiColumn()) {
				//	System.out.println(Bytes.toString(Util.concatColumns(indexedCol.getColumnList())));
				//} else {
				//	System.out.println(Bytes.toString(Util.concatByteArray(indexedCol.getColumnFamily(), indexedCol.getQualifier())));
				//}
				System.out.println(indexedCol.toString());
			}
			
		}
	}
	
	public static void main(String [] args) {
		RegionIndex regionIndex = new RegionIndex(100);
		// singleMappedPut
		// removeFromSingleMappedIndex
		System.out.println("Test for  (singleMappedPut | removeFromSingleMappedIndex) ");
		
		System.out.println("Initializing......");

		// Multi-column
		List<Column> list1 = new ArrayList<Column>();
		list1.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("a")));
		list1.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("b")));
		List<Column> list2 = new ArrayList<Column>();
		list2.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("b")));
		list2.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("c")));
		List<Column> list3 = new ArrayList<Column>();
		list3.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("c")));
		list3.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("d")));
		List<Column> list4 = new ArrayList<Column>();
		list4.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("a")));
		list4.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("d")));

		// Single-column
		regionIndex.singleMappedPut(Arrays.asList(new Column("cf", "a")));
		regionIndex.singleMappedPut(Arrays.asList(new Column("cf", "b")));
		regionIndex.singleMappedPut(Arrays.asList(new Column("cf", "c")));
		regionIndex.singleMappedPut(Arrays.asList(new Column("cf", "d")));

		regionIndex.singleMappedPut(list1);
		regionIndex.singleMappedPut(list2);
		regionIndex.singleMappedPut(list3);
		regionIndex.singleMappedPut(list4);
		
		printSingleMappedIndex(regionIndex.getSingleMappedIndex());

		List<Column> removelist1 = new ArrayList<Column>();
		removelist1.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("a")));
		removelist1.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("b")));
		List<Column> removelist2 = new ArrayList<Column>();
		removelist2.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("b")));
		removelist2.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("c")));
		List<Column> removelist3 = new ArrayList<Column>();
		removelist3.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("c")));
		removelist3.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("d")));
		List<Column> removelist4 = new ArrayList<Column>();
		removelist4.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("a")));
		removelist4.add(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("d")));
		
		System.out.println("Removing cf:a....");
		regionIndex.removeFromSingleMappedIndex(Arrays.asList(new Column("cf", "a")));
		printSingleMappedIndex(regionIndex.getSingleMappedIndex());
		
		System.out.println("Removing cf:a,cf:b....");
		regionIndex.removeFromSingleMappedIndex(removelist1);
		printSingleMappedIndex(regionIndex.getSingleMappedIndex());
		
		System.out.println("Removing cf:b,cf:c....");
		regionIndex.removeFromSingleMappedIndex(removelist2);
		printSingleMappedIndex(regionIndex.getSingleMappedIndex());
		
		System.out.println("Removing cf:a,cf:d....");
		regionIndex.removeFromSingleMappedIndex(removelist4);
		printSingleMappedIndex(regionIndex.getSingleMappedIndex());
		
		System.out.println("Removing cf:c,cf:d....");
		regionIndex.removeFromSingleMappedIndex(removelist3);
		printSingleMappedIndex(regionIndex.getSingleMappedIndex());
	}
}
