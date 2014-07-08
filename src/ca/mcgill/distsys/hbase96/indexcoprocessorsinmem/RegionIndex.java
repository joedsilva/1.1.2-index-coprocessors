package ca.mcgill.distsys.hbase96.indexcoprocessorsinmem;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.Util;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.ByteArrayCriterion;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.IndexedColumnQuery;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion.CompareType;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex.AbstractPluggableIndex;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.ProtoResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RegionIndex implements Serializable {
	private static final Log LOG = LogFactory.getLog(RegionIndex.class);
	private static final long serialVersionUID = 2883387553546148042L;
	// Modified by COng
	private HashMap<String, AbstractPluggableIndex> colIndex;

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

	public void add(String key, HRegion region, String indexType,
			Object[] arguments)
			throws IOException, ClassNotFoundException, NoSuchMethodException {
		rwLock.writeLock().lock();

		try {
			// Modified by Cong
			AbstractPluggableIndex newColIdx = AbstractPluggableIndex
					.getInstance(indexType, arguments);
			colIndex.put(key, newColIdx);
			// Modified by Cong
			if (region != null) {
				newColIdx.fullBuild(region);
			}

		} finally {
			rwLock.writeLock().unlock();
		}
	}

	public void remove(String key) {
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

	private List<ProtoResult> prefilteredLocalMultiGet(Set<byte[]> rows,
			FilterList filterList, List<Column> columnList, HRegion region)
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
}
