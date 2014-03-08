package ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex.hybridBased;

import java.io.IOException;
import java.io.InterruptedIOException;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.BoundType;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;
import com.google.common.hash.Hashing;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Range;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex.AbstractPluggableIndex;

public class HybridIndex extends AbstractPluggableIndex implements Serializable {

	private transient static Log LOG;

	private TreeMultiset<HybridRowIndex> sortedTree;
	private HashMap<Integer, ArrayList<HybridRowIndex>> rowIndexMap;
	private transient ReadWriteLock rwLock;
	private byte[] columnFamily;
	private byte[] qualifier;

	private void readObject(ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		rwLock = new ReentrantReadWriteLock(true);
		LOG = LogFactory.getLog(HybridIndex.class);
	}

	public HybridIndex(byte[] cf, byte[] qualifier) {
		LOG = LogFactory.getLog(HybridIndex.class);
		sortedTree = TreeMultiset.create();
		rowIndexMap = new HashMap<Integer, ArrayList<HybridRowIndex>>(15000);
		rwLock = new ReentrantReadWriteLock(true);
		this.columnFamily = cf;
		this.qualifier = qualifier;
	}

	@Override
	public void add(byte[] key, byte[] value) {
		rwLock.writeLock().lock();
		internalAdd(key, Arrays.copyOf(value, value.length));
		rwLock.writeLock().unlock();
	}

	private void internalAdd(byte[] key, byte[] value) {
		int hashedValue = Hashing.murmur3_32().hashBytes(key).asInt();
		ArrayList<HybridRowIndex> list = rowIndexMap.get(hashedValue);
		if (list == null) {
			list = new ArrayList<HybridRowIndex>();
			HybridRowIndex rowIndex = new HybridRowIndex(key);
			rowIndex.add(value);
			list.add(rowIndex);
			rowIndexMap.put(hashedValue, list);
			sortedTree.add(rowIndex);
		} else {
			for (HybridRowIndex singleRowIndex : list) {
				if (Arrays.equals(key, singleRowIndex.getRowKey())) {
					singleRowIndex.add(value);
					// Because it's reference type, don't have to manipulate
					// treeSet anymore
					return;
				}
			}
			// if the arraylist doesn't contain HybridRowIndex with rowKey is
			// equal to key
			HybridRowIndex rowIndex = new HybridRowIndex(key);
			list.add(rowIndex);
			sortedTree.add(rowIndex);
			return;
		}
	}

	@Override
	public byte[][] get(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	public void fullBuild(HRegion region) {
		rwLock.writeLock().lock();

		try {
			Scan scan = new Scan();
			scan.addColumn(columnFamily, qualifier);
			scan.setCacheBlocks(false); // don't want to fill the cache
										// uselessly and create churn
			RegionScanner scanner = region.getScanner(scan);
			MultiVersionConsistencyControl.setThreadReadPoint(scanner
					.getMvccReadPoint());
			region.startRegionOperation();
			try {
				synchronized (scanner) {

					// Modified by Cong
					// List<KeyValue> values = new ArrayList<KeyValue>();
					List<Cell> values = new ArrayList<Cell>();
					rowIndexMap.clear();

					boolean more;
					do {

						more = scanner.nextRaw(values);
						if (!values.isEmpty() && values.get(0) != null
								&& values.get(0).getValue() != null) {
							if (values.get(0).getRow() == null) {
								// LOG.error("NULL ROW for VALUE [" +
								// values.get(0).getValue() +
								// "] in column [" + new
								// String(columnFamily) + ":"
								// + new String(qualifier) + "]");
							} else {
								byte[] rowid = values.get(0).getRow();
								try {
									internalAdd(values.get(0).getValue(),
											Arrays.copyOf(rowid, rowid.length));
								} catch (NullPointerException NPEe) {
									// LOG.error("NPE for VALUE [" + new
									// String(values.get(0).getValue()) +
									// "] ROW ["
									// + new String(rowid) + "] in column ["
									// + new String(columnFamily) + ":"
									// + new String(qualifier) + "]", NPEe);
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
		} catch (NotServingRegionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RegionTooBusyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			rwLock.writeLock().unlock();
		}

	}

	@Override
	public void removeValueFromIdx(byte[] key, byte[] value) {
		int hashedValue = Hashing.murmur3_32().hashBytes(key).asInt();
		ArrayList<HybridRowIndex> list = rowIndexMap.get(hashedValue);
		if (list != null) {
			for (HybridRowIndex singleRowIndex : list) {
				if (Arrays.equals(key, singleRowIndex.getRowKey())) {
					singleRowIndex.remove(value);
					// If the HybridRowIndex doesn't contain anything, then we
					// should remove it
					if (singleRowIndex.getPKRefs().isEmpty()) {
						list.remove(singleRowIndex);
						sortedTree.remove(singleRowIndex);
					}
					if (list.isEmpty()) {
						rowIndexMap.remove(hashedValue);
					}
				}
				return;
			}
		}

	}

	@Override
	public Set<byte[]> filterRowsFromCriteria(Criterion<?> criterion) {

		Set<byte[]> rowKeys = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		Object key = criterion.getComparisonValue();
		LOG.info("filterRowsFromCriteria..." + criterion.getComparisonType());
		
		switch (criterion.getComparisonType()) {
		case EQUAL:
			int hashedValue = Hashing.murmur3_32().hashBytes((byte[]) key)
					.asInt();
			ArrayList<HybridRowIndex> list = rowIndexMap.get(hashedValue);
			if (list != null) {
				for (HybridRowIndex singleRowIndex : list) {
					if (Arrays.equals((byte[]) key, singleRowIndex.getRowKey())) {
						rowKeys.addAll(singleRowIndex.getPKRefs());
						return rowKeys;
					}
				}
			}
			return null;
		case GREATER:
			HybridRowIndex greaterRowIndex = new HybridRowIndex((byte[]) key);
			SortedMultiset<HybridRowIndex> greaterSet = sortedTree
					.tailMultiset(greaterRowIndex, BoundType.OPEN);
			for (HybridRowIndex singleRowIndex : greaterSet) {
				rowKeys.addAll(singleRowIndex.getPKRefs());
			}
			return rowKeys;
		case LESS:
			HybridRowIndex lessRowIndex = new HybridRowIndex((byte[]) key);
			SortedMultiset<HybridRowIndex> lessSet = sortedTree.headMultiset(
					lessRowIndex, BoundType.OPEN);
			for (HybridRowIndex singleRowIndex : lessSet) {
				rowKeys.addAll(singleRowIndex.getPKRefs());
			}
			return rowKeys;
		case RANGE:
			Range range = criterion.getRange();
			HybridRowIndex lowerBound = new HybridRowIndex(
					range.getLowerBound());
			HybridRowIndex higherBound = new HybridRowIndex(
					range.getHigherBound());
			SortedMultiset<HybridRowIndex> rangeSet = sortedTree
					.subMultiset(lowerBound, BoundType.CLOSED, higherBound,
							BoundType.CLOSED);
			for (HybridRowIndex singleRowIndex : rangeSet) {
				rowKeys.addAll(singleRowIndex.getPKRefs());
			}
			return rowKeys;
		default:
			return null;
		}
	}

	public String toString() {
		for (int key : rowIndexMap.keySet()) {
			System.out.println("HashMap key: " + key);
			ArrayList<HybridRowIndex> list = rowIndexMap.get(key);
			for (HybridRowIndex singleRowIndex : list) {
				System.out.println("RowIndexKey: "
						+ new String(singleRowIndex.getRowKey()));
				System.out.print("RowIndexValues: ");
				for (byte[] value : singleRowIndex.getPKRefs()) {
					System.out.print(" " + new String(value) + ",");
				}
				System.out.println("");
			}
			System.out.println("");

		}
		return "";
	}

	public static void main(String[] args) {
		System.out.println("Hello World");
		TreeMultiset<HybridRowIndex> set = TreeMultiset.create();

	}

}
