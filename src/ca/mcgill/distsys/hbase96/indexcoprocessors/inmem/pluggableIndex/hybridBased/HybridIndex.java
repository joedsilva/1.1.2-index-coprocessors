package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased;

import ca.mcgill.distsys.hbase96.indexcommons.ByteUtil;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Range;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.AbstractPluggableIndex;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.commons.ByteArrayWrapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HybridIndex extends AbstractPluggableIndex
		implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 2201469150576800226L;

	private transient static Log LOG = LogFactory.getLog(HybridIndex.class);

	private TreeMultiset<HybridRowIndex> sortedTree;
	private HashMap<ByteArrayWrapper, ArrayList<HybridRowIndex>> rowIndexMap;
	private transient ReadWriteLock rwLock;
	private byte[] columnFamily;
	private byte[] qualifier;

	private void readObject(ObjectInputStream in) throws IOException,
	ClassNotFoundException {
		in.defaultReadObject();
		rwLock = new ReentrantReadWriteLock(true);
	}

	public HybridIndex(Object[] arguments) {
		byte[] cf = (byte[]) arguments[0];
		byte[] qualifier = (byte[]) arguments[1];
		sortedTree = TreeMultiset.create();
		rowIndexMap = new HashMap<ByteArrayWrapper, ArrayList<HybridRowIndex>>();
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
		// int hashedValue = Hashing.murmur3_32().hashBytes(key).asInt();
		ByteArrayWrapper hashedValue = new ByteArrayWrapper(key);
		ArrayList<HybridRowIndex> list = rowIndexMap.get(hashedValue);
		if (list == null) {
			list = new ArrayList<HybridRowIndex>();
			HybridRowIndex rowIndex = new HybridRowIndex(key);
			rowIndex.add(value);
			list.add(rowIndex);
			rowIndexMap.put(hashedValue, list);
			// Add rowindex to the tree as well
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
								// + Bytes.toString(qualifier) + "]");
							} else {
								byte[] rowid = values.get(0).getRow();
								try {
									internalAdd(values.get(0).getValue(),
											Arrays.copyOf(rowid, rowid.length));
								} catch (NullPointerException NPEe) {
									// LOG.error("NPE for VALUE [" + new
									// String(values.get(0).getValue()) +
									// "] ROW ["
									// + Bytes.toString(rowid) + "] in column ["
									// + Bytes.toString(columnFamily) + ":"
									// + Bytes.toString(qualifier) + "]", NPEe);
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
	public void remove(byte[] key, byte[] value) {
		rwLock.writeLock().lock();
		// int hashedValue = Hashing.murmur3_32().hashBytes(key).asInt();
		ByteArrayWrapper hashedValue = new ByteArrayWrapper(key);
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
					rwLock.writeLock().unlock();
					return;
				}
			}
			// No value key found in the list
		}
		// No corresponding list found in the hashma
		rwLock.writeLock().unlock();

	}

	@Override
	public Set<byte[]> filterRowsFromCriteria(Criterion<?> criterion) {
		rwLock.readLock().lock();

		Set<byte[]> rowKeys = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		Object value = criterion.getComparisonValue();

		switch (criterion.getComparisonType()) {
		case EQUAL:
			// int hashedValue = Hashing.murmur3_32().hashBytes((byte[]) key)
			// .asInt();
			ByteArrayWrapper hashedValue = new ByteArrayWrapper((byte[]) value);
			ArrayList<HybridRowIndex> list = rowIndexMap.get(hashedValue);
			if (list != null) {
				for (HybridRowIndex singleRowIndex : list) {
					if (Arrays.equals((byte[]) value, singleRowIndex.getRowKey())) {
						rowKeys.addAll(singleRowIndex.getPKRefs());
						rwLock.readLock().unlock();
						return rowKeys;
					}
				}
			}
			rwLock.readLock().unlock();
			return null;
		case GREATER:
			HybridRowIndex greaterRowIndex = new HybridRowIndex((byte[]) value);
			SortedMultiset<HybridRowIndex> greaterSet = sortedTree
					.tailMultiset(greaterRowIndex, BoundType.OPEN);
			for (HybridRowIndex singleRowIndex : greaterSet) {
				rowKeys.addAll(singleRowIndex.getPKRefs());
			}
			rwLock.readLock().unlock();
			return rowKeys;
		case LESS:
			HybridRowIndex lessRowIndex = new HybridRowIndex((byte[]) value);
			SortedMultiset<HybridRowIndex> lessSet = sortedTree
					.headMultiset(lessRowIndex, BoundType.OPEN);
			for (HybridRowIndex singleRowIndex : lessSet) {
				rowKeys.addAll(singleRowIndex.getPKRefs());
			}
			rwLock.readLock().unlock();
			return rowKeys;
		case GREATER_OR_EQUAL:
			HybridRowIndex greaterOrEqualRowIndex =
					new HybridRowIndex((byte[]) value);
			SortedMultiset<HybridRowIndex> greaterOrEqualSet = sortedTree
					.tailMultiset(greaterOrEqualRowIndex, BoundType.CLOSED);
			for (HybridRowIndex singleRowIndex : greaterOrEqualSet) {
				rowKeys.addAll(singleRowIndex.getPKRefs());
			}
			rwLock.readLock().unlock();
			return rowKeys;
		case LESS_OR_EQUAL:
			HybridRowIndex lessOrEqualRowIndex =
					new HybridRowIndex((byte[]) value);
			SortedMultiset<HybridRowIndex> lessOrEqualSet = sortedTree
					.headMultiset(lessOrEqualRowIndex, BoundType.CLOSED);
			for (HybridRowIndex singleRowIndex : lessOrEqualSet) {
				rowKeys.addAll(singleRowIndex.getPKRefs());
			}
			rwLock.readLock().unlock();
			return rowKeys;
		case RANGE:
			Range range = criterion.getRange();
			HybridRowIndex lowerBound = new HybridRowIndex(range.getLowerBound());
			HybridRowIndex higherBound = new HybridRowIndex(range.getHigherBound());
			SortedMultiset<HybridRowIndex> rangeSet = sortedTree.subMultiset(
					lowerBound, BoundType.CLOSED, higherBound, BoundType.CLOSED);
			for (HybridRowIndex singleRowIndex : rangeSet) {
				rowKeys.addAll(singleRowIndex.getPKRefs());
			}
			rwLock.readLock().unlock();
			return rowKeys;
		default:
			rwLock.readLock().unlock();
			return null;
		}
	}

	// public String toString() {
	// for (int key : rowIndexMap.keySet()) {
	// System.out.println("HashMap key: " + key);
	// ArrayList<HybridRowIndex> list = rowIndexMap.get(key);
	// for (HybridRowIndex singleRowIndex : list) {
	// System.out.println("RowIndexKey: "
	// + Bytes.toString(singleRowIndex.getRowKey()));
	// System.out.print("RowIndexValues: ");
	// for (byte[] value : singleRowIndex.getPKRefs()) {
	// System.out.print(" " + Bytes.toString(value) + ",");
	// }
	// System.out.println("");
	// }
	// System.out.println("");
	//
	// }
	// return "";
	// }

	@Override
	public void split(AbstractPluggableIndex daughterRegionA,
			AbstractPluggableIndex daughterRegionB, byte[] splitRow) {

		rwLock.writeLock().lock();

		Iterator<HybridRowIndex> keyIterator = sortedTree.iterator();
		while (keyIterator.hasNext()) {

			HybridRowIndex key = keyIterator.next();
			byte[][] sortedPKRefArray = key.getPKRefsAsArray();
			int splitPoint = Arrays.binarySearch(sortedPKRefArray, splitRow,
					ByteUtil.BYTES_COMPARATOR);
			for (int i = 0; i < sortedPKRefArray.length; i++) {
				if ((splitPoint >= 0 && i < splitPoint)
						|| (splitPoint < 0 && i < Math.abs(splitPoint + 1))) {
					daughterRegionA.add(key.getRowKey(), sortedPKRefArray[i]);
				} else {
					daughterRegionB.add(key.getRowKey(), sortedPKRefArray[i]);
				}
			}
		}

		rwLock.writeLock().unlock();
	}
}
