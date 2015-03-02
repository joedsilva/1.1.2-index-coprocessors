package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2;

import ca.mcgill.distsys.hbase96.indexcommons.ByteUtil;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Range;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.AbstractPluggableIndex;

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

	// private TreeMultiset<HybridRowIndex> sortedTree;
	public IMBLTree tree;
	// private HashMap<ByteArrayWrapper, ArrayList<HybridRowIndex>> rowIndexMap;
	public HashMap<DeepCopyObject, ArrayList<DeepCopyObject>> map;
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
		// create tree with maximum node size 6
		// using default comparator -> rewrite the compare method of
		// DeepCopyObject
		tree = new IMBLTree(6, IMBLTree.COMPARABLE_COMPARATOR);
		map = new HashMap<DeepCopyObject, ArrayList<DeepCopyObject>>();
		rwLock = new ReentrantReadWriteLock(true);
		this.columnFamily = cf;
		this.qualifier = qualifier;
	}

	@Override
	public void add(byte[] key, byte[] value) {
		rwLock.writeLock().lock();
		ByteArrayNodeKey hashedValue = new ByteArrayNodeKey(key);
		ArrayList<DeepCopyObject> list = map.get(hashedValue);
		if (list == null) {
			list = new ArrayList<DeepCopyObject>();
			ByteArrayNodeValue rowIndex = new ByteArrayNodeValue(key);
			rowIndex.add(value);
			list.add(rowIndex);
			map.put(hashedValue, list);
			rwLock.writeLock().unlock();

			// Add rowindex to the tree as well
			// Allow multi-threading on the tree
			tree.put(hashedValue, rowIndex);
			return;
		} else {
			for (DeepCopyObject singleRowIndex : list) {
				if (Arrays.equals(key,
						((ByteArrayNodeValue) singleRowIndex).getRowKey())) {
					((ByteArrayNodeValue) singleRowIndex).add(value);
					// Because it's reference type, don't have to manipulate
					// treeSet anymore
					rwLock.writeLock().unlock();
					return;
				}
			}
			// if the arraylist doesn't contain HybridRowIndex with rowKey is
			// equal to key
			ByteArrayNodeValue rowIndex = new ByteArrayNodeValue(key);
			list.add(rowIndex);
			rwLock.writeLock().unlock();
			// allow multi-threading on the tree
			tree.put(hashedValue, rowIndex);
			return;
		}
	}

	// private void internalAdd(byte[] key, byte[] value) {
	// // int hashedValue = Hashing.murmur3_32().hashBytes(key).asInt();
	// ByteArrayWrapper hashedValue = new ByteArrayWrapper(key);
	// ArrayList<HybridRowIndex> list = rowIndexMap.get(hashedValue);
	// if (list == null) {
	// list = new ArrayList<HybridRowIndex>();
	// HybridRowIndex rowIndex = new HybridRowIndex(key);
	// rowIndex.add(value);
	// list.add(rowIndex);
	// rowIndexMap.put(hashedValue, list);
	// // Add rowindex to the tree as well
	// sortedTree.add(rowIndex);
	// } else {
	// for (HybridRowIndex singleRowIndex : list) {
	// if (Arrays.equals(key, singleRowIndex.getRowKey())) {
	// singleRowIndex.add(value);
	// // Because it's reference type, don't have to manipulate
	// // treeSet anymore
	// return;
	// }
	// }
	// // if the arraylist doesn't contain HybridRowIndex with rowKey is
	// // equal to key
	// HybridRowIndex rowIndex = new HybridRowIndex(key);
	// list.add(rowIndex);
	// sortedTree.add(rowIndex);
	// return;
	// }
	// }

	@Override
	public byte[][] get(byte[] key) {
		rwLock.readLock().lock();

		Set<byte[]> rowKeys = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		ByteArrayNodeKey hashedValue = new ByteArrayNodeKey(key);
		ArrayList<DeepCopyObject> list = map.get(hashedValue);

		if (list != null) {
			for (DeepCopyObject singleRowIndex : list) {
				if (Arrays.equals(key,
						((ByteArrayNodeValue) singleRowIndex).getRowKey())) {
					rowKeys.addAll(((ByteArrayNodeValue) singleRowIndex)
							.getPKRefs());
					rwLock.readLock().unlock();
					return (rowKeys.toArray(new byte[rowKeys.size()][]));
				}
			}
		}
		rwLock.readLock().unlock();
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
					map.clear();

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
									this.add(values.get(0).getValue(),
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
		ByteArrayNodeKey hashedValue = new ByteArrayNodeKey(key);
		ArrayList<DeepCopyObject> list = map.get(hashedValue);
		if (list != null) {
			for (DeepCopyObject singleRowIndex : list) {
				if (Arrays.equals(key,
						((ByteArrayNodeValue) singleRowIndex).getRowKey())) {
					// wait until the insertion of the tree finished
					while (((ByteArrayNodeValue) singleRowIndex)
							.getUpdateStatus() == 1) {
						;
					}
					((ByteArrayNodeValue) singleRowIndex).remove(value);
					// If the HybridRowIndex doesn't contain anything, then we
					// should remove it
					if (((ByteArrayNodeValue) singleRowIndex).getPKRefs()
							.isEmpty()) {
						list.remove(singleRowIndex);
						if (list.isEmpty()) {
							map.remove(hashedValue);
						}
						rwLock.writeLock().unlock();
						tree.remove(hashedValue, singleRowIndex);
						return;
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
			ByteArrayNodeKey hashedValue = new ByteArrayNodeKey((byte[]) value);
			ArrayList<DeepCopyObject> list = map.get(hashedValue);
			if (list != null) {
				for (DeepCopyObject singleRowIndex : list) {
					if (Arrays.equals((byte[]) value,
							((ByteArrayNodeValue) singleRowIndex).getRowKey())) {
						rowKeys.addAll(((ByteArrayNodeValue) singleRowIndex)
								.getPKRefs());
						rwLock.readLock().unlock();
						return rowKeys;
					}
				}
			}
			rwLock.readLock().unlock();
			return null;
		case GREATER:
			ByteArrayNodeKey greaterRowIndex = new ByteArrayNodeKey(
					(byte[]) value);
			List<DeepCopyObject> greaterSet = tree.rangeSearch(greaterRowIndex,
					false, null, false);
			for (DeepCopyObject singleRowIndex : greaterSet) {
				rowKeys.addAll(((ByteArrayNodeValue) singleRowIndex)
						.getPKRefs());
			}
			rwLock.readLock().unlock();
			return rowKeys;
		case LESS:
			ByteArrayNodeKey lessRowIndex = new ByteArrayNodeKey((byte[]) value);
			List<DeepCopyObject> lessSet = tree.rangeSearch(null, false,
					lessRowIndex, false);
			for (DeepCopyObject singleRowIndex : lessSet) {
				rowKeys.addAll(((ByteArrayNodeValue) singleRowIndex)
						.getPKRefs());
			}
			rwLock.readLock().unlock();
			return rowKeys;
		case GREATER_OR_EQUAL:
			ByteArrayNodeKey greaterOrEqualRowIndex = new ByteArrayNodeKey(
					(byte[]) value);
			List<DeepCopyObject> greaterOrEqualSet = tree.rangeSearch(
					greaterOrEqualRowIndex, true, null, false);
			for (DeepCopyObject singleRowIndex : greaterOrEqualSet) {
				rowKeys.addAll(((ByteArrayNodeValue) singleRowIndex)
						.getPKRefs());
			}
			rwLock.readLock().unlock();
			return rowKeys;
		case LESS_OR_EQUAL:
			ByteArrayNodeKey lessOrEqualRowIndex = new ByteArrayNodeKey(
					(byte[]) value);
			List<DeepCopyObject> lessOrEqualSet = tree.rangeSearch(null, false,
					lessOrEqualRowIndex, true);
			for (DeepCopyObject singleRowIndex : lessOrEqualSet) {
				rowKeys.addAll(((ByteArrayNodeValue) singleRowIndex)
						.getPKRefs());
			}
			rwLock.readLock().unlock();
			return rowKeys;
		case RANGE:
			Range range = criterion.getRange();
			ByteArrayNodeKey lowerBound = new ByteArrayNodeKey(
					range.getLowerBound());
			ByteArrayNodeKey higherBound = new ByteArrayNodeKey(
					range.getHigherBound());
			List<DeepCopyObject> rangeSet = tree.rangeSearch(lowerBound, true,
					higherBound, true);
			for (DeepCopyObject singleRowIndex : rangeSet) {
				rowKeys.addAll(((ByteArrayNodeValue) singleRowIndex)
						.getPKRefs());
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
	// System.out.print(" " + Bytes.toString(value) + ",");lk
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
		ArrayList<DeepCopyObject> list;
		ByteArrayNodeValue tempValue;
		for (DeepCopyObject value : map.keySet()) {
			list = map.get(value);
			for(int i = 0; i < list.size(); i++) {
				tempValue = (ByteArrayNodeValue) list.get(i);
				
				byte[][] sortedPKRefArray = tempValue.getPKRefsAsArray();
				int splitPoint = Arrays.binarySearch(sortedPKRefArray, splitRow,
						ByteUtil.BYTES_COMPARATOR);
				for (int j = 0; j < sortedPKRefArray.length; j++) {
					if ((splitPoint >= 0 && j < splitPoint)
							|| (splitPoint < 0 && j < Math.abs(splitPoint + 1))) {
						daughterRegionA.add(tempValue.getRowKey(), sortedPKRefArray[j]);
					} else {
						daughterRegionB.add(tempValue.getRowKey(), sortedPKRefArray[j]);
					}
				}
			}
			
		}
		
//		Iterator<HybridRowIndex> keyIterator = sortedTree.iterator();
//		<DeepCopyObject> keySet= map.keySet();
//		List<DeepCopyObject> list;
//		for(int i = 0 ; i < keySet.size(); i++) {
//			list = keySet.
//		}
//		while (current != null) {
//
//			HybridRowIndex key = keyIterator.next();
//			byte[][] sortedPKRefArray = key.getPKRefsAsArray();
//			int splitPoint = Arrays.binarySearch(sortedPKRefArray, splitRow,
//					ByteUtil.BYTES_COMPARATOR);
//			for (int i = 0; i < sortedPKRefArray.length; i++) {
//				if ((splitPoint >= 0 && i < splitPoint)
//						|| (splitPoint < 0 && i < Math.abs(splitPoint + 1))) {
//					daughterRegionA.add(key.getRowKey(), sortedPKRefArray[i]);
//				} else {
//					daughterRegionB.add(key.getRowKey(), sortedPKRefArray[i]);
//				}
//			}
//		}

		rwLock.writeLock().unlock();
	}

	public static void main(String[] args) {
		System.out.println("This is for test");
		HybridIndex hybrid = new HybridIndex(new Object[] {
				Bytes.toBytes("cf"), Bytes.toBytes("qualifier") });
		for (int i = 0; i < 100; i++) {
			hybrid.add(Bytes.toBytes(i), Bytes.toBytes("I am #" + i));
		}
		// get result
		byte[][] result;
		for (int i = 0; i < 100; i++) {
			System.out.println("");
			System.out.print("key: " + i + " values: {");
			result = hybrid.get(Bytes.toBytes(i));
			for (int j = 0; j < result.length; j++) {
				System.out.print(Bytes.toString(result[j]) + ", ");
			}
			System.out.print("}");
		}

		// readd test
		hybrid.add(Bytes.toBytes(98), Bytes.toBytes("I am #" + 100));
		hybrid.add(Bytes.toBytes(99), Bytes.toBytes("I am #" + 101));
		for (int i = 0; i < 100; i++) {
			System.out.println("");
			System.out.print("key: " + i + " values: {");
			result = hybrid.get(Bytes.toBytes(i));
			for (int j = 0; j < result.length; j++) {
				System.out.print(Bytes.toString(result[j]) + ", ");
			}
			System.out.print("}");
		}

		// remove test 1
		hybrid.remove(Bytes.toBytes(98), Bytes.toBytes("I am #" + 98));
		hybrid.remove(Bytes.toBytes(99), Bytes.toBytes("I am #" + 99));
		for (int i = 0; i < 100; i++) {
			System.out.println("");
			System.out.print("key: " + i + " values: {");
			result = hybrid.get(Bytes.toBytes(i));
			for (int j = 0; j < result.length; j++) {
				System.out.print(Bytes.toString(result[j]) + ", ");
			}
			System.out.print("}");
		}

		List<DeepCopyObject> list;
		// hybrid.remove(Bytes.toBytes(98), Bytes.toBytes("I am #" + 100));
		// hybrid.remove(Bytes.toBytes(99), Bytes.toBytes("I am #" + 101));
		// range test 1
		list = hybrid.tree.rangeSearch(new ByteArrayNodeKey(Bytes.toBytes(91)),
				true, new ByteArrayNodeKey(Bytes.toBytes(100)), true);
		System.out.println("\nRangeSearch 91 to 100: " + list.size());
		for (int i = 0; i < list.size(); i++) {
			System.out.println();
			System.out.print("key: "
					+ Bytes.toInt(((ByteArrayNodeValue) list.get(i))
							.getRowKey()) + " vals: {");
			Set<byte[]> refs = ((ByteArrayNodeValue) list.get(i)).getPKRefs();
			Iterator<byte[]> iter = refs.iterator();
			while (iter.hasNext()) {
				System.out.print(Bytes.toString(iter.next()) + ", ");
			}
			System.out.print("}");
		}

		// range test 2
		list = hybrid.tree.rangeSearch(new ByteArrayNodeKey(Bytes.toBytes(91)),
				true, new ByteArrayNodeKey(Bytes.toBytes(100)), false);
		System.out.println("\nRangeSearch 91 to 100: " + list.size());
		for (int i = 0; i < list.size(); i++) {
			System.out.println();
			System.out.print("key: "
					+ Bytes.toInt(((ByteArrayNodeValue) list.get(i))
							.getRowKey()) + " vals: {");
			Set<byte[]> refs = ((ByteArrayNodeValue) list.get(i)).getPKRefs();
			Iterator<byte[]> iter = refs.iterator();
			while (iter.hasNext()) {
				System.out.print(Bytes.toString(iter.next()) + ", ");
			}
			System.out.print("}");
		}

		// range test 3
		list = hybrid.tree.rangeSearch(new ByteArrayNodeKey(Bytes.toBytes(91)),
				false, new ByteArrayNodeKey(Bytes.toBytes(100)), true);
		System.out.println("\nRangeSearch 91 to 100: " + list.size());
		for (int i = 0; i < list.size(); i++) {
			System.out.println();
			System.out.print("key: "
					+ Bytes.toInt(((ByteArrayNodeValue) list.get(i))
							.getRowKey()) + " vals: {");
			Set<byte[]> refs = ((ByteArrayNodeValue) list.get(i)).getPKRefs();
			Iterator<byte[]> iter = refs.iterator();
			while (iter.hasNext()) {
				System.out.print(Bytes.toString(iter.next()) + ", ");
			}
			System.out.print("}");
		}

		// range test 4
		list = hybrid.tree.rangeSearch(new ByteArrayNodeKey(Bytes.toBytes(91)),
				false, new ByteArrayNodeKey(Bytes.toBytes(100)), false);
		System.out.println("\nRangeSearch 91 to 100: " + list.size());
		for (int i = 0; i < list.size(); i++) {
			System.out.println();
			System.out.print("key: "
					+ Bytes.toInt(((ByteArrayNodeValue) list.get(i))
							.getRowKey()) + " vals: {");
			Set<byte[]> refs = ((ByteArrayNodeValue) list.get(i)).getPKRefs();
			Iterator<byte[]> iter = refs.iterator();
			while (iter.hasNext()) {
				System.out.print(Bytes.toString(iter.next()) + ", ");
			}
			System.out.print("}");
		}
	}
}
