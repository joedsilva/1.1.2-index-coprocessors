package ca.mcgill.distsys.hbase.indexcoprocessorsinmem.pluggableIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.BoundType;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;
import com.google.common.hash.Hashing;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Range;

public class HybridIndex extends AbstractPluggableIndex {


	private TreeMultiset<HybridRowIndex> sortedTree;
	private HashMap<Integer, ArrayList<HybridRowIndex>> rowIndexMap;
	private transient ReadWriteLock rwLock;
	private byte[] columnFamily;
	private byte[] qualifier;
	private String tableName;

	public HybridIndex(byte[] cf, byte[] qualifier, String tableName) {
		sortedTree = TreeMultiset.create();
		rowIndexMap = new HashMap<Integer, ArrayList<HybridRowIndex>>(15000);
		rwLock = new ReentrantReadWriteLock(true);
		this.columnFamily = cf;
		this.qualifier = qualifier;
		this.tableName = tableName;
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
					// Because it's reference type, don't have to manipulate treeSet anymore
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

	@Override
	public void fullBuild(HRegion region) {
		// Need to implement the full build when region is not null
		if (region == null) {
			Configuration config = HBaseConfiguration.create();
			HTable table;
			try {
				table = new HTable(config, this.tableName);
				Scan s = new Scan();
				s.addColumn(columnFamily, qualifier);
				ResultScanner scanner = table.getScanner(s);
				List<KeyValue> values = new ArrayList<KeyValue>();
				rowIndexMap.clear();

				for (Result rr = scanner.next(); rr != null; rr = scanner
						.next()) {
					values = rr.list();
					// Should be getRow or getKey??
					byte[] rowid = values.get(0).getRow();
					internalAdd(values.get(0).getValue(),
							Arrays.copyOf(rowid, rowid.length));
				}
				scanner.close();
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

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

		switch (criterion.getComparisonType()) {
		case EQUAL:
			int hashedValue = Hashing.murmur3_32().hashBytes((byte [])key).asInt();
			ArrayList<HybridRowIndex> list = rowIndexMap.get(hashedValue);
			if (list != null) {
				for (HybridRowIndex singleRowIndex : list) {
					if (Arrays.equals((byte [])key, singleRowIndex.getRowKey())) {
						rowKeys.addAll(singleRowIndex.getPKRefs());
						return rowKeys;
					}
				}
			}
			return null;
		case GREATER:
			HybridRowIndex greaterRowIndex = new HybridRowIndex((byte [])key);
			SortedMultiset<HybridRowIndex> greaterSet = sortedTree.tailMultiset(greaterRowIndex, BoundType.OPEN);
			for(HybridRowIndex singleRowIndex: greaterSet){
				rowKeys.addAll(singleRowIndex.getPKRefs());
			}
			return rowKeys;
		case LESS:
			HybridRowIndex lessRowIndex = new HybridRowIndex((byte [])key);
			SortedMultiset<HybridRowIndex> lessSet = sortedTree.headMultiset(lessRowIndex, BoundType.OPEN);
			for(HybridRowIndex singleRowIndex: lessSet){
				rowKeys.addAll(singleRowIndex.getPKRefs());
			}
			return rowKeys;
		case RANGE:
			
			Range range = criterion.getRange();
			HybridRowIndex lowerBound = new HybridRowIndex(range.getLowerBound());
			HybridRowIndex higherBound = new HybridRowIndex(range.getHigherBound());
			SortedMultiset<HybridRowIndex> rangeSet = sortedTree.subMultiset(lowerBound, BoundType.CLOSED, higherBound, BoundType.CLOSED);
			for(HybridRowIndex singleRowIndex: rangeSet){
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
