package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hashtableBased;

import ca.mcgill.distsys.hbase96.indexcommons.ByteUtil;
import ca.mcgill.distsys.hbase96.indexcommons.Util;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Range;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.AbstractPluggableIndex;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.commons.ByteArrayWrapper;


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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// modified by Cong

public class RegionColumnIndex extends AbstractPluggableIndex implements
		Serializable {

	private static final Log LOG = LogFactory.getLog(RegionColumnIndex.class);
	private static final long serialVersionUID = -1641091015504586661L;

	private HashMap<ByteArrayWrapper, RowIndex> rowIndexMap;
	private transient ReadWriteLock rwLock;
	private int maxTreeSize;
	private List<Column> colList;

	private String tableName = "";
	private String test = "";


	private void readObject(ObjectInputStream in) throws IOException,
	ClassNotFoundException {
		in.defaultReadObject();
		rwLock = new ReentrantReadWriteLock(true);
	}

	public RegionColumnIndex(Object[] arguments) {
		assert arguments.length == 2;
		assert arguments[0] instanceof Integer;
		assert arguments[1] instanceof List;
		Integer maxTreeSize = (Integer) arguments[0];
		List<Column> colList = (List) arguments[1];
		rowIndexMap = new HashMap<ByteArrayWrapper, RowIndex>(15000);
		rwLock = new ReentrantReadWriteLock(true);
		this.colList = colList;
		this.maxTreeSize = maxTreeSize;
	}

	public void add(byte[] key, byte[] value) {
		rwLock.writeLock().lock();
		try {
			internalAdd(key, Arrays.copyOf(value, value.length));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			rwLock.writeLock().unlock();
		}
	}

	// Key is the column value we want to add, value is the primary key
	void internalAdd(byte[] key, byte[] value) throws IOException,
			ClassNotFoundException {
		RowIndex rowIndex;
		boolean newPKRefTree = false;
		ByteArrayWrapper keyByteArray = new ByteArrayWrapper(key);
		rowIndex = rowIndexMap.get(keyByteArray);
		if (rowIndex == null) {
			rowIndex = new RowIndex();
			newPKRefTree = true;
		}

		rowIndex.add(value, maxTreeSize);

		if (newPKRefTree) {
			rowIndexMap.put(keyByteArray, rowIndex);
		}
	}

	public byte[][] get(byte[] key) {
		return get(new ByteArrayWrapper(key));
	}

	public byte[][] get(ByteArrayWrapper key) {
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
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			rwLock.readLock().unlock();
		}
		return null;

	}

	public void fullBuild(HRegion region) {
		rwLock.writeLock().lock();

		try {
			Scan scan = new Scan();
			for (Column col : colList) {
				scan.addColumn(col.getFamily(), col.getQualifier());
			}

			scan.setCacheBlocks(false); // don't want to fill the cache
										// uselessly and create churn
			RegionScanner scanner = region.getScanner(scan);
			MultiVersionConsistencyControl.setThreadReadPoint(
					scanner.getMvccReadPoint());
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
						

						if (!values.isEmpty()) {
							// checking for multiIndexing
							if(values.size() != colList.size()){
								continue;
							}
							byte[] rowid = values.get(0).getRow();
							byte[] concatValues = null;
							for (Cell cell : values) {
								// concat the values of colList
								concatValues = Util.concatByteArray(concatValues,
										cell.getValue());
							}
							try {
								internalAdd(concatValues,
										Arrays.copyOf(rowid, rowid.length));
							} catch (NullPointerException NPEe) {
								// LOG.error("NPE for VALUE [" + new
								// String(values.get(0).getValue()) +
								// "] ROW ["
								// + Bytes.toString(rowid) + "] in column ["
								// + Bytes.toString(columnFamily) + ":"
								// + Bytes.toString(qualifier) + "]", NPEe);
								throw NPEe;
							} catch (ClassNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
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

	public void remove(byte[] key, byte[] value) {
		rwLock.writeLock().lock();
		try {
			RowIndex rowIndex = rowIndexMap.get(new ByteArrayWrapper(key));
			if (rowIndex != null) {
				rowIndex.remove(value, maxTreeSize);
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			rwLock.writeLock().unlock();
		}
	}

	public Set<byte[]> filterRowsFromCriteria(Criterion<?> criterion) {
		rwLock.readLock().lock();

		Set<byte[]> rowKeys = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		// Htable based only handles EQUAL relation
		// Because only the indexing structure knows how to handle different
		// criteria
		// I moved all the case handling from Criterion to this class
		// Modified by Cong

		// for (String value : criterion
		// .getMatchingValueSetFromIndex(rowIndexMap.keySet())) {
		// rowKeys.addAll(rowIndexMap.get(value).getPKRefs());
		// }
		ByteArrayWrapper criterionValue = new ByteArrayWrapper(
				(byte[]) criterion.getComparisonValue());

		switch (criterion.getComparisonType()) {

		case EQUAL:
			RowIndex rowIndex = rowIndexMap.get(criterionValue);
			if (rowIndex == null) {
				rwLock.readLock().unlock();
				return null;
			} else {
				try {
					rowKeys.addAll(rowIndex.getPKRefs());
					return rowKeys;
				} catch (ClassNotFoundException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					rwLock.readLock().unlock();
				}
			}

		// not supported multiColumn query
		case RANGE:
			Range range = criterion.getRange();

			// Add lower bound and higher bound filter
			Column column = colList.get(0);
			FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
					column.getFamily(), column.getQualifier(), CompareOp.GREATER_OR_EQUAL,
					range.getLowerBound());
			list.addFilter(filter1);
			SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
					column.getFamily(), column.getQualifier(), CompareOp.LESS_OR_EQUAL,
					range.getHigherBound());
			list.addFilter(filter2);

			// scan the table based on the filter
			Configuration config = HBaseConfiguration.create();
			HTable table;
			try {
				table = new HTable(config, this.tableName);
				Scan s = new Scan();
				// set the filter lists
				s.setFilter(list);
				s.addColumn(column.getFamily(), column.getQualifier());
				ResultScanner scanner = table.getScanner(s);
				List<KeyValue> values = new ArrayList<KeyValue>();

				for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
					values = rr.list();
					byte[] rowid = values.get(0).getRow();
					rowKeys.add(rowid);
				}
				scanner.close();
				table.close();
				return rowKeys;

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				rwLock.readLock().unlock();
			}

			return null;

		default:
			rwLock.readLock().unlock();
			return null;
		}
	}

	@Override
	public void split(AbstractPluggableIndex daughterRegionA,
					  AbstractPluggableIndex daughterRegionB, byte[] splitRow) {

		rwLock.writeLock().lock();

		for (ByteArrayWrapper value : rowIndexMap.keySet()) {
			byte[][] sortedPKRefArray = this.get(value);
			int splitPoint = Arrays.binarySearch(sortedPKRefArray, splitRow,
					ByteUtil.BYTES_COMPARATOR);
			for (int i = 0; i < sortedPKRefArray.length; i++) {
				if ((splitPoint >= 0 && i < splitPoint)
						|| (splitPoint < 0 && i < Math.abs(splitPoint + 1))) {
					daughterRegionA.add(value.get(), sortedPKRefArray[i]);
				} else {
					daughterRegionB.add(value.get(), sortedPKRefArray[i]);
				}
			}
		}

		rwLock.writeLock().unlock();
	}


	// just for test purpose...
	public void sayHello() {
		System.out.println("Hello I am regionColumnIndex :)");
		System.out.println("Index type: " + this.test);
	}

	public static void main(String[] args) throws Throwable {

		String namespace = "ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex";
		String indexType = namespace + ".hashtableBased.RegionColumnIndex";

		List<Column> colList = new ArrayList<Column>();
		Column column1 = new Column(Bytes.toBytes("cf1"), Bytes.toBytes("a"));
		Column column2 = new Column(Bytes.toBytes("cf2"), Bytes.toBytes("c"));
		colList.add(column1);
		colList.add(column2);

		Object[] arguments = {2, colList};
		//RegionColumnIndex test = (RegionColumnIndex) AbstractPluggableIndex.getInstance(true, indexType, arguments, new Class [] {int.class, List.class});
		//RegionColumnIndex test = (RegionColumnIndex) AbstractPluggableIndex.getInstance(indexType, new Object[] {1,  Bytes.toBytes("test"), Bytes.toBytes("2")});
		//test.sayHello();

		List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(int.class);
		classes.add(byte[].class);
		Class<?>[] array = (Class<?>[]) classes.toArray();
	}
}
