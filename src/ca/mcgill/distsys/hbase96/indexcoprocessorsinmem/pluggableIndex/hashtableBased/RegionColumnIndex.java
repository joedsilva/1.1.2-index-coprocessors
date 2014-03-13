package ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex.hashtableBased;

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

// modified by Cong
import org.apache.hadoop.hbase.Cell;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Range;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex.AbstractPluggableIndex;

public class RegionColumnIndex extends AbstractPluggableIndex implements
		Serializable {

	// private transient static Log LOG;

	/**
	 * 
	 */
	private static final long serialVersionUID = -1641091015504586661L;
	private HashMap<String, RowIndex> rowIndexMap;
	private transient ReadWriteLock rwLock;
	private byte[] columnFamily;
	private byte[] qualifier;
	private int maxTreeSize;
	private String tableName;

	private void readObject(ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		rwLock = new ReentrantReadWriteLock(true);
		// LOG = LogFactory.getLog(RegionColumnIndex.class);
	}

	public RegionColumnIndex(int maxTreeSize, byte[] columnFamily,
			byte[] qualifier) {
		// LOG = LogFactory.getLog(RegionColumnIndex.class);
		rowIndexMap = new HashMap<String, RowIndex>(15000);
		rwLock = new ReentrantReadWriteLock(true);
		this.columnFamily = columnFamily;
		this.qualifier = qualifier;
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

	public byte[][] get(byte[] key) {
		return get(new String(key));
	}

	public byte[][] get(String key) {
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
								} catch (ClassNotFoundException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
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

	public void removeValueFromIdx(byte[] key, byte[] value) {
		rwLock.writeLock().lock();
		try {
			RowIndex rowIndex = rowIndexMap.get(new String(key));
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
		String key = new String((byte[]) criterion.getComparisonValue());
		
		switch (criterion.getComparisonType()) {

		case EQUAL:
			RowIndex rowIndex = rowIndexMap.get(key);
			if (rowIndex == null) {
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
		case RANGE:
			Range range = criterion.getRange();

			// Add lower bound and higher bound filter
			FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
					columnFamily, qualifier, CompareOp.GREATER_OR_EQUAL,
					range.getLowerBound());
			list.addFilter(filter1);
			SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
					columnFamily, qualifier, CompareOp.LESS_OR_EQUAL,
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
				s.addColumn(columnFamily, qualifier);
				ResultScanner scanner = table.getScanner(s);
				List<KeyValue> values = new ArrayList<KeyValue>();

				for (Result rr = scanner.next(); rr != null; rr = scanner
						.next()) {
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

	Set<String> keySet() {
		return rowIndexMap.keySet();
	}

	byte[] getColumnFamily() {
		return columnFamily;
	}

	byte[] getQualifier() {
		return qualifier;
	}

	public String toString() {
		for (String key : keySet()) {
			System.out.print("Key: " + key + "  Values: ");
			try {
				for (byte[] value : rowIndexMap.get(key).getPKRefs()) {
					System.out.print(new String(value) + ", ");
				}
				System.out.println("");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return "";
	}
}
