package ca.mcgill.distsys.hbase.indexcoprocessorsinmem.pluggableIndex;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.IndexedColumnQuery;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.SecondaryIndexConstants;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.Util;

public class IndexOperations {

	private RegionIndex regionIndex = null;
	private String tableName;
	private Configuration config;
	private HTable table;

	public RegionIndex getRegionIndex() {
		return regionIndex;
	}

	public RegionIndex readRegionIndexFromFile(String filePath) {

		try {
			InputStream file = new FileInputStream(filePath);
			InputStream buffer = new BufferedInputStream(file);
			ObjectInput input = new ObjectInputStream(buffer);
			regionIndex = (RegionIndex) input.readObject();
			input.close();
			buffer.close();
			file.close();
			return regionIndex;
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Can't read from file.....");
		}
		return regionIndex;
		
	}

	public void writeRegionIndexToFile(String filePath) {

		try {
			OutputStream file = new FileOutputStream(filePath);
			OutputStream buffer = new BufferedOutputStream(file);
			ObjectOutput output = new ObjectOutputStream(buffer);
			output.writeObject(regionIndex);
			output.close();
			buffer.close();
			file.close();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Can't write to file...");
		}
	}

	public IndexOperations(String tableName, Configuration config) {
		this.tableName = tableName;
		this.config = config;
		try {
			this.table = new HTable(config, tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void preput(byte[] family, byte[] qualifier, byte[] rowID,
			byte[] value) {
		try {
			AbstractPluggableIndex index = regionIndex.get(family, qualifier);
			if (index != null) {
				Get g = new Get(rowID);
				Result r = table.get(g);
				byte[] quriedValue = r.getValue(family, qualifier);
				if (quriedValue == null) {

					index.add(value, rowID);
//					System.out.println("Inserting value: " + Bytes.toString(value));


				} else {

					index.removeValueFromIdx(quriedValue, rowID);
					index.add(value, rowID);
//					System.out.println("Removing and Inserting value: " + Bytes.toString(value));

				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void preDelete(byte[] family, byte[] qualifier, byte[] rowID,
			byte[] value) {

		try {
			AbstractPluggableIndex index = regionIndex.get(family, qualifier);
			if (index != null) {
				Get g = new Get(rowID);
				Result r = table.get(g);
				byte[] quriedValue = r.getValue(family, qualifier);
				if (quriedValue == null) {

				} else {
					if (Arrays.equals(quriedValue, value)) {

						index.removeValueFromIdx(quriedValue, rowID);

					}

				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Set<byte[]> execIndexedQuery(IndexedColumnQuery query) {
		try {
			return regionIndex.filterRowsFromCriteria(query.getCriteria(),
					null, query, null);
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public void createIndex(byte[] columnFamily, byte[] qualifier,
			HRegion region, String indexType, Object[] arguments) {
		if (regionIndex == null) {
			int maxTreeSize = this.config.getInt(
					SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
					SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
			regionIndex = new RegionIndex(maxTreeSize);
		}
		byte[] idxColKey = Util.concatByteArray(columnFamily, qualifier);
		if (!regionIndex.getIndexedColumns().contains(Bytes.toString(idxColKey))) {
			try {
				regionIndex.add(columnFamily, qualifier, region, indexType,
						arguments);
//				System.out.println("creating index table for columnFamily: " + Bytes.toString(columnFamily) + " qualifier: " + Bytes.toString(qualifier));
			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch blockin
				e.printStackTrace();
			}
		}
	}

	public void printIndex(byte[] family, byte[] qualifier) {
		try {
			AbstractPluggableIndex index = regionIndex.get(family, qualifier);
			System.out.println(index.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
