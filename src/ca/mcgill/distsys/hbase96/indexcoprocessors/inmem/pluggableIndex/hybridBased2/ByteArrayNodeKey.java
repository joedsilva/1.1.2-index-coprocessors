package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.hash.Hashing;

public class ByteArrayNodeKey extends DeepCopyObject{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3664343359591830076L;
	private final byte[] data;

	public ByteArrayNodeKey(byte[] data) {
		if (data == null) {
			throw new NullPointerException();
		}
		this.data = data;
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof ByteArrayNodeKey)) {
			return false;
		}
		return Arrays.equals(data, ((ByteArrayNodeKey) other).data);
	}

	@Override
	public int hashCode() {
		//return Arrays.hashCode(data);
		// Using murmur hash instead of Arrays hash
		// COULD COMPARE THE PERFORMANCE LATER
		return Hashing.murmur3_32().hashBytes(data).asInt();
	}
	
	public byte[] get() {
		return data;
	}

	@Override
	public DeepCopyObject deepCopy() {
		byte [] newByteArray = new byte [data.length];
		System.arraycopy(data, 0, newByteArray, 0, data.length);
		return (new ByteArrayNodeKey(newByteArray));
	}

	@Override
	public int compareObject(DeepCopyObject object) {
		// using hbase bytes compareTo method
		return Bytes.compareTo(this.data, ((ByteArrayNodeKey) object).data);
	}
	
	public static void main(String [] args) {
		ByteArrayNodeKey node1 = new ByteArrayNodeKey(Bytes.toBytes("5"));
		ByteArrayNodeKey node2 = new ByteArrayNodeKey(Bytes.toBytes("10"));
		System.out.println(node1.compareTo(node2));
		System.out.println(Bytes.compareTo(Bytes.toBytes("5"), Bytes.toBytes("10")));
	}

}
