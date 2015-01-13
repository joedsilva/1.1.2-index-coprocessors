package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.commons;

import java.io.Serializable;
import java.util.Arrays;

import com.google.common.hash.Hashing;

public final class ByteArrayWrapper implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5726937581098237015L;
	private final byte[] data;

	public ByteArrayWrapper(byte[] data) {
		if (data == null) {
			throw new NullPointerException();
		}
		this.data = data;
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof ByteArrayWrapper)) {
			return false;
		}
		return Arrays.equals(data, ((ByteArrayWrapper) other).data);
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
}