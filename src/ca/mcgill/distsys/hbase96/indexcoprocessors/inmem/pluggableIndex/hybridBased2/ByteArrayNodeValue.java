package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2;

import java.util.TreeSet;


import org.apache.hadoop.hbase.util.Bytes;

import ca.mcgill.distsys.hbase96.indexcommons.ByteUtil;

public class ByteArrayNodeValue extends DeepCopyObject{

	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -795716486126519173L;
	private byte [] rowKey;
	private TreeSet<byte[]> pkRefs;
	private int updateStatus;
	//private transient ReentrantReadWriteLock rwLock;
	
	
	public ByteArrayNodeValue(byte [] rowKey){
		pkRefs = new TreeSet<byte[]>(ByteUtil.BYTES_COMPARATOR);
        //rwLock = new ReentrantReadWriteLock(true);
        this.rowKey = Bytes.copy(rowKey);
        this.updateStatus = 1;
	} 
	
	public int getUpdateStatus() {
		return updateStatus;
	}
	
	public void setUpdateDone() {
		updateStatus = 0;
	}
	
	public TreeSet<byte[]> getPKRefs(){
		return pkRefs;
	}
	
	public void setPKRefs(TreeSet<byte[]> pkRefs) {
		this.pkRefs = pkRefs;
	}
	
	protected byte [] [] getPKRefsAsArray(){
		//rwLock.readLock().lock();

        try {
            if(pkRefs != null){
            	return pkRefs.toArray(new byte[pkRefs.size()][]);
            } else {
            	return null;
            }
        } finally {
            //rwLock.readLock().unlock();
        }
	}
	
	public byte[] getRowKey(){
		return rowKey;
	}
	
	public void add(byte[] value){
		//rwLock.writeLock().lock();
		pkRefs.add(value);
		//rwLock.writeLock().unlock();
	}
	
	public void remove(byte[] value) {
		//rwLock.writeLock().lock();
		pkRefs.remove(value);
		//rwLock.writeLock().unlock();
	}

	@Override
	public DeepCopyObject deepCopy() {
		byte [] newRowKey = new byte [this.rowKey.length];
		System.arraycopy(this.rowKey, 0, newRowKey, 0, this.rowKey.length);
		ByteArrayNodeValue newNodeValue = new ByteArrayNodeValue(newRowKey);
		// The value can't be deep copied
		// only copy the reference!
		newNodeValue.setPKRefs(pkRefs);
		return newNodeValue;
	}

	@Override
	public int compareObject(DeepCopyObject object) {
		return Bytes.compareTo(this.rowKey, ((ByteArrayNodeValue) object).rowKey);
	}

}
