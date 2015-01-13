package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hashtableBased;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import ca.mcgill.distsys.hbase96.indexcommons.ByteUtil;

public class RowIndex implements Serializable {
    private static final long serialVersionUID = 8982587193479743555L;
    private boolean compressedTree;
    private TreeSet<byte[]> pkRefs;
    private byte[] compressedPKRefs;
    private transient ReentrantReadWriteLock rwLock;

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        rwLock = new ReentrantReadWriteLock(true);
    }
    
    public RowIndex() {
        compressedTree = false;
        pkRefs = new TreeSet<byte[]>(ByteUtil.BYTES_COMPARATOR);
        rwLock = new ReentrantReadWriteLock(true);
    }
    
    private void compressTree() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(new SnappyOutputStream(baos));
        oos.writeObject(pkRefs);
        oos.flush();
        compressedPKRefs = baos.toByteArray();
        oos.close();
        pkRefs = null;
    }

    @SuppressWarnings("unchecked")
    private TreeSet<byte[]> decompressTree() throws IOException, ClassNotFoundException {
        TreeSet<byte[]> pkRefs;
        ObjectInputStream ois = new ObjectInputStream(new SnappyInputStream(new ByteArrayInputStream(compressedPKRefs)));
        pkRefs = (TreeSet<byte[]>) ois.readObject();
        ois.close();
        return pkRefs;
    }

    public TreeSet<byte[]> getPKRefs() throws IOException, ClassNotFoundException {
        rwLock.readLock().lock();
        
        try {
            if (compressedTree) {
                return decompressTree();
            } else {
                return pkRefs;
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void add(byte[] value, int maxTreeSize) throws IOException, ClassNotFoundException {
        rwLock.writeLock().lock();

        try {
            if (compressedTree) {
                pkRefs = decompressTree();
            }

            pkRefs.add(value);
            if (pkRefs.size() > maxTreeSize) {
                compressTree();
                compressedTree = true;
            } else {
                compressedTree = false;
                compressedPKRefs = null;
            }

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void remove(byte[] value, int maxTreeSize) throws IOException, ClassNotFoundException {
        rwLock.writeLock().lock();
        
        try {
            if (compressedTree) {
                pkRefs = decompressTree();
            }

            pkRefs.remove(value);
            if (pkRefs.size() > maxTreeSize) {
                compressTree();
                compressedTree = true;
            } else {
                compressedTree = false;
                compressedPKRefs = null;
            }

        } finally {
            rwLock.writeLock().unlock();
        }
        
    }

}
