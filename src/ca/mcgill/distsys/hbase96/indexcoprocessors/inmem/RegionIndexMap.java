package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem;

import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RegionIndexMap {
    private transient static Log LOG;

    private static RegionIndexMap instance;
    private HashMap<String, RegionIndex> regionIndexMap;
    private transient ReadWriteLock rwLock;

    private RegionIndexMap() {
        LOG = LogFactory.getLog(RegionIndexMap.class);
        regionIndexMap = new HashMap<String, RegionIndex>();
        rwLock = new ReentrantReadWriteLock(true);
    }

    public synchronized static RegionIndexMap getInstance() {
        if (instance == null) {
            instance = new RegionIndexMap();
        }

        return instance;
    }

    public void remove(String regionName) {
        rwLock.writeLock().lock();

        try {
            regionIndexMap.remove(regionName);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void add(String regionName, RegionIndex regionIndex) {
        rwLock.writeLock().lock();

        try {
            if (regionIndexMap.get(regionName) != null) {
                LOG.error("REGIONINDEXMAP: Attempt to add a RegionIndex to RegionIndexMap where one already exists for HRegion [" + regionName + "]");
            }

            regionIndexMap.put(regionName, regionIndex);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public RegionIndex get(String regionName) {
        rwLock.readLock().lock();

        try {
            RegionIndex result = regionIndexMap.get(regionName);
            return result;
        } finally {
            rwLock.readLock().unlock();
        }

    }
}
