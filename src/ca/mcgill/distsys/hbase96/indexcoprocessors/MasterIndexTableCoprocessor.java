package ca.mcgill.distsys.hbase96.indexcoprocessors;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.util.Util;

import ca.mcgill.distsys.hbase96.indexcommons.SecondaryIndexConstants;

public class MasterIndexTableCoprocessor extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(MasterIndexTableCoprocessor.class);
    private boolean doNotRun = true;
    private Configuration configuration;
    private boolean masterIndexBroadcast = false;
    private JChannel jchannel;

    @Override
    public void start(CoprocessorEnvironment environment) {
        // make sure we are on a region server
        if (!(environment instanceof RegionCoprocessorEnvironment)) {
            throw new IllegalArgumentException("Indexes only act on regions - started in an environment that was not a region");
        }

        RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) environment;
        HTableDescriptor desc = env.getRegion().getTableDesc();

        if (desc.getNameAsString().equals(SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME)) {
            doNotRun = false;
            configuration = HBaseConfiguration.create();
            if (masterIndexBroadcast) {
                try {
                    initJChannel();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void initJChannel() throws Exception {
        loadConfiguration();
        jchannel = new JChannel(ClassLoader.getSystemResource("tcp.xml"));
        jchannel.setReceiver(new ReceiverAdapter());
        jchannel.connect("MasterIndexUpdateCluster");
    }

    private void loadConfiguration() throws Exception {
        String hosts = configuration.get("indexCoprocessor.regionserver_hosts");
        if (hosts == null) {
            LOG.fatal("Failed to setup communcation channel for indexing. indexProcessor.regionserver_hosts not set.");
            throw new Exception("Failed to setup communcation channel for indexing. indexProcessor.regionserver_hosts not set.");
        }
        System.setProperty("jgroups.tcpping.initial_hosts", hosts);

        String bindaddr = configuration.get("indexCoprocessor.bindaddr");
        if (bindaddr != null) {
            System.setProperty("jgroups.bind_addr", bindaddr);
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        if (!doNotRun && masterIndexBroadcast && jchannel != null) {
            jchannel.close();
        }
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        if (!doNotRun) {
            try {
                LOG.debug("MASTER INDEX TABLE: " + Bytes.toString(put.getRow()) + " must update its indexed columns cache due to a new index on a column; sending message.");
                jchannel.send(new Message(null, null, Util.objectToByteBuffer(put.getRow())));
            } catch (Exception ex) {
                throw new IOException("Failed to send message that an index has been created for table [" + Bytes.toString(put.getRow()) + "].", ex);
            }
        }
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        if (!doNotRun) {
            try {
                LOG.debug("MASTER INDEX TABLE: " + Bytes.toString(delete.getRow()) + " must update its indexed columns cache due to a removal of an index on a column; sending message.");
                jchannel.send(new Message(null, null, Util.objectToByteBuffer(delete.getRow())));
            } catch (Exception ex) {
                throw new IOException("Failed to send message that an index has been created for table [" + Bytes
                    .toString(delete.getRow()) + "].", ex);
            }
        }
    }

}
