package ca.mcgill.distsys.hbase96.indexcoprocessors.jgroups;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;

import ca.mcgill.distsys.hbase96.indexcoprocessors.HTableIndexCoprocessor;

public class MasterIndexUpdateReceiver extends ReceiverAdapter {
    private static final Log LOG = LogFactory.getLog(MasterIndexUpdateReceiver.class);
    HTableIndexCoprocessor idxCoprocessor;
    JChannel channel;

    public void start(HTableIndexCoprocessor idxCoprocessor) throws Exception {
        this.idxCoprocessor = idxCoprocessor;
        loadConfiguration();
        channel = new JChannel(ClassLoader.getSystemResource("tcp.xml"));
        channel.setReceiver(this);
        channel.connect("MasterIndexUpdateCluster");
    }

    private void loadConfiguration() throws Exception {
      String hosts = idxCoprocessor.getConfiguration().get("indexCoprocessor.regionserver_hosts");
      //hosts = "disl-cn1.CS.McGill.CA[7800]";
      if (hosts == null) {
            LOG.fatal("Failed to setup communcation channel for indexing. indexProcessor.regionserver_hosts not set.");
            throw new Exception("Failed to setup communcation channel for indexing. indexProcessor.regionserver_hosts not set.");
        }
        System.setProperty("jgroups.tcpping.initial_hosts", hosts);

        String bindaddr = idxCoprocessor.getConfiguration().get("indexCoprocessor.bindaddr");
        //bindaddr = "132.206.51.144";
        if (bindaddr != null) {
            System.setProperty("jgroups.bind_addr", bindaddr);
        }

    }

    public void stop() {
        channel.close();
    }

    @Override
    public void receive(Message msg) {
        try {
            LOG.debug("Table " +  Bytes.toString(idxCoprocessor.getTableName())  +  " received message: Table " + Bytes.toString((byte[]) msg.getObject()) + " must update its index columns cache.");
            if (Arrays.equals((byte[]) msg.getObject(), idxCoprocessor.getTableName())) {
                LOG.debug("Table " + Bytes.toString(idxCoprocessor.getTableName())  + " is updating its indexed columns cache.");
                idxCoprocessor.initIndexedColumnsForTable();
            }
        } catch (IOException e1) {
            LOG.error("Failed to update from the master index table.", e1);
        } catch (Exception e) {
            LOG.error("Failed to read contents of message; updating from master index table just in case.", e);
            try {
                idxCoprocessor.initIndexedColumnsForTable();
            } catch (IOException e1) {
                LOG.error("Failed to update from the master index table.", e1);
            }
        }

    }

}
