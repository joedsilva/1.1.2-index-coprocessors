package ca.mcgill.distsys.hbase96.indexcoprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import ca.mcgill.distsys.hbase96.indexcommons.IndexedColumn;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Column;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

import ca.mcgill.distsys.hbase96.indexcommons.SecondaryIndexConstants;
import ca.mcgill.distsys.hbase96.indexcommons.Util;

/**
 * Handles index updates for Put Mutations. --- NOTE This version does not
 * handle multiple versions so only the latest version of a row is tracked ---
 * 
 * @author rrc
 * 
 */
public class HTableIndexPutHandler {
    private static final Log LOG = LogFactory.getLog(HTableIndexPutHandler.class);

    private Put put;
    private HTableIndexCoprocessor regionIndexCoprocessor;
    private ObserverContext<RegionCoprocessorEnvironment> env;

    public HTableIndexPutHandler(Put put, HTableIndexCoprocessor regionIndexCoprocessor, ObserverContext<RegionCoprocessorEnvironment> e) {
        this.put = put;
        this.regionIndexCoprocessor = regionIndexCoprocessor;
        env = e;
    }

    public void processPut() throws IOException {
        Set<String> indexedColumns = regionIndexCoprocessor.getIndexedColumnsForTable();
        if (!indexedColumns.isEmpty()) {
            List<KeyValue> kVListToIndex = new ArrayList<KeyValue>();
            getValueToIndex(put, kVListToIndex, indexedColumns);
            if (!kVListToIndex.isEmpty()) {
                Get get = new Get(put.getRow());
                Result result = null;

                try {
                    result = env.getEnvironment().getRegion().get(get);
                } catch (IOException e) {
                    LOG.error("PUT: Failed to retrieve the current row. " +
                        "This is required for index update. The index may be " +
                        "in an invalid state if the put succeeds and " +
                        "affects an already indexed column value.", e);
                    throw e;
                }
                updateTableIndexes(kVListToIndex, result);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void getValueToIndex(Put put, List<KeyValue> kVListToIndex,
        Set<String> indexedColumns) {
        for (byte[] family : put.getFamilyMap().keySet()) {
            for (KeyValue kv : (List<KeyValue>) put.getFamilyMap().get(family)) {
                if (indexedColumns.contains(Util.getSecondaryIndexTableName(
                        regionIndexCoprocessor.getTableNameAsString(),
                        new Column(family, kv.getQualifier())))) {
                    kVListToIndex.add(kv);
                }
            }
        }
    }

    private void updateTableIndexes(List<KeyValue> kVListToIndex, Result currentRow) throws IOException {
        for (KeyValue kv : kVListToIndex) {
            HTableInterface idxTable = null;
            byte[] idxCF = Bytes.toBytes(SecondaryIndexConstants.INDEX_TABLE_IDX_CF_NAME);
            byte[] idxC = Bytes.toBytes(SecondaryIndexConstants.INDEX_TABLE_IDX_C_NAME);
            try {
                String idxTableName = Util.getSecondaryIndexTableName(
                    regionIndexCoprocessor.getTableNameAsString(),
                    new Column(kv.getFamily(), kv.getQualifier()));
                idxTable = regionIndexCoprocessor.getHTableInterface(idxTableName);
                KeyValue currentValue = currentRow.getColumnLatest(kv.getFamily(), kv.getQualifier());
                
                if (currentValue != null) {
                    updateCurrentValueMapping(kv, idxTable, idxCF, idxC, currentValue);
                }

                Put idxPut = new Put(kv.getValue());
                idxPut.add(Bytes.toBytes(SecondaryIndexConstants.INDEX_TABLE_IDX_CF_NAME),
                        SecondaryIndexConstants.INDEX_TABLE_IDX_PUT_TO_IDX, kv.getRow());
                idxTable.put(idxPut);
            } catch (IOException IOe) {
                LOG.error("PUT: Failed to add to index for table [" + Bytes.toString(regionIndexCoprocessor.getTableName()) + "], column ["
                        + Bytes.toString(kv.getFamily()) + ":" + Bytes.toString(kv.getQualifier()) + "]", IOe);
            } finally {
                regionIndexCoprocessor.returnHTableInterface(idxTable);
            }
        }
    }

    private void updateCurrentValueMapping(KeyValue kv, HTableInterface idxTable, byte[] idxCF, byte[] idxC, KeyValue currentValue)
            throws IOException {
        if (!Arrays.equals(currentValue.getValue(), kv.getValue())) {
            // The current value is different from the value to be inserted; we
            // have to remove current value->row mapping from the index before
            // adding the new one.

            Put idxPut = new Put(currentValue.getValue());
            idxPut.add(Bytes.toBytes(SecondaryIndexConstants.INDEX_TABLE_IDX_CF_NAME), SecondaryIndexConstants.INDEX_TABLE_IDX_DEL_FROM_IDX,
                    currentValue.getRow());
            idxTable.put(idxPut);

        }
    }
}
