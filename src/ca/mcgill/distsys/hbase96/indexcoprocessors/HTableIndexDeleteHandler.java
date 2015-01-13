package ca.mcgill.distsys.hbase96.indexcoprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
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
 * Handles index updates for Delete Mutations.
 * --- NOTE This version does not handle multiple versions so only the latest version of a row is tracked ---
 * @author rrc
 *
 */
public class HTableIndexDeleteHandler {
    private static final Log LOG = LogFactory.getLog(HTableIndexDeleteHandler.class);

    private Delete delete;
    private HTableIndexCoprocessor regionIndexCoprocessor;
    private ObserverContext<RegionCoprocessorEnvironment> env;

    public HTableIndexDeleteHandler(Delete delete, HTableIndexCoprocessor regionIndexCoprocessor, ObserverContext<RegionCoprocessorEnvironment> e) {
        this.delete = delete;
        this.regionIndexCoprocessor = regionIndexCoprocessor;
        env = e;
    }

    public void processDelete() throws IOException {
        Set<String> indexedColumns;
        indexedColumns = regionIndexCoprocessor.getIndexedColumnsForTable();

        System.out.println("DELETE: Starting preDelete");
        if (!indexedColumns.isEmpty()) {
            System.out.println("indexedColumns is not Empty; index update may occur.");
            Get get = new Get(delete.getRow());
            Result result = null;
            
            try {
                result = env.getEnvironment().getRegion().get(get);
            } catch (IOException e) {
                LOG.error(
                        "DELETE: Failed to retrieve the current row. This is required for index update. The index may be in an invalid state if the delete succeeds and affects and indexed column.",
                        e);
            }
            
            List<KeyValue> indexedKVList = makeIndexedKVList(indexedColumns, result);
            
            if(!indexedKVList.isEmpty()) {
                updateTableIndexes(indexedKVList);
            } else {
                System.out.println("DELETE: No Index Update to do for Delete.");
            }

        }
    }

    private List<KeyValue> makeIndexedKVList(Set<String> indexedColumns, Result result) {
        List<KeyValue> indexedKVList = new ArrayList<KeyValue>();
        if (delete.isEmpty()) {
            // DELETE ROW
            System.out.println("DELETE: ROW [" + Bytes.toString(delete.getRow()) + "] to be deleted.");
            indexedKVList.addAll(getIndexedKV(Arrays.asList(result.raw()), indexedColumns));
        } else {
            for (byte[] family : delete.getFamilyMap().keySet()) {
                List<KeyValue> columnsToDelete = getColumsToDeleteForFamily(delete, family);
                if (columnsToDelete.isEmpty()) {
                    // DELETE ENTIRE COLUMN FAMILY                         
                    System.out.println("DELETE: ROW [" + Bytes.toString(delete.getRow()) + "], CF [" + Bytes.toString(family) + "] to be deleted.");
                    List<KeyValue> columnsToDeleteCurrentValueList = new ArrayList<KeyValue>();
                    for(KeyValue kv: result.raw()) {
                        if(Arrays.equals(kv.getFamily(), family)) {
                            columnsToDeleteCurrentValueList.add(kv);
                            indexedKVList.addAll(getIndexedKV(columnsToDeleteCurrentValueList, indexedColumns));
                        }
                    }
                } else {
                    // DELETE FOUND COLUMS OF COLUMN FAMILY                    
                    List<KeyValue> columnsToDeleteCurrentValueList = new ArrayList<KeyValue>();
                    for(KeyValue ctd: columnsToDelete){
                        for(KeyValue kv: result.raw()) {
                            if(Arrays.equals(ctd.getQualifier(),kv.getQualifier())) {
                                columnsToDeleteCurrentValueList.add(kv);
                                break;
                            }
                        }
                    }
                    System.out.println("DELETE: ROW [" + Bytes.toString(delete.getRow()) + "], CF [" + Bytes.toString(family) + "], COL(s) [" + columnsToDeleteCurrentValueList + "] to be deleted.");
                    indexedKVList.addAll(getIndexedKV(columnsToDeleteCurrentValueList, indexedColumns));
                }
                
            }
        }
        return indexedKVList;
    }

    private List<KeyValue> getIndexedKV(List<KeyValue> columnsToDelete, Set<String> indexedColumns) {
        List<KeyValue> result = new ArrayList<KeyValue>();
        for (KeyValue kv : columnsToDelete) {
            if (indexedColumns.contains(Bytes.toString(Util.getSecondaryIndexTableName(regionIndexCoprocessor.getTableName(), kv.getFamily(),
                    kv.getQualifier())))) {
                result.add(kv);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<KeyValue> getColumsToDeleteForFamily(Delete del, byte[] family) {
        List<KeyValue> result = new ArrayList<KeyValue>();
        for (KeyValue kv : (List<KeyValue>)del.getFamilyMap().get(family)) {
            if (kv.getQualifierLength() > 0) {
                result.add(kv);
            }
        }
        return result;
    }
    
    private void updateTableIndexes(List<KeyValue> kVListToIndex) throws IOException {
        System.out.println("DELETE: Updating index for key-values " + kVListToIndex);
        for (KeyValue kv : kVListToIndex) {
            System.out.println("DELETE: Updating index for [" + Bytes.toString(kv.getValue()) + "].");
            HTableInterface idxTable = null;
            try {
                
                idxTable = regionIndexCoprocessor.getHTableInterface(Bytes.toString(Util.getSecondaryIndexTableName(regionIndexCoprocessor.getTableName(), kv.getFamily(), kv.getQualifier())));
                
                Put idxPut = new Put(kv.getValue());
                idxPut.add(Bytes.toBytes(SecondaryIndexConstants.INDEX_TABLE_IDX_CF_NAME), SecondaryIndexConstants.INDEX_TABLE_IDX_DEL_FROM_IDX,
                        kv.getRow());
                idxTable.put(idxPut);

            } catch (IOException IOe) {
                LOG.error("DELETE: Failed to update index for table [" + Bytes.toString(regionIndexCoprocessor.getTableName()) + "], column [" + Bytes.toString(kv.getFamily()) + ":"
                        + Bytes.toString(kv.getQualifier()) + "]", IOe);
            } finally {
                if (idxTable != null) {
                    try {
                        regionIndexCoprocessor.returnHTableInterface(idxTable);
                    } catch (IOException IOe) {
                        LOG.warn("Failed to close table [" + Bytes.toString(regionIndexCoprocessor.getTableName()) + "]", IOe);
                    }
                }
            }
        }
    }

}
