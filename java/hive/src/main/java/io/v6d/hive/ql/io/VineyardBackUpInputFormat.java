package io.v6d.hive.ql.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.v6d.core.client.IPCClient;
import io.v6d.core.client.ds.ObjectFactory;
import io.v6d.core.common.util.ObjectID;
import io.v6d.core.common.util.VineyardException;
import io.v6d.modules.basic.arrow.Arrow;
import io.v6d.modules.basic.arrow.RecordBatchBuilder;
import io.v6d.modules.basic.arrow.Table;
import io.v6d.modules.basic.arrow.TableBuilder;
import lombok.val;

public class VineyardBackUpInputFormat extends HiveInputFormat<NullWritable, RowWritable> implements VectorizedInputFormatInterface {
    @Override
    public RecordReader<NullWritable, RowWritable>
    getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter)
        throws IOException {
        reporter.setStatus(genericSplit.toString());
        System.out.printf("creating vineyard record reader\n");
        System.out.println("split class:" + genericSplit.getClass().getName());
        return new VineyardRecordReader(job, (VineyardSplit) genericSplit);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        System.out.println("getSplits");
        System.out.println("Utilities.getPlanPath(conf) in get splits is " + Utilities.getPlanPath(job));
        (new Exception()).printStackTrace();
        List<InputSplit> splits = new ArrayList<InputSplit>();
        Path paths[] = FileInputFormat.getInputPaths(job);

        IPCClient client;
        ObjectID tableObjectID[];
        Table table[];
        try {
            client = new IPCClient(System.getenv("VINEYARD_IPC_SOCKET"));
        } catch (Exception e) {
            System.out.println("Connect vineyard failed.");
            return splits.toArray(new VineyardSplit[splits.size()]);
        }
        Arrow.instantiate();

        table = new Table[paths.length];
        tableObjectID = new ObjectID[paths.length];
        for (int i = 0; i < paths.length; i++) {
            // int index = path.toString().lastIndexOf("/");
            // System.out.println("Path:" + path.toString());
            // String tableName = path.toString().substring(index + 1);

            // Construct table name.
            Path path = paths[i];
            String tableName = path.toString();
            tableName = tableName.replace("/", "#");
            System.out.println("table name:" + tableName);

            // Get table from vineyard.
            try {
                tableObjectID[i] = client.getName(tableName);
                table[i] = (Table) ObjectFactory.getFactory().resolve(client.getMetaData(tableObjectID[i]));
            } catch (Exception e) {
                System.out.println("Get table failed");
                VineyardSplit vineyardSplit = new VineyardSplit(path, 0, 0, job);
                splits.add(vineyardSplit);
                return splits.toArray(new VineyardSplit[splits.size()]);
            }
        }

        // Split table.
        int batchSize, realNumSplits;
        int totalRecordBatchCount = 0;
        int partitionsSplitCount[] = new int[table.length];

        System.out.println("numSplits:" + numSplits + " table length:" + table.length);
        if (numSplits <= table.length) {
            realNumSplits = table.length;
            for (int i = 0; i < table.length; i++) {
                partitionsSplitCount[i] = 1;
            }
        } else {
            realNumSplits = 0;
            for (int i = 0; i < table.length; i++) {
                totalRecordBatchCount += table[i].getBatches().size();
            }
            batchSize = totalRecordBatchCount / numSplits == 0 ? 1 : totalRecordBatchCount / numSplits;
            for (int i = 0; i < table.length; i++) {
                partitionsSplitCount[i] = (table[i].getBatches().size() + batchSize - 1) / batchSize;
                realNumSplits += partitionsSplitCount[i];
            }
        }

        for (int i = 0; i < partitionsSplitCount.length; i++) {
            int partitionSplitCount = partitionsSplitCount[i];
            int partitionBatchSize = table[i].getBatches().size() / partitionSplitCount;
            System.out.println("partitionBatchSize:" + partitionBatchSize);
            for (int j = 0; j < partitionSplitCount; j++) {
                VineyardSplit vineyardSplit = new VineyardSplit(paths[i], 0, table[i].getBatch(j).getBatch().getRowCount(), job);
                System.out.println("split path:" + paths[i].toString());
                if (j == partitionSplitCount - 1) {
                    vineyardSplit.setBatch(j * partitionBatchSize, table[i].getBatches().size() - j * partitionBatchSize);
                } else {
                    vineyardSplit.setBatch(j * partitionBatchSize, partitionBatchSize);
                }
                splits.add(vineyardSplit);
            }
        }
            // int batchSize = table.getBatches().size();
            // int realNumSplits = batchSize < numSplits ? batchSize : numSplits;
            // int size =  table.getBatches().size() / realNumSplits;
        
            // Fill splits
            // for (int i = 0; i < realNumSplits; i++) {
            //     VineyardSplit vineyardSplit = new VineyardSplit(path, 0, 0, job);
            //     if (i == realNumSplits - 1) {
            //         vineyardSplit.setBatch(i * size, table.getBatches().size() - i * size);
            //     } else {
            //         vineyardSplit.setBatch(i * size, size);
            //     }
            //     splits.add(vineyardSplit);
            // }
        System.out.println("num split:" + numSplits + " real num split:" + realNumSplits);
        System.out.println("table length:" + table.length);
        for (int i = 0; i < table.length; i++) {
            System.out.println("table[" + i + "] batch size:" + table[i].getBatches().size());
            System.out.println("table[" + i + "] split count:" + partitionsSplitCount[i]);
        }
        client.disconnect();
        System.out.println("Splits size:" + splits.size());
        for (int i = 0; i < splits.size(); i++) {
            System.out.println("Split[" + i + "] length:" + splits.get(i).getLength());
        }
        System.out.println("Utilities.getPlanPath(conf) in get splits is " + Utilities.getPlanPath(job));
        // HiveConf.setVar(job, HiveConf.ConfVars.PLAN, "vineyard:///");
        return splits.toArray(new VineyardSplit[splits.size()]);
    }
}

class VineyardRecordReader implements RecordReader<NullWritable, RowWritable> {
    private static Logger logger = LoggerFactory.getLogger(VineyardRecordReader.class);

    // vineyard field
    private static IPCClient client;
    private String tableName;
    private Table table;
    private int recordBatchIndex = 0;
    private int recordBatchInnerIndex = 0;
    private Boolean tableNameValid = false;
    private VectorSchemaRoot vectorSchemaRoot;

    private int batchStartIndex;
    private int batchSize;

    private TableBuilder tableBuilder;
    private List<RecordBatchBuilder> recordBatchBuilders;
    private static RecordBatchBuilder recordBatchBuilder;

    VineyardRecordReader(JobConf job, VineyardSplit split) {
        System.out.println("VineyardBatchRecordReader");
        String path = split.getPath().toString();
        // int index = path.lastIndexOf("/");
        // tableName = path.substring(index + 1);
        tableName = path.replace('/', '#');
        System.out.println("Table name:" + tableName);
        tableNameValid = true;

        batchStartIndex = split.getBatchStartIndex();
        batchSize = split.getBatchSize();
        recordBatchIndex = batchStartIndex;
        // connect to vineyard
        if (client == null) {
            // TBD: get vineyard socket path from table properties
            try {
                client = new IPCClient(System.getenv("VINEYARD_IPC_SOCKET"));
            } catch (Exception e) {
                System.out.println("connect to vineyard failed!");
                System.out.println(e.getMessage());
            }
        }
        if (client == null || !client.connected()) {
            System.out.println("connected to vineyard failed!");
            return;
        } else {
            System.out.println("connected to vineyard succeed!");
            System.out.println("Hello, vineyard!");
        }

        Arrow.instantiate();
        // HiveConf.setVar(job, HiveConf.ConfVars.PLAN, "vineyard:///");
        System.out.println("Utilities.getPlanPath(conf) is " + Utilities.getPlanPath(job));
        System.out.println("Map work:" + Utilities.getMapWork(job));
    }

    @Override
    public void close() throws IOException {
        System.out.printf("closing\n");
        if(client.connected()) {
            client.disconnect();
            System.out.println("Bye, vineyard!");
        }
    }

    @Override
    public NullWritable createKey() {
        System.out.printf("creating key\n");
        return null;
    }

    @Override
    public RowWritable createValue() {
        System.out.printf("creating value\n");
        return new RowWritable();
    }

    @Override
    public long getPos() throws IOException {
        System.out.printf("get pos\n");
        return 0;
    }

    @Override
    public float getProgress() throws IOException {
        System.out.printf("get progress\n");
        return 0;
    }

    @Override
    public boolean next(NullWritable key, RowWritable value) throws IOException {
        System.out.printf("next\n");
        if (tableNameValid) {
            if (table == null) {
                try {
                    System.out.println("Get objdect id:" + tableName);
                    ObjectID objectID = client.getName(tableName, false);
                    if (objectID != null)
                        System.out.println("get object done");
                    table = (Table) ObjectFactory.getFactory().resolve(client.getMetaData(objectID));
                } catch (Exception e) {
                    System.out.println("Get objectID failed.");
                    return false;
                }
            }
            System.out.println("table length:" + table.getBatches().size());
            System.out.println("recordBatchIndex:" + recordBatchIndex + " batchSize:" + batchSize + " batchStartIndex:" + batchStartIndex);
            if (recordBatchIndex >= batchSize + batchStartIndex) {
                return false;
            }
            if (vectorSchemaRoot == null) {
                vectorSchemaRoot = table.getArrowBatch(recordBatchIndex);
            }
            Schema schema;
            schema = vectorSchemaRoot.getSchema();
            System.out.println("recordBatchInnerIndex:" + recordBatchInnerIndex + " vectorSchemaRoot.getRowCount():" + vectorSchemaRoot.getRowCount());
            value.constructRow(schema, vectorSchemaRoot, recordBatchInnerIndex);
            recordBatchInnerIndex += 1;
            if (recordBatchInnerIndex >= vectorSchemaRoot.getRowCount()) {
                recordBatchInnerIndex = 0;
                recordBatchIndex++;
                vectorSchemaRoot = null;
            }
            System.out.println("return true");
            return true;
        }
        return false;
    }
}
