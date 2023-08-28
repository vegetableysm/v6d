/** Copyright 2020-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.v6d.hive.ql.io;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
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
import io.v6d.modules.basic.arrow.Arrow;
import io.v6d.modules.basic.arrow.RecordBatchBuilder;
import io.v6d.modules.basic.arrow.Table;
import io.v6d.modules.basic.arrow.TableBuilder;

public class VineyardInputFormat extends HiveInputFormat<NullWritable, RowWritable> implements VectorizedInputFormatInterface {
    @Override
    public RecordReader<NullWritable, RowWritable>
    getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter)
        throws IOException {
        reporter.setStatus(genericSplit.toString());
        Context.println("creating vineyard record reader");
        Context.println("split class:" + genericSplit.getClass().getName());
        return new VineyardRecordReader(job, (VineyardSplit) genericSplit);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        Context.println("getSplits");
        Context.println("Utilities.getPlanPath(conf) in get splits is " + Utilities.getPlanPath(job));
        List<InputSplit> splits = new ArrayList<InputSplit>();
        Path paths[] = FileInputFormat.getInputPaths(job);

        IPCClient client;
        // Table table[][];
        // List<Table> table[];
        long splitSize[];
        try {
            client = new IPCClient(System.getenv("VINEYARD_IPC_SOCKET"));
        } catch (Exception e) {
            Context.println("Connect vineyard failed.");
            return splits.toArray(new VineyardSplit[splits.size()]);
        }
        Arrow.instantiate();

        // table = new ArrayList[paths.length];
        splitSize = new long[paths.length];
        for (int i = 0; i < paths.length; i++) {

            // Construct table name.
            Path path = paths[i];
            String tableName = path.toString();
            Context.println("table name:" + tableName);

            // get object id from vineyard filesystem
            FileSystem fs = path.getFileSystem(job);
            FileStatus[] status = fs.listStatus(path);
            if (status.length < 1) {
                throw new IOException("Table file not found.");
            }

            // Maybe there exists more than one table file.
            // table[i] = new Table[status.length];
            splitSize[i] = 0;
            for (int j = 0; j < status.length; j++) {
                Path tableFilePath = status[j].getPath();
                FSDataInputStream in = fs.open(tableFilePath);
                FileStatus fileStatus = fs.getFileStatus(tableFilePath);
                byte[] buffer = new byte[(int)fileStatus.getLen()];
                int len = in.read(buffer, 0, (int)fileStatus.getLen());
                if (len == -1) {
                    continue;
                }
                String []objectIDStrs = new String(buffer, StandardCharsets.UTF_8).split("\n");
                for (int k = 0; k < objectIDStrs.length; k++) {
                    Context.println("ObjectID:" + objectIDStrs[k]);
                    try {
                        ObjectID tableID = new ObjectID(Long.parseLong(objectIDStrs[k].replaceAll("[^0-9]", "")));
                        System.out.println("tableID:" + tableID.value());
                        // Get table from vineyard.
                        try {
                            Table table = (Table) ObjectFactory.getFactory().resolve(client.getMetaData(tableID));
                            splitSize[i] += table.getBatches().size();
                        } catch (Exception e) {
                            throw new IOException("Get table failed.");
                        }
                    } catch (Exception e) {
                        continue;
                    }
                }
            }
        }

        // Split table.
        int realNumSplits = paths.length;
        for (int i = 0; i < realNumSplits; i++) {
            VineyardSplit vineyardSplit = new VineyardSplit(paths[i], 0, splitSize[i], job);
            splits.add(vineyardSplit);
        }
        

        // int batchSize, realNumSplits;
        // int totalRecordBatchCount = 0;
        // int partitionsSplitCount[] = new int[table.length];

        // Context.println("numSplits:" + numSplits + " table length:" + table.length);
        // if (numSplits <= table.length) {
        //     realNumSplits = table.length;
        //     for (int i = 0; i < table.length; i++) {
        //         partitionsSplitCount[i] = 1;
        //     }
        // } else {
        //     realNumSplits = 0;
        //     for (int i = 0; i < table.length; i++) {
        //         totalRecordBatchCount += table[i].getBatches().size();
        //     }
        //     batchSize = totalRecordBatchCount / numSplits == 0 ? 1 : totalRecordBatchCount / numSplits;
        //     for (int i = 0; i < table.length; i++) {
        //         partitionsSplitCount[i] = (table[i].getBatches().size() + batchSize - 1) / batchSize;
        //         realNumSplits += partitionsSplitCount[i];
        //     }
        // }

        // for (int i = 0; i < partitionsSplitCount.length; i++) {
        //     int partitionSplitCount = partitionsSplitCount[i];
        //     int partitionBatchSize = table[i].getBatches().size() / partitionSplitCount;
        //     Context.println("partitionBatchSize:" + partitionBatchSize);
        //     for (int j = 0; j < partitionSplitCount; j++) {
        //         VineyardSplit vineyardSplit = new VineyardSplit(paths[i], 0, table[i].getBatch(j).getBatch().getRowCount(), job);
        //         Context.println("split path:" + paths[i].toString());
        //         if (j == partitionSplitCount - 1) {
        //             vineyardSplit.setBatch(j * partitionBatchSize, table[i].getBatches().size() - j * partitionBatchSize);
        //         } else {
        //             vineyardSplit.setBatch(j * partitionBatchSize, partitionBatchSize);
        //         }
        //         splits.add(vineyardSplit);
        //     }
        // }

        // Context.println("num split:" + numSplits + " real num split:" + realNumSplits);
        // Context.println("table length:" + table.length);
        // for (int i = 0; i < table.length; i++) {
        //     Context.println("table[" + i + "] batch size:" + table[i].getBatches().size());
        //     Context.println("table[" + i + "] split count:" + partitionsSplitCount[i]);
        // }
        // client.disconnect();
        // Context.println("Splits size:" + splits.size());
        // for (int i = 0; i < splits.size(); i++) {
        //     Context.println("Split[" + i + "] length:" + splits.get(i).getLength());
        // }
        // Context.println("Utilities.getPlanPath(conf) in get splits is " + Utilities.getPlanPath(job));

        return splits.toArray(new VineyardSplit[splits.size()]);
    }
}

class VineyardRecordReader implements RecordReader<NullWritable, RowWritable> {
    private static Logger logger = LoggerFactory.getLogger(VineyardRecordReader.class);

    // vineyard field
    private static IPCClient client;
    // private String tableName;
    private List<ObjectID> tableObjectID = new ArrayList<ObjectID>();
    private Table table;
    // private int recordBatchIndex = 0;
    private int recordBatchInnerIndex = 0;
    private VectorSchemaRoot vectorSchemaRoot;

    // private int batchStartIndex;
    // private int batchSize;
    private int tableIndex = 0;
    private int recordBatchIndex = 0;

    VineyardRecordReader(JobConf job, VineyardSplit split) throws IOException {
        Context.println("VineyardBatchRecordReader");

        Path path = split.getPath();
        Context.println("Get path from splits:" + path.toString());

        FileSystem fs = path.getFileSystem(job);
        FileStatus[] status = fs.listStatus(path);
        if (status.length < 1) {
            throw new IOException("Table file not found or files are not merged.");
        }

        // There may be more than one table file.
        // tableObjectID = new ObjectID[status.length];
        for (int i = 0; i < status.length; i++) {
            Path tableFilePath = status[i].getPath();
            FSDataInputStream in = fs.open(tableFilePath);
            FileStatus fileStatus = fs.getFileStatus(tableFilePath);
            byte[] buffer = new byte[(int)fileStatus.getLen()];
            int len = in.read(buffer, 0, (int)fileStatus.getLen());
            if (len == -1) {
                continue;
            }
            String []objectIDStrs = new String(buffer, StandardCharsets.UTF_8).split("\n");
            for (int j = 0; j < objectIDStrs.length; j++) {
                Context.println("ObjectID:" + objectIDStrs[j]);
                try {
                    ObjectID tableID = new ObjectID(Long.parseLong(objectIDStrs[j].replaceAll("[^0-9]", "")));
                    Context.println("tableID:" + tableID.value());
                    tableObjectID.add(tableID);
                } catch (Exception e) {
                    continue;
                }
            // tableObjectID[i] = new ObjectID(Long.parseLong(objectIDStr.replaceAll("[^0-9]", "")));
            }
        }

        // batchStartIndex = split.getBatchStartIndex();
        // batchSize = split.getBatchSize();
        // recordBatchIndex = batchStartIndex;

        // connect to vineyard
        if (client == null) {
            // TBD: get vineyard socket path from table properties
            try {
                client = new IPCClient(System.getenv("VINEYARD_IPC_SOCKET"));
            } catch (Exception e) {
                Context.println("connect to vineyard failed!");
                Context.println(e.getMessage());
            }
        }
        if (client == null || !client.connected()) {
            Context.println("connected to vineyard failed!");
            return;
        } else {
            Context.println("connected to vineyard succeed!");
            Context.println("Hello, vineyard!");
        }

        Arrow.instantiate();
        // HiveConf.setVar(job, HiveConf.ConfVars.PLAN, "vineyard:///");
        Context.println("Utilities.getPlanPath(conf) is " + Utilities.getPlanPath(job));
        Context.println("Map work:" + Utilities.getMapWork(job));
    }

    @Override
    public void close() throws IOException {
        System.out.printf("closing\n");
        if(client.connected()) {
            client.disconnect();
            Context.println("Bye, vineyard!");
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
        // System.out.printf("get pos\n");
        return 0;
    }

    @Override
    public float getProgress() throws IOException {
        System.out.printf("get progress\n");
        return 0;
    }

    @Override
    public boolean next(NullWritable key, RowWritable value) throws IOException {
        // System.out.printf("next\n");
        Context.println("next");
        if (tableIndex > tableObjectID.size()) {
            Context.println("return false 1");
            return false;
        }

        if (table == null) {
            try {
                // Context.println("Get objdect id:" + tableName);
                // ObjectID objectID = client.getName(tableName, false);
                while (tableIndex < tableObjectID.size() && tableObjectID.get(tableIndex) == null) {
                    tableIndex++;
                }
                if (tableIndex >= tableObjectID.size()) {
                    Context.println("return false 2");
                    return false;
                }

                ObjectID objectID = tableObjectID.get(tableIndex++);
                Context.println("Table Id:" + objectID.value());
                table = (Table) ObjectFactory.getFactory().resolve(client.getMetaData(objectID));
            } catch (Exception e) {
                Context.println("Get objectID failed.");
                return false;
            }
        }
        Context.println("table length:" + table.getBatches().size());
        // Context.println("recordBatchIndex:" + recordBatchIndex + " batchSize:" + batchSize + " batchStartIndex:" + batchStartIndex);
        // if (recordBatchIndex >= batchSize + batchStartIndex) {
        //     Context.println("return false 1");
        //     return false;
        // }
        if (vectorSchemaRoot == null) {
            vectorSchemaRoot = table.getArrowBatch(recordBatchIndex);
        }

        Schema schema;
        schema = vectorSchemaRoot.getSchema();
        // Context.println("recordBatchInnerIndex:" + recordBatchInnerIndex + " vectorSchemaRoot.getRowCount():" + vectorSchemaRoot.getRowCount());
        value.constructRow(schema, vectorSchemaRoot, recordBatchInnerIndex);
        recordBatchInnerIndex += 1;
        if (recordBatchInnerIndex >= vectorSchemaRoot.getRowCount()) {
            recordBatchInnerIndex = 0;
            recordBatchIndex++;
            vectorSchemaRoot = null;
        }

        if (recordBatchIndex >= table.getBatches().size()) {
            recordBatchIndex = 0;
            table = null;
        }
        // Context.println("return true");
        return true;
    }
}
