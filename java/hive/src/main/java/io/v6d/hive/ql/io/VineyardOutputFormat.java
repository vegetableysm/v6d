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

import com.google.common.base.Stopwatch;
import com.google.common.base.StopwatchContext;

import io.v6d.core.common.util.ObjectID;
import io.v6d.core.common.util.VineyardException;
import io.v6d.core.client.IPCClient;
import io.v6d.core.client.ds.ObjectMeta;
import io.v6d.modules.basic.arrow.TableBuilder;
import io.v6d.modules.basic.columnar.ColumnarDataBuilder;
import io.v6d.modules.basic.arrow.SchemaBuilder;
import io.v6d.modules.basic.arrow.Arrow;
import io.v6d.modules.basic.arrow.RecordBatchBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.*;

public class VineyardOutputFormat<K extends NullWritable, V extends VineyardRowWritable>
        implements HiveOutputFormat<K, V> {
    private static Logger logger = LoggerFactory.getLogger(VineyardOutputFormat.class);

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(
            JobConf jc,
            Path finalOutPath,
            Class<? extends Writable> valueClass,
            boolean isCompressed,
            Properties tableProperties,
            Progressable progress)
            throws IOException {
        return new SinkRecordWriter(
                jc, finalOutPath, valueClass, isCompressed, tableProperties, progress);
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(
            FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable)
            throws IOException {
        return new MapredRecordWriter<K, V>();
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {}
}

class SinkRecordWriter implements FileSinkOperator.RecordWriter {
    private static Logger logger = LoggerFactory.getLogger(SinkRecordWriter.class);

    private static CloseableReentrantLock lock = new CloseableReentrantLock();

    private JobConf jc;
    private Path finalOutPath;
    private FileSystem fs;
    private List<VineyardRowWritable[]> objects;
    private int lastBatchIndex = 0;
    private final int batchSize = 3000000;
    private VineyardRowWritable[] currentRows;

    private Schema schema;
    private Properties tableProperties;
    private Progressable progress;

    // vineyard
    private TableBuilder tableBuilder;
    private SchemaBuilder schemaBuilder;
    RecordBatchBuilder recordBatchBuilder;
    List<RecordBatchBuilder> recordBatchBuilders;
    // private String tableName;
    private FSDataOutputStream output;

    private Stopwatch writeTimer = StopwatchContext.createUnstarted();

    public static final PathFilter VINEYARD_FILES_PATH_FILTER = new PathFilter() {
        @Override
        public boolean accept(Path p) {
            String name = p.getName();
            return name.startsWith("_task_tmp.") && (!name.substring(10, name.length()).startsWith("-"));
        }
    };

    private void getTableName() throws IOException {
        fs = finalOutPath.getFileSystem(jc);
        this.output = FileSystem.create(fs, finalOutPath, new FsPermission("777"));
        if (output == null) {
            throw new VineyardException.Invalid("Create table file failed.");
        }
    }

    public SinkRecordWriter(
            JobConf jc,
            Path finalOutPath,
            Class<? extends Writable> valueClass,
            boolean isCompressed,
            Properties tableProperties,
            Progressable progress) throws IOException {
        val watch = StopwatchContext.create();
        this.jc = jc;
        if (!ArrowWrapperWritable.class.isAssignableFrom(valueClass)) {
            throw new VineyardException.Invalid("value class must be ArrowWrapperWritable");
        }
        if (isCompressed) {
            throw new VineyardException.Invalid("compressed output is not supported");
        }
        this.finalOutPath = finalOutPath;
        this.tableProperties = tableProperties;
        this.progress = progress;

        // for (Object key : tableProperties.keySet()) {
        //     System.out.printf("table property: %s, %s\n", key, tableProperties.getProperty((String) key));
        // }
        getTableName();

        // objects = new ArrayList<VineyardRowWritable>();
        objects = new ArrayList<VineyardRowWritable[]>();
        VineyardRowWritable[] rows = new VineyardRowWritable[batchSize];
        recordBatchBuilders = new ArrayList<RecordBatchBuilder>();
        currentRows = rows;
        objects.add(rows);
        Arrow.instantiate();

        Context.println("creating a sink record writer uses: " + watch.stop());
    }

    @Override
    public void write(Writable w) throws IOException {
        writeTimer.start();
        // Context.println("Write");
        // if (w == null) {
        //     Context.println("w is null");
        //     return;
        // }

        VineyardRowWritable rowWritable = (VineyardRowWritable) w;
        // Context.println("value:" + (rowWritable.getValues().get(0)) + " " + (rowWritable.getValues().get(1)));
        // objects.add(rowWritable);
        if (lastBatchIndex < batchSize) {
            currentRows[lastBatchIndex++] = rowWritable;
        } else {
            Context.println("Record batch is full. Create new batch!");
            VineyardRowWritable[] rows = new VineyardRowWritable[batchSize];
            currentRows = rows;
            objects.add(rows);
            lastBatchIndex = 0;
            currentRows[lastBatchIndex++] = rowWritable;
        }
        writeTimer.stop();
    }
    
    // check if the table is already created.
    // if not, create a new table.
    // if yes, append the data to the table.(Get from vineyard, and seal it in a new table) 
    @SneakyThrows(VineyardException.class)
    @Override
    public void close(boolean abort) throws IOException {
        // Table oldTable = null;
        if (objects.get(0)[0] == null) {
            Context.println("No data to write.");
            return;
        }
        Context.println("closing SinkRecordWriter: objects size:" + objects.size() + ", write uses " + writeTimer);

        // construct record batch
        val client = Context.getClient();
        constructRecordBatch(client);
        tableBuilder = new TableBuilder(client, schemaBuilder);

        try (val lock = this.lock.open()) {
            val watch = StopwatchContext.create();
            for (int i = 0; i < recordBatchBuilders.size(); i++) {
                Context.println("record batch builder: " + i + ", row size: " + recordBatchBuilders.get(i).getNumRows());
                tableBuilder.addBatch(recordBatchBuilders.get(i));
            }
            ObjectMeta meta = tableBuilder.seal(client);
            Context.println("Table id in vineyard:" + meta.getId().value());
            client.persist(meta.getId());
            // Context.println("Table persisted, name:" + tableName);

            // client.putName(meta.getId(), tableName);
            Context.println("record batch size:" + tableBuilder.getBatchSize());
            Context.println("construct table from record batch builders use " + watch.stop());
            output.write((Long.toString(meta.getId().value()) + "\n").getBytes(StandardCharsets.UTF_8));
            output.close();
        }
    }

    private static ArrowType toArrowType(TypeInfo typeInfo) {
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
                    case BOOLEAN:
                        return Types.MinorType.BIT.getType();
                    case BYTE:
                        return Types.MinorType.TINYINT.getType();
                    case SHORT:
                        return Types.MinorType.SMALLINT.getType();
                    case INT:
                        return Types.MinorType.INT.getType();
                    case LONG:
                        return Types.MinorType.BIGINT.getType();
                    case FLOAT:
                        return Types.MinorType.FLOAT4.getType();
                    case DOUBLE:
                        return Types.MinorType.FLOAT8.getType();
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        return Types.MinorType.VARCHAR.getType();
                    case DATE:
                        return Types.MinorType.DATEDAY.getType();
                    case TIMESTAMP:
                        return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
                    case BINARY:
                        return Types.MinorType.VARBINARY.getType();
                    case DECIMAL:
                        final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                        return new ArrowType.Decimal(decimalTypeInfo.precision(), decimalTypeInfo.scale(), 128);
                    case INTERVAL_YEAR_MONTH:
                        return Types.MinorType.INTERVALYEAR.getType();
                    case INTERVAL_DAY_TIME:
                        return Types.MinorType.INTERVALDAY.getType();
                    case VOID:
                    // case TIMESTAMPLOCALTZ:
                    case UNKNOWN:
                    default:
                        throw new IllegalArgumentException();
                }
            case LIST:
                return ArrowType.List.INSTANCE;
            case STRUCT:
                return ArrowType.Struct.INSTANCE;
            case MAP:
                return new ArrowType.Map(false);
            case UNION:
                default:
                throw new IllegalArgumentException();
        }
    }

    private void constructRecordBatch(IPCClient client) throws VineyardException {
        val watch = StopwatchContext.create();
        // construct schema
        // StructVector rootVector = StructVector.empty(null, allocator);
        List<Field> fields = new ArrayList<Field>();
        // int columns = objects.get(0).getTargetTypeInfos().length;
        int columns = objects.get(0)[0].getTargetTypeInfos().length;
        for (int i = 0; i < columns; i++) {
            // Field field = Field.nullable(objects.get(0).getTargetTypeInfos()[i].getTypeName(), toArrowType(objects.get(0).getTargetTypeInfos()[i]));
            Field field = Field.nullable(objects.get(0)[0].getTargetTypeInfos()[i].getTypeName(), toArrowType(objects.get(0)[0].getTargetTypeInfos()[i]));
            fields.add(field);
        }
        schema = new Schema((Iterable<Field>)fields);
        schemaBuilder = SchemaBuilder.fromSchema(schema);

        // construct recordBatch
        for(int i = 0; i < objects.size(); i++) {
            int rowCount = objects.get(i).length;
            if (i == objects.size() - 1) {
                rowCount = lastBatchIndex;
            }
            recordBatchBuilder = new RecordBatchBuilder(client, schema, rowCount);
            Context.println("Create done!");
            fillRecordBatchBuilder(schema, objects.get(i), rowCount);
            Context.println("Fill done!");
            recordBatchBuilders.add(recordBatchBuilder);
        }
        Context.println("construct a record batch use " + watch.stop() + ", number of rows: " + objects.size());
    }

    private void fillRecordBatchBuilder(Schema schema, VineyardRowWritable[] batch, int rowCount) throws VineyardException {
        val watch = StopwatchContext.create();
        Context.println("rowCount: " + rowCount);
        for (int i = 0; i < schema.getFields().size(); i++) {
            ColumnarDataBuilder column;
            column = recordBatchBuilder.getColumnBuilder(i);
            Field field = schema.getFields().get(i);
            if (field.getType().equals(Arrow.Type.Boolean)) {
                for (int j = 0; j < rowCount; j++) {
                    boolean value;
                    if (batch[j].getValues().get(i) instanceof Boolean) {
                        value = (Boolean) batch[j].getValues().get(i);
                    } else {
                        value = ((BooleanWritable) batch[j].getValues().get(i)).get();
                    }
                    column.setBoolean(j, value);
                }
            } else if (field.getType().equals(Arrow.Type.Int)) {
                for (int j = 0; j < rowCount; j++) {
                    int value;
                    if (batch[j].getValues().get(i) instanceof Integer) {
                        value = (Integer) batch[j].getValues().get(i);
                    } else {
                        value = ((IntWritable) batch[j].getValues().get(i)).get();
                    }
                    column.setInt(j, value);
                }
            } else if (field.getType().equals(Arrow.Type.Int64)) {
                for (int j = 0; j < rowCount; j++) {
                    long value;
                    if (batch[j].getValues().get(i) instanceof Long) {
                        value = (Long) batch[j].getValues().get(i);
                    } else {
                        value = ((LongWritable) batch[j].getValues().get(i)).get();
                    }
                    column.setLong(j, value);
                }
            } else if (field.getType().equals(Arrow.Type.Float)) {
                for (int j = 0; j < rowCount; j++) {
                    float value;
                    if (batch[j].getValues().get(i) instanceof Float) {
                        value = (Float) batch[j].getValues().get(i);
                    } else {
                        value = ((FloatWritable) batch[j].getValues().get(i)).get();
                    }
                    column.setFloat(j, value);
                }
            } else if (field.getType().equals(Arrow.Type.Double)) {
                for (int j = 0; j < rowCount; j++) {
                    double value;
                    if (batch[j].getValues().get(i) instanceof Double) {
                        value = (Double) batch[j].getValues().get(i);
                    } else {
                        value = ((DoubleWritable) batch[j].getValues().get(i)).get();
                    }
                    column.setDouble(j, value);
                }
            } else if (field.getType().equals(Arrow.Type.VarChar)) {
                Context.println("var char");
                // may be not correct
                for (int j = 0; j < rowCount; j++) {
                    String value;
                    if (batch[j].getValues().get(i) instanceof String) {
                        value = (String) batch[j].getValues().get(i);
                    } else {
                        value = ((Text) batch[j].getValues().get(i)).toString();
                    }
                    column.setUTF8String(j, new org.apache.arrow.vector.util.Text(value));
                }
            } else {
                Context.println("Type:" + field.getType() + " is not supported");
                throw new VineyardException.NotImplemented(
                        "array builder for type " + field.getType() + " is not supported");
            }
        }
        Context.println("filling record batch builder use " + watch.stop());
    }
}

class MapredRecordWriter<K extends NullWritable, V extends VineyardRowWritable>
        implements RecordWriter<K, V> {
    MapredRecordWriter() throws IOException {
        Context.println("creating vineyard record writer");
        throw new RuntimeException("mapred record writter: unimplemented");
    }

    @Override
    public void write(K k, V v) throws IOException {
        System.out.printf("write: k = %s, v = %s\n", k, v);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        System.out.printf("closing\n");
    }
}
