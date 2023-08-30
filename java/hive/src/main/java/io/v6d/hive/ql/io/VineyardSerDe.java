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

import java.util.Properties;

import com.google.common.base.Stopwatch;
import com.google.common.base.StopwatchContext;
import io.v6d.core.client.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.*;

import lombok.val;

public class VineyardSerDe extends AbstractSerDe {
    private StructTypeInfo rowTypeInfo;
    private TypeInfo[] targetTypeInfos;
    private StructObjectInspector objectInspector;
    private ObjectInspector[] objectInspectors;

    private RecordWrapperWritable writable = new RecordWrapperWritable();

    private long elements = 0;
    private Stopwatch serializeWatch = StopwatchContext.createUnstarted();
    private Stopwatch deserializeWatch = StopwatchContext.createUnstarted();

    @Override
    public void initialize(Configuration configuration, Properties tableProperties)
            throws SerDeException {
        initializeTypeInfo(configuration, tableProperties);
    }

    public void initializeTypeInfo(Configuration configuration, Properties tableProperties) {
        this.rowTypeInfo = TypeContext.computeStructTypeInfo(tableProperties);
        this.targetTypeInfos = TypeContext.computeTargetTypeInfos(this.rowTypeInfo, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
        this.objectInspector = (StructObjectInspector) TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo (this.rowTypeInfo);
        this.objectInspectors = this.objectInspector.getAllStructFieldRefs().stream().map(f -> f.getFieldObjectInspector()).toArray(ObjectInspector[]::new);
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return RecordWrapperWritable.class;
    }

    @Override
    public RecordWrapperWritable serialize(Object obj, ObjectInspector objInspector) {
        // VineyardException.asserts(obj.getClass().isArray(), "expect an array of object as the row");

        val inspectors = ((StructObjectInspector)objInspector).getAllStructFieldRefs().stream().map(f -> f.getFieldObjectInspector()).toArray(ObjectInspector[]::new);
        // serializeWatch.start();
        val values = ObjectInspectorUtils.copyToStandardObject((Object[])obj,
                inspectors, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
        writable.record = values;
        // serializeWatch.stop();
        // elements++;
        // if (elements % 1000000 == 0){
        //     Context.printf("serialize %d elements use %s\n", elements, serializeWatch.toString());
        // }
        return writable;
    }

    @Override
    public Object deserialize(Writable writable) {
        deserializeWatch.start();
        // System.out.println("deserialize called");
        val values = ((RowWritable) writable).getValues();
        deserializeWatch.stop();
        elements++;
        if (elements % 1000000 ==0 ){
            Context.printf("deserialize %d elements use %s\n", elements, deserializeWatch.toString());
        }
        return values;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public ObjectInspector getObjectInspector() {
        return this.objectInspector;
    }
}
