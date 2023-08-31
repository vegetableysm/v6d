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

import java.io.DataInput;
import java.io.DataOutput;
import java.util.function.BiConsumer;

import io.v6d.modules.basic.columnar.ColumnarData;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.io.*;

import io.v6d.modules.basic.arrow.Arrow;

import lombok.val;

public class RowWritable implements Writable {
    private Writable[] colValues;
    private boolean[] nullIndicators;
    private BiConsumer<Writable, Object>[] setters;

    public RowWritable(Schema schema) {
        this.colValues = new Writable[schema.getFields().size()];
        this.nullIndicators = new boolean[schema.getFields().size()];
        this.setters = new BiConsumer[schema.getFields().size()];
        for (int i = 0; i < schema.getFields().size(); i++) {
            val dtype = schema.getFields().get(i).getFieldType().getType();
            if (Arrow.Type.Boolean.equals(dtype)) {
                this.colValues[i] = new BooleanWritable();
                this.setters[i] = RowWritable::setBool;
            } else if (Arrow.Type.Int.equals(dtype) || Arrow.Type.UInt.equals(dtype)) {
                this.colValues[i] = new IntWritable();
                this.setters[i] = RowWritable::setInt;
            } else if (Arrow.Type.Int64.equals(dtype) || Arrow.Type.UInt64.equals(dtype)) {
                this.colValues[i] = new LongWritable();
                this.setters[i] = RowWritable::setLong;
            } else if (Arrow.Type.Float.equals(dtype)) {
                this.colValues[i] = new FloatWritable();
                this.setters[i] = RowWritable::setFloat;
            } else if (Arrow.Type.Double.equals(dtype)) {
                this.colValues[i] = new DoubleWritable();
                this.setters[i] = RowWritable::setDouble;
            } else if (Arrow.Type.VarChar.equals(dtype)
                    || Arrow.Type.ShortVarChar.equals(dtype)
                    || Arrow.Type.VarBinary.equals(dtype)
                    || Arrow.Type.ShortVarBinary.equals(dtype)) {
                this.colValues[i] = new Text();
                this.setters[i] = RowWritable::setString;
            } else {
                throw new UnsupportedOperationException("Unsupported type: " + dtype);
            }
        }
    }

    public Object[] getValues() {
        return colValues;
    }

    public void setValues(ColumnarData[] columns, int index) {
        for (int i = 0; i < columns.length; i++) {
            setters[i].accept(colValues[i], columns[i].getObject(index));
        }
    }

    @Override
    public void write(DataOutput out) {
        System.out.println("write");
    }

    @Override
    public void readFields(DataInput in) {
        System.out.println("readFields");
    }

    private BooleanWritable makeWritable(boolean value) {
        return new BooleanWritable(value);
    }

    private static void setBool(Writable w, Object value) {
        ((BooleanWritable) w).set((boolean) value);
    }

    private static void setInt(Writable w, Object value) {
        ((IntWritable) w).set((int) value);
    }

    private static void setLong(Writable w, Object value) {
        ((LongWritable) w).set((long) value);
    }

    private static void setFloat(Writable w, Object value) {
        ((FloatWritable) w).set((float) value);
    }

    private static void setDouble(Writable w, Object value) {
        ((DoubleWritable) w).set((double) value);
    }

    private static void setString(Writable w, Object value) {
        ((Text) w).set((String) value);
    }
}

