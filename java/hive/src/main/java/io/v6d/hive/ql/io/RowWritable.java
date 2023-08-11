package io.v6d.hive.ql.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.io.*;

public class RowWritable implements Writable {
    private Schema schema;
    private Object[] colValues;
    // private final boolean[] nullIndicators;
    private int columnCount = 0;

    public void constructRow(Schema schema, VectorSchemaRoot root, int index) {
        this.schema = schema;
        this.colValues = new Writable[schema.getFields().size()];
        // this.nullIndicators = new boolean[schema.getFields().size()];
        this.columnCount = schema.getFields().size();
        for (int i = 0; i < columnCount; i++) {
            colValues[i] = makeWritable((root.getFieldVectors().get(i).getObject(index)));
        }
    }

    public RowWritable() {

    }

    public Object[] getValues() {
        return colValues;
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
        System.out.println("makeWritable(boolean value) called");
        return new BooleanWritable(value);
    }

    private IntWritable makeWritable(int value) {
        System.out.println("makeWritable(int value) called");
        return new IntWritable(value);
    }

    private LongWritable makeWritable(long value) {
        System.out.println("makeWritable(long value) called");
        return new LongWritable(value);
    }

    private FloatWritable makeWritable(float value) {
        System.out.println("makeWritable(float value) called");
        return new FloatWritable(value);
    }

    private DoubleWritable makeWritable(double value) {
        System.out.println("makeWritable(double value) called");
        return new DoubleWritable(value);
    }

    private Text makeWritable(String value) {
        System.out.println("makeWritable(String value) called");
        return new Text(value);
    }

    private Object makeWritable(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return makeWritable((boolean) value);
        }
        if (value instanceof Integer) {
            return makeWritable((int) value);
        }
        if (value instanceof Long) {
            return makeWritable((long) value);
        }
        if (value instanceof Float) {
            return makeWritable((float) value);
        }
        if (value instanceof Double) {
            return makeWritable((double) value);
        }
        if (value instanceof String) {
            return makeWritable((String) value);
        }
        if (value instanceof org.apache.arrow.vector.util.Text) {
            return new Text(((org.apache.arrow.vector.util.Text) value).toString());
        }
        return value;
    }
}
