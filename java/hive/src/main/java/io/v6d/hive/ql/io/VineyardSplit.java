package io.v6d.hive.ql.io;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;

public class VineyardSplit extends FileSplit {
    String customPath;
    int batchStartIndex;
    int batchSize;

    protected VineyardSplit() {
        super();
    }

    public VineyardSplit(Path file, long start, long length, JobConf conf) {
        super(file, start, length, (String[]) null);
    }

    public VineyardSplit(Path file, long start, long length, String[] hosts) {
        super(file, start, length, hosts);
    }

    @Override
    public Path getPath() {
        System.out.println("getPath");
        return super.getPath();
    }

    @Override
    public long getStart() {
        return super.getStart();
    }

    @Override
    public long getLength() {
        return super.getLength();
    }

    @Override
    public String[] getLocations() throws IOException {
        System.out.println("getLocations");
        return new String[0];
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        System.out.println("readFields");
        super.readFields(in);
        batchStartIndex = in.readInt();
        batchSize = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        System.out.println("write");
        super.write(out);
        out.writeInt(batchStartIndex);
        out.writeInt(batchSize);
    }

    public void setBatch(int batchStartIndex, int batchSize) {
        this.batchStartIndex = batchStartIndex;
        this.batchSize = batchSize;
    }

    public int getBatchStartIndex() {
        return batchStartIndex;
    }

    public int getBatchSize() {
        return batchSize;
    }
}