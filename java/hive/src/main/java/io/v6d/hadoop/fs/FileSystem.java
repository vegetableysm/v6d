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
package io.v6d.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Options.HandleOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VineyardOutputStream extends FSDataOutputStream {

    public VineyardOutputStream() throws IOException {
        super(new DataOutputBuffer(100), null);
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    public String toString() {
        return new String("vineyard");
    }
}
public class FileSystem extends org.apache.hadoop.fs.FileSystem {
    public static final String SCHEME = "vineyard";

    private URI uri = null;
    private static Logger logger = LoggerFactory.getLogger(FileSystem.class);

    public FileSystem() {
        super();
    }

    public FileSystem(final URI uri, final Configuration conf) {
        super();
        System.out.println("=================");
        System.out.println("FileSystem: " + uri);
        System.out.println("=================");
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    protected URI canonicalizeUri(URI uri) {
        return uri;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);

        System.out.println("=================");
        logger.info("Initialize vineyard file system: {}", name);
        System.out.println("Initialize vineyard file system: " + name);
        System.out.println("=================");
        this.uri = name;
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        System.out.println("=================");
        System.out.println("open: " + path.toString());
        System.out.println("=================");
        return null;
    }

    @Override
    public FSDataOutputStream create(
            Path path,
            FsPermission fsPermission,
            boolean b,
            int i,
            short i1,
            long l,
            Progressable progressable)
            throws IOException {
        System.out.println("=================");
        System.out.println("Create:" + path);
        System.out.println("=================");
        return new FSDataOutputStream(new VineyardOutputStream(), null);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable)
            throws IOException {
        System.out.println("=================");
        System.out.println("append:" + path);
        System.out.println("=================");
        return null;
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        System.out.println("=================");
        System.out.println("rename: " + path + " to " + path1);
        System.out.println("=================");
        return true;
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        System.out.println("=================");
        System.out.println("delete: " + path);
        System.out.println("=================");
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        System.out.println("=================");
        System.out.println("listStatus: " + path);
        System.out.println("=================");
        //FileStatus fileStatus = new FileStatus(10, true, 1, 10, 0, path);
        // return null;// new FileStatus[] {fileStatus};
        return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path path) {}

    @Override
    public Path getWorkingDirectory() {
        System.out.println("=================");
        System.out.println("getWorkingDirectory");
        System.out.println("=================");
        return null;// new Path("/");
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        System.out.println("=================");
        System.out.println("mkdirs: " + path);
        System.out.println("=================");
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        System.out.println("=================");
        System.out.println("getFileStatus: " + path.toString());
        System.out.println("=================");
        return new FileStatus(10, true, 1, 10, 0, path);
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        System.out.println("=================");
        System.out.println("getXAttr: " + path);
        System.out.println("Get name:" + name);
        System.out.println("=================");
        return new byte[0];
    }

    // @Override
    // public BlockLocation[] getFileBlockLocations(FileStatus file,
    //   long start, long len) throws IOException {
    //     System.out.println("=================");
    //     System.out.println("getFileBlockLocations: " + file);
    //     System.out.println("=================");
    //     return new BlockLocation[0];
    // }

    // @Override
    // public FsServerDefaults getServerDefaults() throws IOException {
    //     System.out.println("=================");
    //     System.out.println("getServerDefaults: ");
    //     System.out.println("=================");
    //     return null;
    // }
}
