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

import com.google.common.base.StopwatchContext;
import com.google.common.jimfs.Jimfs;
import io.v6d.core.client.Context;
import io.v6d.hive.ql.io.CloseableReentrantLock;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Queue;
import lombok.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VineyardOutputStream extends FSDataOutputStream {
    private FileChannel channel;

    public VineyardOutputStream(FileChannel channel) throws IOException {
        super(new DataOutputBuffer(), null);
        this.channel = channel;
    }

    @Override
    public void close() throws IOException {
        this.channel.close();
    }

    @Override
    public String toString() {
        return "vineyard";
    }

    @Override
    public void write(int b) throws IOException {
        throw new UnsupportedOperationException("should not call this function.");
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        channel.write(java.nio.ByteBuffer.wrap(b, off, len));
    }
}

class VineyardInputStream extends FSInputStream {
    private FileChannel channel;

    public VineyardInputStream(FileChannel channel) throws IOException {
        this.channel = channel;
    }

    @Override
    public void seek(long offset) throws IOException {
        throw new UnsupportedOperationException("Vineyard input stream not support seek.");
    }

    @Override
    public long getPos() throws IOException {
        throw new UnsupportedOperationException("Vineyard input stream not support getPos.");
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        throw new UnsupportedOperationException(
                "Vineyard input stream not support seekToNewSource.");
    }

    @Override
    public int read() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        int ret = channel.read(buffer);
        if (ret <= 0) {
            return -1;
        }
        return buffer.get(0);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}

public class FileSystem extends org.apache.hadoop.fs.FileSystem {
    public static final String SCHEME = "vineyard";

    private URI uri = URI.create(SCHEME + ":/");
    private static Logger logger = LoggerFactory.getLogger(FileSystem.class);

    static final CloseableReentrantLock lock = new CloseableReentrantLock();
    private Configuration conf = null;

    static java.nio.file.FileSystem jimfs = null;
    static boolean enablePrintAllFile = false;

    Path workingDir = new Path("vineyard:/");

    public FileSystem() {
        super();
    }

    public static void printAllFile(java.nio.file.Path root, java.nio.file.FileSystem fs)
            throws IOException {
        DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(root);
        Queue<java.nio.file.Path> queue = new java.util.LinkedList<java.nio.file.Path>();
        for (java.nio.file.Path p : stream) {
            queue.add(p);
        }
        while (!queue.isEmpty()) {
            java.nio.file.Path p = queue.poll();
            Context.println(p.toString());
            if (Files.isDirectory(p)) {
                DirectoryStream<java.nio.file.Path> stream1 = Files.newDirectoryStream(p);
                for (java.nio.file.Path p1 : stream1) {
                    queue.add(p1);
                }
                stream1.close();
            }
        }
        stream.close();
    }

    public static void printAllFile() throws IOException {
        if (enablePrintAllFile) {
            printAllFile(jimfs.getPath("/"), jimfs);
        }
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
    public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag)
            throws IOException {}

    @Override
    protected URI canonicalizeUri(URI uri) {
        return uri;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        this.conf = conf;
        this.uri = name;
        try {
            if (jimfs == null) {
                jimfs = Jimfs.newFileSystem(com.google.common.jimfs.Configuration.unix());
            }
        } catch (Exception e) {
            Context.println("Exception: " + e.getMessage());
            throw e;
        }
        mkdirs(new Path(uri.toString().replaceAll("///", "/")));
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        FileChannel channel =
                FileChannel.open(
                        jimfs.getPath(path.toString().substring(path.toString().indexOf(":") + 1)),
                        StandardOpenOption.READ);
        return new FSDataInputStream(new VineyardInputStream(channel));
    }

    @Override
    public FSDataOutputStream create(
            Path path,
            FsPermission fsPermission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progressable)
            throws IOException {
        try (val lock = this.lock.open()) {
            return createInternal(
                    path,
                    fsPermission,
                    overwrite,
                    bufferSize,
                    replication,
                    blockSize,
                    progressable);
        }
    }

    private FSDataOutputStream createInternal(
            Path path,
            FsPermission fsPermission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progressable)
            throws IOException {
        java.nio.file.Path nioFilePath =
                jimfs.getPath(path.toString().substring(path.toString().indexOf(":") + 1));
        java.nio.file.Path nioParentDirPath = nioFilePath.getParent();
        if (nioParentDirPath != null) {
            Files.createDirectories(nioParentDirPath);
        }
        Files.createFile(nioFilePath);
        FileChannel channel = FileChannel.open(nioFilePath, StandardOpenOption.WRITE);
        printAllFile();
        return new FSDataOutputStream(new VineyardOutputStream(channel), null);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable)
            throws IOException {
        return null;
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        try (val lock = this.lock.open()) {
            return this.deleteInternal(
                    jimfs.getPath(path.toString().substring(path.toString().indexOf(":") + 1)), b);
        }
    }

    private boolean deleteInternal(java.nio.file.Path path, boolean b) throws IOException {
        java.nio.file.Path nioFilePath =
                jimfs.getPath(path.toString().substring(path.toString().indexOf(":") + 1));

        // check if the path is a directory
        if (Files.isDirectory(nioFilePath)) {
            DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(nioFilePath);
            for (java.nio.file.Path p : stream) {
                deleteInternal(p, b);
            }
            stream.close();
        }
        Files.deleteIfExists(nioFilePath);

        printAllFile();
        return false;
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        try (val lock = this.lock.open()) {
            val watch = StopwatchContext.create();
            val renamed = this.renameInternal(path, path1);
            Context.println("filesystem rename uses: " + watch.stop());
            return renamed;
        }
    }

    private void mergeFile(java.nio.file.Path path, java.nio.file.Path path1) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        FileChannel channel1 = FileChannel.open(path1, StandardOpenOption.APPEND);
        channel.transferTo(0, channel.size(), channel1);
        channel.close();
        channel1.close();

        channel1 = FileChannel.open(path1, StandardOpenOption.READ);
        ByteBuffer bytes = ByteBuffer.allocate(255);
        int len = channel1.read(bytes);
        String objectIDStr = new String(bytes.array(), 0, len, StandardCharsets.UTF_8);
        System.out.println();
    }

    public boolean renameInternal(Path path, Path path1) throws IOException {
        // now we create new file and delete old file to simulate rename
        java.nio.file.Path nioFilePath =
                jimfs.getPath(path.toString().substring(path.toString().indexOf(":") + 1));
        java.nio.file.Path nioFilePath1 =
                jimfs.getPath(path1.toString().substring(path1.toString().indexOf(":") + 1));
        java.nio.file.Path nioParentDirPath = nioFilePath.getParent();
        Files.createDirectories(nioParentDirPath);
        if (Files.exists(nioFilePath1)) {
            mergeFile(nioFilePath, nioFilePath1);
            Files.delete(nioFilePath);
        } else {
            Files.move(nioFilePath, nioFilePath1);
        }
        printAllFile();

        return true;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        try (val lock = this.lock.open()) {
            java.nio.file.Path nioFilePath =
                    jimfs.getPath(path.toString().substring(path.toString().indexOf(":") + 1));
            if (Files.isDirectory(nioFilePath)) {
                DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(nioFilePath);
                for (java.nio.file.Path p : stream) {
                    result.add(
                            new FileStatus(
                                    Files.size(p),
                                    Files.isDirectory(p),
                                    1,
                                    1,
                                    0,
                                    0,
                                    new FsPermission((short) 777),
                                    null,
                                    null,
                                    new Path(SCHEME + ":/" + p.toString())));
                }
                stream.close();
            }
        }
        printAllFile();
        return result.toArray(new FileStatus[result.size()]);
    }

    @Override
    public void setWorkingDirectory(Path path) {
        workingDir = path;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        try (val lock = this.lock.open()) {
            return this.mkdirsInternal(path, fsPermission);
        }
    }

    private boolean mkdirsInternal(Path path, FsPermission fsPermission) throws IOException {
        java.nio.file.Path nioDirPath =
                jimfs.getPath(path.toString().substring(path.toString().indexOf(":") + 1));
        Files.createDirectories(nioDirPath);
        printAllFile();
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        try (val lock = this.lock.open()) {
            return this.getFileStatusInternal(path);
        }
    }

    public FileStatus getFileStatusInternal(Path path) throws IOException {
        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        java.nio.file.Path nioFilePath = jimfs.getPath(pathStr);
        if (Files.exists(nioFilePath)) {
            printAllFile();
            return new FileStatus(
                    Files.size(nioFilePath),
                    Files.isDirectory(nioFilePath),
                    1,
                    1,
                    0,
                    0,
                    new FsPermission((short) 777),
                    null,
                    null,
                    path);
        }

        pathStr = pathStr.replaceAll("//", "/");
        int stageDirIndex = pathStr.indexOf(HiveConf.getVar(conf, HiveConf.ConfVars.STAGINGDIR));
        if (stageDirIndex >= 0 && pathStr.substring(stageDirIndex).split("/").length == 1) {
            Context.println("Staging dir not exists, create file as dir!");
            Files.createDirectories(nioFilePath);
            printAllFile();
            return new FileStatus(
                    1, true, 1, 1, 0, 0, new FsPermission((short) 777), null, null, path);
        }
        printAllFile();
        throw new FileNotFoundException();
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        throw new UnsupportedOperationException("Vineyard file system not support getXAttr.");
    }

    @Override
    public void moveFromLocalFile(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException(
                "Vineyard file system not support moveFromLocalFile.");
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException(
                "Vineyard file system not support copyFromLocalFile.");
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst)
            throws IOException {
        throw new UnsupportedOperationException(
                "Vineyard file system not support copyFromLocalFile.");
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
            throws IOException {
        throw new UnsupportedOperationException(
                "Vineyard file system not support copyFromLocalFile.");
    }

    @Override
    public void copyToLocalFile(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException(
                "Vineyard file system not support copyToLocalFile.");
    }

    @Override
    public void moveToLocalFile(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException(
                "Vineyard file system not support moveToLocalFile.");
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException(
                "Vineyard file system not support copyToLocalFile.");
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem)
            throws IOException {
        throw new UnsupportedOperationException(
                "Vineyard file system not support copyToLocalFile.");
    }
}
