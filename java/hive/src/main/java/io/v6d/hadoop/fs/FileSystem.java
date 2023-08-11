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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Options.HandleOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class MockOutputStream extends FSDataOutputStream {
    public final MockFile file;

    public MockOutputStream(MockFile file) throws IOException {
      super(new DataOutputBuffer(), null);
      this.file = file;
    }

    /**
     * Set the blocks and their location for the file.
     * Must be called after the stream is closed or the block length will be
     * wrong.
     * @param blocks the list of blocks
     */
    public void setBlocks(MockBlock... blocks) {
      file.blocks = blocks;
      int offset = 0;
      int i = 0;
      while (offset < file.length && i < blocks.length) {
        blocks[i].offset = offset;
        blocks[i].length = Math.min(file.length - offset, file.blockSize);
        offset += blocks[i].length;
        i += 1;
      }
    }

    @Override
    public void close() throws IOException {
      System.out.println("=== close ===");
      super.close();
      DataOutputBuffer buf = (DataOutputBuffer) getWrappedStream();
      file.length = buf.getLength();
      file.content = new byte[file.length];
      MockBlock block = new MockBlock("host1");
      block.setLength(file.length);
      setBlocks(block);
      System.arraycopy(buf.getData(), 0, file.content, 0, file.length);
      for (int i = 0; i < buf.getData().length; i++) {
        System.out.println((int)(buf.getData()[i]));
      }
    }

    @Override
    public String toString() {
      return "Out stream to " + file.toString();
    }
  }

class MockBlock {
    int offset;
    int length;
    final String[] hosts;

    public MockBlock(String... hosts) {
      this.hosts = hosts;
    }

    public void setOffset(int offset) {
      this.offset = offset;
    }

    public void setLength(int length) {
      this.length = length;
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("block{offset: ");
      buffer.append(offset);
      buffer.append(", length: ");
      buffer.append(length);
      buffer.append(", hosts: [");
      for(int i=0; i < hosts.length; i++) {
        if (i != 0) {
          buffer.append(", ");
        }
        buffer.append(hosts[i]);
      }
      buffer.append("]}");
      return buffer.toString();
    }
  }

class MockFile {
    public final Path path;
    public int blockSize;
    public int length;
    public MockBlock[] blocks;
    public byte[] content;
    public boolean cannotDelete = false;
    // This is purely for testing convenience; has no bearing on FS operations such as list.
    public boolean isDeleted = false;

    public MockFile(String path, int blockSize, byte[] content,
                    MockBlock... blocks) {
      this.path = new Path(path);
      this.blockSize = blockSize;
      this.blocks = blocks;
      this.content = content;
      this.length = content.length;
      int offset = 0;
      for(MockBlock block: blocks) {
        block.offset = offset;
        block.length = Math.min(length - offset, blockSize);
        offset += block.length;
      }
    }

    @Override
    public int hashCode() {
      return path.hashCode() + 31 * length;
    }

    @Override
    public boolean equals(final Object obj) {
      if (!(obj instanceof MockFile)) { return false; }
      return ((MockFile) obj).path.equals(this.path) && ((MockFile) obj).length == this.length;
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("mockFile{path: ");
      buffer.append(path.toString());
      buffer.append(", blkSize: ");
      buffer.append(blockSize);
      buffer.append(", len: ");
      buffer.append(length);
      buffer.append(", blocks: [");
      for(int i=0; i < blocks.length; i++) {
        if (i != 0) {
          buffer.append(", ");
        }
        buffer.append(blocks[i]);
      }
      buffer.append("]}");
      return buffer.toString();
    }
  }
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
public class FileSystem extends RawLocalFileSystem {
    public static final String SCHEME = "vineyard";

    private URI uri = URI.create(SCHEME + ":///");
    private static Logger logger = LoggerFactory.getLogger(FileSystem.class);

    final List<MockFile> files = new ArrayList<MockFile>();
    final Map<MockFile, FileStatus> fileStatusMap = new HashMap<>();
    Path workingDir = new Path("/");
    public FileSystem() {
        super();
    }

    public FileSystem(final URI uri, final Configuration conf) {
        super();
        System.out.println("=================");
        System.out.println("VineyardFileSystem: " + uri);
        System.out.println("=================");
    }

    @Override
    public String getScheme() {
        System.out.println("=================");
        System.out.println("getScheme: " + SCHEME);
        System.out.println("=================");
        return SCHEME;
    }

    @Override
    public URI getUri() {
        System.out.println("=================");
        System.out.println("getUri: " + uri);
        System.out.println("=================");
        // return uri;
        return super.getUri();
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value,
        EnumSet<XAttrSetFlag> flag) throws IOException {
        System.out.println("=================");
        System.out.println("setXAttr: " + path.toString());
        System.out.println("=================");
    }

    @Override
    protected URI canonicalizeUri(URI uri) {
        System.out.println("=================");
        System.out.println("canonicalizeUri: " + uri);
        System.out.println("=================");
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
        // HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "vineyard:///");
        super.initialize(name, conf);
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        System.out.println("=================");
        System.out.println("open: " + path.toString());
        System.out.println("=================");
        Path p = new Path(path.toString().replace("vineyard:", "file:"));
        return super.open(p, i);
    }

      public MockFile findFile(Path path) {
        for (MockFile file: files) {
            if (file.path.equals(path)) {
                System.out.println("find");
                return file;
            }
        }
    // for (MockFile file: globalFiles) {
    //   if (file.path.equals(path)) {
    //     return file;
    //   }
    // }
    return null;
  }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission,
                                    boolean overwrite, int bufferSize,
                                    short replication, long blockSize,
                                    Progressable progressable
                                    ) throws IOException {
        System.out.println("=================");
        System.out.println("create: " + path.toString());
        System.out.println("=================");
        Path p = new Path(path.toString().replace("vineyard:", "file:"));
        return super.create(p, fsPermission, overwrite, bufferSize, replication, blockSize, progressable);
        // MockFile file = findFile(path);
        // if (file == null) {
        //     file = new MockFile(path.toString(), (int) blockSize, new byte[0]);
        //     files.add(file);
        // }
        // return new MockOutputStream(file);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable)
            throws IOException {
        System.out.println("=================");
        System.out.println("append:" + path);
        System.out.println("=================");
        Path p = new Path(path.toString().replace("vineyard:", "file:"));
        return super.append(p, i, progressable);
        // return null;
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        System.out.println("=================");
        System.out.println("rename: " + path + " to " + path1);
        System.out.println("=================");
        Path p = new Path(path.toString().replace("vineyard:", "file:"));
        return super.rename(p, path1);
        // return true;
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        System.out.println("=================");
        System.out.println("delete: " + path);
        System.out.println("=================");
        Path p = new Path(path.toString().replace("vineyard:", "file:"));
        return super.delete(p, b);
        // return true;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        System.out.println("=================");
        System.out.println("listStatus: " + path);
        System.out.println("=================");
        //FileStatus fileStatus = new FileStatus(10, true, 1, 10, 0, path);
        // return null;// new FileStatus[] {fileStatus};
        Path p = new Path(path.toString().replace("vineyard:", "file:"));
        return super.listStatus(p);
        // return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path path) {
        System.out.println("=================");
        System.out.println("setWorkingDirectory: " + path);
        System.out.println("=================");
        workingDir = path;
        super.setWorkingDirectory(path);
    }

    @Override
    public Path getWorkingDirectory() {
        System.out.println("=================");
        System.out.println("getWorkingDirectory");
        System.out.println("=================");
        return workingDir;// new Path("/");
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        System.out.println("=================");
        System.out.println("mkdirs: " + path);
        System.out.println("=================");
        Path p = new Path(path.toString().replace("vineyard:", "file:"));
        return super.mkdirs(p, fsPermission);
        // return true;
    }

    private FileStatus createStatus(MockFile file) {
        if (fileStatusMap.containsKey(file)) {
            return fileStatusMap.get(file);
        }
        FileStatus fileStatus = new FileStatus(file.length, false, 1, file.blockSize, 0, 0,
            new FsPermission((short) 777), null, null,
            file.path);
        fileStatusMap.put(file, fileStatus);
        return fileStatus;
    }

    private FileStatus createDirectory(Path dir) {
        return new FileStatus(1, true, 0, 1, 0, 0,
            new FsPermission((short) 777), null, null, dir);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        System.out.println("=================");
        System.out.println("getFileStatus: " + path.toString());
        System.out.println("=================");
        Path p = new Path(path.toString().replace("vineyard:", "file:"));
        return super.getFileLinkStatus(p);
        // // return new FileStatus(10, true, 1, 10, 0, path);
        // // String pathnameAsDir = path.toString() + "/";
        // if (path.toString().equals("vineyard:/opt/hive/data/warehouse/hive_example")) {
        //     return new FileStatus(1, true, 0, 1, 0, 0,
        //     new FsPermission((short) 777), null, null, path);
        // }
        // MockFile file = findFile(path);
        // if (file != null)
        //     return createStatus(file);
        // // for (MockFile dir : files) {
        //     // if (dir.path.toString().startsWith(pathnameAsDir)) {
        // return createDirectory(path);
        //     // }
        // // }
        // // throw new FileNotFoundException("File " + path + " does not exist");
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
