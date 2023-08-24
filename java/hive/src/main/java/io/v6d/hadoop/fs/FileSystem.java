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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Stopwatch;
import com.google.common.base.StopwatchContext;
import io.v6d.hive.ql.io.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.*;

import io.v6d.core.client.ds.ObjectFactory;
import io.v6d.core.client.ds.ObjectMeta;
import io.v6d.core.common.util.ObjectID;
import io.v6d.core.common.util.VineyardException;
import io.v6d.modules.basic.arrow.SchemaBuilder;
import io.v6d.modules.basic.arrow.Table;
import io.v6d.modules.basic.arrow.TableBuilder;
import io.v6d.hive.ql.io.CloseableReentrantLock;

class VineyardOutputStream extends FSDataOutputStream {
    private File file;
    private byte[] content = new byte[0];
    private int pos = 0;

    public VineyardOutputStream(File file) throws IOException {
        super(new DataOutputBuffer(100), null);
        this.file = file;
    }

    @Override
    public void close() throws IOException {
        val watch = StopwatchContext.create();
        super.close();
        String contentStr = new String(content, StandardCharsets.UTF_8);
        Context.println("Change table name from:" + contentStr +
                " to: " + file.getFileStatus().getPath().toString());
        String tableName = null;
        if (contentStr.startsWith(FileSystem.SCHEME + ":")) {
            tableName = contentStr.replaceAll("//", "/");
            if (tableName.contains("/_tmp.")) {
                tableName = tableName.substring(0, tableName.lastIndexOf("/")).replaceAll("/", "#");
            }
        }
        Context.println("Closing: table name: " + tableName);
        if (tableName == null || tableName.length() == 0) {
            return;
        }

        String newTableName = file.getFileStatus().getPath().toString().replaceAll("//", "/");
        if (newTableName.length() == 0 || newTableName.lastIndexOf("/") < -1) {
            return;
        }
        newTableName = newTableName.substring(0, newTableName.lastIndexOf("/")).replaceAll("/", "#");
        Context.println("Closing: new table name:" + newTableName);

        try {
            val client = Context.getClient();
            ObjectID oldTableID = client.getName(tableName, false);
            if (oldTableID == null) {
                Context.println("Table not exist.");
            }
            client.putName(oldTableID, newTableName);
            Context.println("update the table name for " + oldTableID + " from " + tableName + " to " + newTableName);
        } catch (VineyardException e) {
            Context.println("failed to inspect the tablename: " + tableName + ": " + e);
        }
        Context.println("vineyard output stream close() uses " + watch.stop());
    }

    @Override
    public String toString() {
        return new String("vineyard");
    }

    @Override
    public void write(int b) throws IOException {
        Context.println("should not call this function.");
        throw new IOException("should not call this function.");
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        Context.println("write:" + new String(b, StandardCharsets.UTF_8) + " off:" + off + " len:" + len + " pos:" + pos);
        Context.println("current file:" + file.getFileStatus().getPath());
        content = new byte[pos + len];
        for (int i = 0; i < len; i++) {
            content[pos++] = b[off + i];
            if (pos > content.length) {
                throw new IOException("content too long");
            }
        }
        Context.println("content: " + new String(content, StandardCharsets.UTF_8));
    }
}

class VineyardInputStream extends FSInputStream {
    int offset = 0;
    byte[] content;

    public VineyardInputStream(File file) throws IOException {
        content = file.getFileStatus().getPath().toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void seek(long offset) throws IOException {
        this.offset = (int) offset;
    }

    @Override
    public long getPos() throws IOException {
        return offset;
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        return false;
    }

    @Override
    public int read() throws IOException {
        if (offset < content.length) {
            return this.content[offset++] & 0xff;
        }
        return -1;
    }
}

class File {
    boolean isDir;
    FileStatus fileStatus;

    public File(boolean isDir, FileStatus fileStatus) {
        this.isDir = isDir;
        this.fileStatus = fileStatus;
    }

    public boolean getIsDir() {
        return isDir;
    }

    public FileStatus getFileStatus() {
        return fileStatus;
    }

    public void setFileStatus(FileStatus fileStatus) {
        this.fileStatus = fileStatus;
    }
}

public class FileSystem extends org.apache.hadoop.fs.FileSystem {
    public static final String SCHEME = "vineyard";

    private URI uri = URI.create(SCHEME + ":/");
    private static Logger logger = LoggerFactory.getLogger(FileSystem.class);

    final static Map<String, File>fileMap = new HashMap<String, File>();
    final static CloseableReentrantLock lock = new CloseableReentrantLock();
    private Configuration conf = null;
 
    Path workingDir = new Path("/");
    public FileSystem() {
        super();
    }

    public void printAllFile ()
    {
        Context.println("print all file:");
        Context.println("Isdir   Path");
        for (String key : fileMap.keySet()) {
            Context.println(fileMap.get(key).getIsDir() +  " path:" + fileMap.get(key).getFileStatus().getPath().toString());
        }
    }

    @Override
    public String getScheme() {
        // Context.println("=================");
        // Context.println("getScheme: " + SCHEME);
        // Context.println("=================");
        return SCHEME;
    }

    @Override
    public URI getUri() {
        // Context.println("=================");
        // Context.println("getUri: " + uri);
        // Context.println("=================");
        return uri;
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value,
        EnumSet<XAttrSetFlag> flag) throws IOException {
        // Context.println("=================");
        // Context.println("setXAttr: " + path.toString());
        // Context.println("=================");
    }

    @Override
    protected URI canonicalizeUri(URI uri) {
        // Context.println("=================");
        // Context.println("canonicalizeUri: " + uri);
        // Context.println("=================");
        return uri;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        this.conf = conf;

        // Context.println("=================");
        // Context.println("Initialize vineyard file system: " + name);
        this.uri = name;
        mkdirs(new Path(uri.toString().replaceAll("///", "/")));
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        // Context.println("=================");
        // Context.println("open: " + path.toString());
        // Context.println("=================");
        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");
        if (!fileMap.containsKey(pathStr)) {
            Context.println("File not exists!");
            throw new FileNotFoundException();
        }
        File file = fileMap.get(pathStr);
        return new FSDataInputStream(new VineyardInputStream(file));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission,
                                    boolean overwrite, int bufferSize,
                                    short replication, long blockSize,
                                    Progressable progressable
                                    ) throws IOException {
        try (val lock = this.lock.open()) {
            return createInternal(path, fsPermission, overwrite, bufferSize, replication, blockSize, progressable);
        }
    }

    private FSDataOutputStream createInternal(Path path, FsPermission fsPermission,
                                              boolean overwrite, int bufferSize,
                                              short replication, long blockSize,
                                              Progressable progressable) throws IOException {
        // Context.println("=================");
        // Context.println("create: " + path.toString());
        
        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");
        if (fileMap.containsKey(pathStr)) {
            Context.println("File exists, delete!");
            fileMap.remove(pathStr);
        }
        // Context.println("create!");
        File f = new File(false, new FileStatus(0, false, 1, 1, 0, 0,
        new FsPermission((short) 777), null, null,
        path));
        fileMap.put(pathStr, f);
        
        String[] pathList = pathStr.split("/");
        String dir = "";
        for (int i = 0; i < pathList.length; i++) {
            if (pathList[i].length() == 0) {
                continue;
            }
            dir += "/" + pathList[i];
            if (fileMap.containsKey(dir)) {
                continue;
            }
            
            File dirFile = new File(true, new FileStatus(1, true, 1, 1, 0, 0,
            new FsPermission((short) 777), null, null,
            new Path(SCHEME + ":/" + dir)));
            fileMap.put(dir, dirFile);
        }
        FSDataOutputStream result = new FSDataOutputStream(new VineyardOutputStream(f), null);
        // printAllFile ();
        // Context.println("=================");
        return result;
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable)
            throws IOException {
        // Context.println("=================");
        // Context.println("append:" + path);
        // Context.println("=================");
        return null;
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        try (val lock = this.lock.open()) {
            return this.deleteInternal(path, b);
        }
    }

    private boolean deleteInternal(Path path, boolean b) throws IOException {
        // Context.println("=================");
        // Context.println("delete: " + path);
        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");
        List<String> deleteList = new ArrayList<String>();
        for (String key : fileMap.keySet()) {
            if (key.startsWith(pathStr) && (key.length() == pathStr.length() || key.substring(pathStr.length()).charAt(0) == '/')) {
                deleteList.add(key);
            }
        }
        for (int i = 0; i < deleteList.size(); i++) {
            fileMap.remove(deleteList.get(i));
        }
        // printAllFile ();
        // Context.println("=================");
        return true;
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

    public boolean renameInternal(Path path, Path path1) throws IOException {
        // now we create the new file and delete old file to simulate rename
        // Context.println("=================");
        // Context.println("rename: " + path.toString() + " to " + path1.toString());

        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        String pathStr1 = path1.toString().substring(path1.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");
        pathStr1 = pathStr1.replaceAll("//", "/");
        List<String> deleteList = new ArrayList<String>();
        List<String> addList = new ArrayList<String>();
        
        for (String key : fileMap.keySet()) {
            if (key.startsWith(pathStr) && (key.length() == pathStr.length() || key.substring(pathStr.length()).charAt(0) == '/')) {
                // Context.println("find:" + key);
                deleteList.add(key);
                addList.add(pathStr1 + key.substring(pathStr.length()));
                // Context.println("prepare change :" + key + " to :" + pathStr1 + key.substring(pathStr.length()));
            }
        }
        
        //delete old file and create new file
        for (int i = 0; i < deleteList.size(); i++) {
            File file = fileMap.get(deleteList.get(i));
            if (file.getIsDir()) {
                mkdirsInternal(new Path("vineyard:" + addList.get(i)), new FsPermission((short) 777));
                continue;
            }
            createInternal(new Path("vineyard:" + addList.get(i)), null, false, 0, (short) 0, 0, null);
            // Context.println("rename create done");

            String tableName = file.getFileStatus().getPath().toString().replaceAll("/", "#");
            tableName = tableName.substring(0, tableName.lastIndexOf("#"));
            Context.println("rename: tableName: " + tableName);
            if (tableName.length() == 0) {
                continue;
            }
            String newTableName = fileMap.get(addList.get(i)).getFileStatus().getPath().toString().replaceAll("/", "#");
            newTableName = newTableName.substring(0, newTableName.lastIndexOf("#"));
            Context.println("New table name:" + newTableName);

            val client = Context.getClient();
            ObjectID objectID;
            Table table1;
            try {
                objectID = client.getName(tableName, false);
                table1 = (Table) ObjectFactory.getFactory().resolve(client.getMetaData(objectID));
            } catch (VineyardException e) {
                Context.println("Table not exists: ignore as might already be renamed");
                continue;
            }

            // check if table exists
            Table table2 = null;
            try {
                ObjectID oldTableID = client.getName(newTableName, false);
                table2 = (Table) ObjectFactory.getFactory().resolve(client.getMetaData(oldTableID));
            } catch (VineyardException e) {
                Context.println("Table not exist: " + e);
            }

            ObjectID mergedTableID = objectID;
            if (table2 != null && table2.getBatches().size() > 0) {
                // Context.println("Table already exists, merge!");
                SchemaBuilder schemaBuilder = SchemaBuilder.fromSchema(table1.getSchema());
                TableBuilder tableBuilder = new TableBuilder(client, schemaBuilder);
                for (int j = 0; j < table1.getBatches().size(); j++) {
                    tableBuilder.addBatch(table1.getBatch(j));
                }
                for (int j = 0; j < table2.getBatches().size(); j++) {
                    tableBuilder.addBatch(table2.getBatch(j));
                }
                ObjectMeta meta = tableBuilder.seal(client);
                // Context.println("Merged table id in vineyard:" + meta.getId().value());
                client.persist(meta.getId());
                // Context.println("Table persisted, name:" + newTableName);
                // Context.println("record batch size:" + tableBuilder.getBatchSize());
                mergedTableID = meta.getId();
            }
            Context.println("Putting new table name: " + newTableName);
            client.putName(mergedTableID, newTableName);
            Context.println("Dropping the old table name: " + tableName);
            client.dropName(tableName);
        }

        this.deleteInternal(path, false);
        // Context.println("rename done");

        // Context.println("return true 2");
        // Context.println("=================");
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        // Context.println("=================");
        // Context.println("listStatus: " + path);
        // printAllFile ();
        
        List<FileStatus> result = new ArrayList<FileStatus>();
        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");

        // Context.println("pathStr:" + pathStr);
        try (val lock = this.lock.open()) {
            for (String key : fileMap.keySet()) {
                // Context.println("key:" + key);
                if (key.startsWith(pathStr) && key.length() > pathStr.length() && key.substring(pathStr.length()).charAt(0) == '/') {
                    if (key.substring(pathStr.length()).split("/").length == 2) {
                        // Context.println("find key:" + key);
                        result.add(fileMap.get(key).getFileStatus());
                    }
                }
            }
        }
        // Context.println("Find num:"  + result.size());
        // Context.println("=================");
        return result.toArray(new FileStatus[result.size()]);
    }

    @Override
    public void setWorkingDirectory(Path path) {
        // Context.println("=================");
        // Context.println("setWorkingDirectory: " + path);
        // Context.println("=================");
        workingDir = path;
    }

    @Override
    public Path getWorkingDirectory() {
        // Context.println("=================");
        // Context.println("getWorkingDirectory");
        // Context.println("=================");
        return workingDir;// new Path("/");
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        try (val lock = this.lock.open()) {
            return this.mkdirsInternal(path, fsPermission);
        }
    }


    private boolean mkdirsInternal(Path path, FsPermission fsPermission) throws IOException {
        // Context.println("=================");
        // Context.println("mkdirs: " + path);

        // create dir with path and all parent dir
        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");
        if (!fileMap.containsKey(pathStr)) {
            // Context.println("Dir not exists, create!");
            File file = new File(true, new FileStatus(1, true, 1, 1, 0, 0,
            new FsPermission((short) 777), null, null,
            path));
            // Context.println("Dir name:" + pathStr);
            fileMap.put(pathStr, file);
            
            String[] pathList = pathStr.split("/");
            String dir = "";
            for (int i = 0; i < pathList.length; i++) {
                if (pathList[i].length() == 0) {
                    continue;
                }
                dir += "/" + pathList[i];
                if (fileMap.containsKey(dir)) {
                    continue;
                }
                file = new File(true, new FileStatus(1, true, 1, 1, 0, 0,
                new FsPermission((short) 777), null, null,
                new Path(SCHEME + ":/" + dir)));
                fileMap.put(dir, file);
            }
            
            // printAllFile ();
        }
        // Context.println("=================");
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        try (val lock = this.lock.open()) {
            return this.getFileStatusInternal(path);
        }
    }

    public FileStatus getFileStatusInternal(Path path) throws IOException {
        // Context.println("=================");
        // Context.println("getFileStatus: " + path.toString());
        // Context.println("=================");

        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");

        FileStatus result = null;
        if (fileMap.containsKey(pathStr)) {
            // Context.println("File exists, return!");
            result =  fileMap.get(pathStr).getFileStatus();
            // printAllFile ();
            return result;
        }
        int stageDirIndex = pathStr.indexOf(HiveConf.getVar(conf, HiveConf.ConfVars.STAGINGDIR));
        if (stageDirIndex >= 0 && pathStr.substring(stageDirIndex).split("/").length == 1) {
            // Context.println("Staging dir not exists, create file as dir!");
            File file = new File(true, new FileStatus(1, true, 1, 1, 0, 0,
                new FsPermission((short) 777), null, null,
                path));
            fileMap.put(pathStr, file);
            // printAllFile ();
            return file.getFileStatus();
        }
        // Context.println("return null");
        throw new FileNotFoundException();
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        // Context.println("=================");
        // Context.println("getXAttr: " + path);
        // Context.println("Get name:" + name);
        // Context.println("=================");
        return new byte[0];
    }

    @Override
    public void copyFromLocalFile(Path src, Path dst)
        throws IOException {
        Context.println("=================");
        Context.println("copyFromLocalFile1: " + src + " to " + dst);
        Context.println("=================");
    }

    @Override
    public void moveFromLocalFile(Path[] srcs, Path dst)
      throws IOException {
        Context.println("=================");
        Context.println("moveFromLocalFile2: " + srcs + " to " + dst);
        Context.println("=================");
    }
    @Override
    public void moveFromLocalFile(Path src, Path dst)
      throws IOException {
        Context.println("=================");
        Context.println("moveFromLocalFile3: " + src + " to " + dst);
        Context.println("=================");
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
        Context.println("=================");
        Context.println("copyFromLocalFile4: " + src + " to " + dst);
        Context.println("=================");
    }
    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path[] srcs, Path dst)
    throws IOException {
        Context.println("=================");
        Context.println("copyFromLocalFile5: " + srcs + " to " + dst);
        Context.println("=================");
    }
  
    @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path src, Path dst)
    throws IOException {
        Context.println("=================");
        Context.println("copyFromLocalFile6: " + src + " to " + dst);
        Context.println("=================");
    }

  @Override
  public void copyToLocalFile(Path src, Path dst) throws IOException {
        Context.println("=================");
        Context.println("copyToLocalFile7: " + src + " to " + dst);
        Context.println("=================");
  }


  @Override
  public void moveToLocalFile(Path src, Path dst) throws IOException {
        Context.println("=================");
        Context.println("moveToLocalFile8: " + src + " to " + dst);
        Context.println("=================");
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
        Context.println("=================");
        Context.println("copyToLocalFile9: " + src + " to " + dst);
        Context.println("=================");
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst,
      boolean useRawLocalFileSystem) throws IOException {
        Context.println("=================");
        Context.println("copyToLocalFile10: " + src + " to " + dst);
        Context.println("=================");
  }
}