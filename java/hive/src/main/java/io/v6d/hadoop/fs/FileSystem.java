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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.v6d.core.client.IPCClient;
import io.v6d.core.common.util.ObjectID;
import io.v6d.core.common.util.VineyardException;


class VineyardOutputStream extends FSDataOutputStream {
    File file;
    byte[] content = new byte[255];
    int pos = 0;

    public VineyardOutputStream(File file) throws IOException {
        super(new DataOutputBuffer(100), null);
        this.file = file;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    public String toString() {
        return new String("vineyard");
    }

    @Override
    public void write(int b) throws IOException {
        System.out.println("write:" + b);
        content[pos++] = (byte) (b & 0xff);
        System.out.println("out file:" + this.file.getFileStatus().getPath().toString());
        System.out.println("content" + this.content.toString());
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        System.out.println("write:" + b + " off:" + off + " len:" + len);
        System.out.println("out file:" + this.file.getFileStatus().getPath().toString());
        for (int i = 0; i < len; i++) {
            content[pos++] = b[off + i];
            if (pos > content.length) {
                throw new IOException("content too long");
            }
        }
        System.out.println("content" + this.content.toString());
    }
}

class VineyardInputStream extends FSInputStream {
    File file;
    int offset = 0;
    int length = 1;
    byte[] content;

    public VineyardInputStream(File file) throws IOException {
      this.file = file;
      String pathStr = file.getFileStatus().getPath().toString();
      pathStr = pathStr.substring(pathStr.indexOf(":") + 1);
      String tableName = pathStr.replaceAll("//", "/");
      content = tableName.getBytes();
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
        System.out.println("read:" + offset + " length:" + this.length);
        System.out.println("read from:" + file.getFileStatus().getPath().toString());
        if (offset < this.length) {
            return this.content[offset++] & 0xff;
        }
      return -1;
    }
}

class File {
    boolean isDir;
    FileStatus fileStatus;
    VineyardOutputStream outputStream;

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

    public VineyardOutputStream getOutputStream() {
        return outputStream;
    }
}

class SpinLock {

    private AtomicReference<Thread> sign = new AtomicReference<>();

    public void lock(){
        System.out.println("lock!");
        Thread current = Thread.currentThread();

        while(!sign.compareAndSet(null, current)) {}
    }

    public void unlock (){
        System.out.println("unlock!");
        Thread current = Thread.currentThread();
        sign.compareAndSet(current, null);
    }
}
public class FileSystem extends org.apache.hadoop.fs.FileSystem {
    private IPCClient client;
    public static final String SCHEME = "vineyard";

    private URI uri = URI.create(SCHEME + ":///");
    private static Logger logger = LoggerFactory.getLogger(FileSystem.class);

    final static Map<String, File>fileMap = new HashMap<String, File>();
    final static SpinLock lock = new SpinLock();
    private Configuration conf;
 
    Path workingDir = new Path("/");
    public FileSystem() {
        super();
    }

    public void printAllFile()
    {
        System.out.println("print all file:");
        System.out.println("Isdir   Path");
        for (String key : fileMap.keySet()) {
            System.out.println(fileMap.get(key).getIsDir() +  " path:" + fileMap.get(key).getFileStatus().getPath().toString());
        }
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
        return uri;
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
        this.conf = conf;

        System.out.println("=================");
        System.out.println("Initialize vineyard file system: " + name);
        this.uri = name;
        mkdirs(new Path(uri.toString().replaceAll("///", "/")));
        // connect to vineyard
        try {
            if (client == null) {
                // TBD: get vineyard socket path from table properties
                client = new IPCClient(System.getenv("VINEYARD_IPC_SOCKET"));
            }
            if (client == null || !client.connected()) {
                throw new VineyardException.Invalid("failed to connect to vineyard");
            } else {
                System.out.printf("Connected to vineyard succeed!\n");
                System.out.printf("Hello vineyard!\n");
            }
        } catch (VineyardException e) {
            System.out.printf("Failed to connect to vineyard!\n");
            System.out.println(e.getMessage());
            throw new IOException(e);
        }
        System.out.println("=================");
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        System.out.println("=================");
        System.out.println("open: " + path.toString());
        System.out.println("=================");
        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");
        if (!fileMap.containsKey(pathStr)) {
            System.out.println("File not exists!");
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
        return create(path, fsPermission, overwrite, bufferSize, replication, blockSize, progressable, false);
    }

    private FSDataOutputStream create(Path path, FsPermission fsPermission,
                                    boolean overwrite, int bufferSize,
                                    short replication, long blockSize,
                                    Progressable progressable, boolean locked
                                    ) throws IOException {
        System.out.println("=================");
        System.out.println("create: " + path.toString());
        
        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");
        if (!locked) {
            lock.lock();
        }
        if (fileMap.containsKey(pathStr)) {
            System.out.println("File exists, delete!");
            fileMap.remove(pathStr);
        }
        System.out.println("create!");
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
        printAllFile();
        if (!locked) {
            lock.unlock();
        }
        System.out.println("=================");
        return result;
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
    public boolean delete(Path path, boolean b) throws IOException {
      return this.delete(path, b, false);
    }


    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        // now we create the new file and delete old file to simulate rename
        System.out.println("=================");
        System.out.println("rename: " + path.toString() + " to " + path1.toString());
        
        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        String pathStr1 = path1.toString().substring(path1.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");
        pathStr1 = pathStr1.replaceAll("//", "/");
        List<String> deleteList = new ArrayList<String>();
        List<String> addList = new ArrayList<String>();
        
        lock.lock();
        for (String key : fileMap.keySet()) {
            if (key.startsWith(pathStr) && (key.length() == pathStr.length() || key.substring(pathStr.length()).charAt(0) == '/')) {
                System.out.println("find:" + key);
                deleteList.add(key);
                addList.add(pathStr1 + key.substring(pathStr.length()));
                System.out.println("prepare change :" + key + " to :" + pathStr1 + key.substring(pathStr.length()));
            }
        }
        
        //delete old file and create new file
        for (int i = 0; i < deleteList.size(); i++) {
            File file = fileMap.get(deleteList.get(i));
            if (file == null) {
                System.out.println("WTF?");
                continue;
            }
            if (!file.getIsDir()) {
                create(new Path("vineyard:" + addList.get(i)), null, false, 0, (short) 0, 0, null, true);
                System.out.println("rename create done");
            } else {
                mkdirs(new Path("vineyard:" + addList.get(i)), new FsPermission((short) 777), true);
                System.out.println("rename mkdir done");
            }
        }

        this.delete(path, false, true);
        System.out.println("rename done");
        lock.unlock();
        
        String tableName = path.toString().replaceAll("/", "#");
        if (tableName.length() > 0) {
            try {
                System.out.println("rename table:" + tableName);
                ObjectID objectID = client.getName(tableName, false);
                if (objectID == null) {
                    System.out.println("Table not exists!");
                } else {
                    client.putName(objectID, path1.toString().replaceAll("/", "#"));
                }
            } catch (Exception e) {
                if (e instanceof VineyardException.ObjectNotExists) {
                    System.out.println(e.getMessage());
                } else {
                    System.out.println("return true 1");
                    System.out.println("=================");
                    return true;
                }
            }
        }
        System.out.println("return true 2");
        System.out.println("=================");
        return true;
    }
    
    private boolean delete(Path path, boolean b, boolean locked) throws IOException {
        System.out.println("=================");
        System.out.println("delete: " + path);
        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");
        if (!locked) {
            lock.lock();
        }
        List<String> deleteList = new ArrayList<String>();
        for (String key : fileMap.keySet()) {
            if (key.startsWith(pathStr) && (key.length() == pathStr.length() || key.substring(pathStr.length()).charAt(0) == '/')) {
                deleteList.add(key);
            }
        }
        for (int i = 0; i < deleteList.size(); i++) {
            fileMap.remove(deleteList.get(i));
        }
        printAllFile();
        if (!locked) {
            lock.unlock();
        }
        System.out.println("=================");
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        System.out.println("=================");
        System.out.println("listStatus: " + path);
        printAllFile();
        
        List<FileStatus> result = new ArrayList<FileStatus>();
        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");
        lock.lock();
        System.out.println("pathStr:" + pathStr);
        for (String key : fileMap.keySet()) {
            System.out.println("key:" + key);
            if (key.startsWith(pathStr) && key.length() > pathStr.length() && key.substring(pathStr.length()).charAt(0) == '/') {
                if (key.substring(pathStr.length()).split("/").length == 2) {
                    System.out.println("find key:" + key);
                    result.add(fileMap.get(key).getFileStatus());
                }
            }
        }
        lock.unlock();
        System.out.println("Find num:"  + result.size());
        System.out.println("=================");
        return result.toArray(new FileStatus[result.size()]);
    }

    @Override
    public void setWorkingDirectory(Path path) {
        System.out.println("=================");
        System.out.println("setWorkingDirectory: " + path);
        System.out.println("=================");
        workingDir = path;
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
        return this.mkdirs(path, fsPermission, false);
    }


    private boolean mkdirs(Path path, FsPermission fsPermission, boolean locked) throws IOException {
        System.out.println("=================");
        System.out.println("mkdirs: " + path);

        // create dir with path and all parent dir
        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");
        if (!locked) {
            lock.lock();
        }
        if (!fileMap.containsKey(pathStr)) {
            System.out.println("Dir not exists, create!");
            File file = new File(true, new FileStatus(1, true, 1, 1, 0, 0,
            new FsPermission((short) 777), null, null,
            path));
            // System.out.println("Dir name:" + pathStr);
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
            
            printAllFile();
        }
        if (!locked) {
            lock.unlock();
        }
        System.out.println("=================");
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        System.out.println("=================");
        System.out.println("getFileStatus: " + path.toString());
        System.out.println("=================");

        String pathStr = path.toString().substring(path.toString().indexOf(":") + 1);
        pathStr = pathStr.replaceAll("//", "/");

        FileStatus result = null;
        lock.lock();
        if (fileMap.containsKey(pathStr)) {
            System.out.println("File exists, return!");
            result =  fileMap.get(pathStr).getFileStatus();
            printAllFile();
            lock.unlock();
            return result;
        }
        int stageDirIndex = pathStr.indexOf(HiveConf.getVar(conf, HiveConf.ConfVars.STAGINGDIR));
        if (stageDirIndex >= 0 && pathStr.substring(stageDirIndex).split("/").length == 1) {
            System.out.println("Staging dir not exists, create file as dir!");
            File file = new File(true, new FileStatus(1, true, 1, 1, 0, 0,
                new FsPermission((short) 777), null, null,
                path));
            fileMap.put(pathStr, file);
            printAllFile();
            lock.unlock();
            return file.getFileStatus();
        }
        System.out.println("return null");
        lock.unlock();
        throw new FileNotFoundException();
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        System.out.println("=================");
        System.out.println("getXAttr: " + path);
        System.out.println("Get name:" + name);
        System.out.println("=================");
        return new byte[0];
    }

    @Override
    public void copyFromLocalFile(Path src, Path dst)
        throws IOException {
        System.out.println("=================");
        System.out.println("copyFromLocalFile1: " + src + " to " + dst);
        System.out.println("=================");
    }

    @Override
    public void moveFromLocalFile(Path[] srcs, Path dst)
      throws IOException {
        System.out.println("=================");
        System.out.println("moveFromLocalFile2: " + srcs + " to " + dst);
        System.out.println("=================");
    }
    @Override
    public void moveFromLocalFile(Path src, Path dst)
      throws IOException {
        System.out.println("=================");
        System.out.println("moveFromLocalFile3: " + src + " to " + dst);
        System.out.println("=================");
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
        System.out.println("=================");
        System.out.println("copyFromLocalFile4: " + src + " to " + dst);
        System.out.println("=================");
    }
    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path[] srcs, Path dst)
    throws IOException {
        System.out.println("=================");
        System.out.println("copyFromLocalFile5: " + srcs + " to " + dst);
        System.out.println("=================");
    }
  
    @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path src, Path dst)
    throws IOException {
        System.out.println("=================");
        System.out.println("copyFromLocalFile6: " + src + " to " + dst);
        System.out.println("=================");
    }

  @Override
  public void copyToLocalFile(Path src, Path dst) throws IOException {
        System.out.println("=================");
        System.out.println("copyToLocalFile7: " + src + " to " + dst);
        System.out.println("=================");
  }


  @Override
  public void moveToLocalFile(Path src, Path dst) throws IOException {
        System.out.println("=================");
        System.out.println("moveToLocalFile8: " + src + " to " + dst);
        System.out.println("=================");
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
        System.out.println("=================");
        System.out.println("copyToLocalFile9: " + src + " to " + dst);
        System.out.println("=================");
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst,
      boolean useRawLocalFileSystem) throws IOException {
        System.out.println("=================");
        System.out.println("copyToLocalFile10: " + src + " to " + dst);
        System.out.println("=================");
  }
}