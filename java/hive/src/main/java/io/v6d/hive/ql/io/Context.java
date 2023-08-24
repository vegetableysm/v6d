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
import io.v6d.core.client.IPCClient;
import io.v6d.core.common.util.VineyardException;

import java.text.SimpleDateFormat;

public class Context {
    public static IPCClient client = null;
    public static ThreadLocal<SimpleDateFormat> formatter = ThreadLocal.withInitial(() -> new SimpleDateFormat("HH:mm:ss.SSS"));

    public static synchronized IPCClient getClient() throws VineyardException {
        if (client == null) {
            client = new IPCClient(System.getenv("VINEYARD_IPC_SOCKET"));
            System.out.println("Connected to vineyard: " + System.getenv("VINEYARD_IPC_SOCKET"));
        }
        return client;
    }

    public static void println(String message) {
        System.out.printf("[%s] %s\n", formatter.get().format(System.currentTimeMillis()), message);
    }

    public static void printf(String format, Object... args) {
        System.out.printf("[%s] ", formatter.get().format(System.currentTimeMillis()));
        System.out.printf(format, args);
    }
}
