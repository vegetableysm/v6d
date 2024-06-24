/** Copyright 2020-2023 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <memory>
#include <string>
#include <thread>

#include "arrow/api.h"
#include "arrow/io/api.h"

#include "basic/ds/array.h"
#include "client/client.h"
#include "client/ds/object_meta.h"
#include "client/rpc_client.h"
#include "common/util/logging.h"

using namespace vineyard;  // NOLINT(build/namespaces)

void threadFunc(Client &client, ObjectID id) {
  LOG(INFO) << "wait 2s...";
  sleep(2);
  VINEYARD_CHECK_OK(client.DelData(id));
  LOG(INFO) << "Object has been del!";

  LOG(INFO) << "Create new!";
  std::vector<double> double_array = {11, 23, 34, 45, 56};

  ArrayBuilder<double> builder(client, double_array);
  auto sealed_double_array =
      std::dynamic_pointer_cast<Array<double>>(builder.Seal(client));
  VINEYARD_CHECK_OK(client.Persist(sealed_double_array->id()));
  LOG(INFO) << "Create new done";
}

int main(int argc, char** argv) {
  if (argc < 3) {
    printf("usage ./rpc_test <ipc_socket> <rpc_endpoint>");
    return 1;
  }
  std::string ipc_socket(argv[1]);
  std::string rpc_endpoint(argv[2]);

  Client ipc_client;
  VINEYARD_CHECK_OK(ipc_client.Connect(ipc_socket));
  LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

  RPCClient rpc_client;
  VINEYARD_CHECK_OK(rpc_client.Connect(rpc_endpoint));
  LOG(INFO) << "Connected to RPCServer: " << rpc_endpoint;

  std::vector<double> double_array = {1.0, 7.0, 3.0, 4.0, 2.0};

  ArrayBuilder<double> builder(ipc_client, double_array);
  auto sealed_double_array =
      std::dynamic_pointer_cast<Array<double>>(builder.Seal(ipc_client));
  VINEYARD_CHECK_OK(ipc_client.Persist(sealed_double_array->id()));

  ObjectID id = sealed_double_array->id();
  std::thread t(threadFunc, std::ref(ipc_client), id);

  ObjectMeta array_meta;
  VINEYARD_CHECK_OK(ipc_client.GetMetaData(id, array_meta));
  ObjectID blob_id = array_meta.GetMemberMeta("buffer_").GetId();
  std::shared_ptr<RemoteBlob> remote_buffer;
  // Get remote blob, but it will sleep 5 second at vineyardd.
  VINEYARD_CHECK_OK(rpc_client.GetRemoteBlob(blob_id, remote_buffer));

  for (size_t i = 0; i < double_array.size(); ++i) {
    const double *data = reinterpret_cast<const double *>(remote_buffer->data());
    LOG(INFO) << "Read data:" << data[i];
    CHECK_EQ(data[i], double_array[i]);
  }
  LOG(INFO) << "Passed rpc client tests...";
  t.join();

  ipc_client.Disconnect();
  rpc_client.Disconnect();

  return 0;
}
