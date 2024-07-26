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

#include "client/rpc_client.h"
#include "common/util/logging.h"

using namespace vineyard;  // NOLINT(build/namespaces)

constexpr int blob_num = 10000;
constexpr int parallel = 4;

void PrepareRemoteBlobs(std::shared_ptr<RPCClient>& client,
                        std::vector<ObjectID>& ids) {
  std::vector<std::shared_ptr<RemoteBlobWriter>> remote_blob_writers;
  for (int i = 0; i < blob_num; i++) {
    auto remote_blob_writer = std::make_shared<RemoteBlobWriter>(1024);
    uint8_t* data = reinterpret_cast<uint8_t*>(remote_blob_writer->data());
    for (size_t j = 0; j < 1024; j++) {
      data[j] = j % 256;
    }
    remote_blob_writers.push_back(remote_blob_writer);
  }
  std::vector<ObjectMeta> metas;
  client->CreateRemoteBlobs(remote_blob_writers, metas);
  for (size_t i = 0; i < metas.size(); i++) {
    ids.push_back(metas[i].GetId());
  }
}
std::vector<std::vector<std::shared_ptr<RemoteBlob>>> remote_blobs;
void GetRemoteBlobs(std::shared_ptr<RPCClient>& client,
                    std::vector<ObjectID> ids, int i) {
  Status status = client->GetRemoteBlobs(ids, remote_blobs[i]);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to get remote blobs: " << status.ToString();
  }
}

void TestDelObject(std::shared_ptr<RPCClient>& client,
                   std::vector<ObjectID> ids) {
  Status status = client->DelData(ids);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to delete objects: " << status.ToString();
  } else {
    LOG(INFO) << "Successfully delete objects!";
  }
}

void CheckValue() {
  for (size_t i = 0; i < remote_blobs.size(); i++) {
    for (size_t j = 0; j < remote_blobs[i].size(); j++) {
      const uint8_t* data = reinterpret_cast<const uint8_t*>(remote_blobs[i][j]->data());
      for (size_t k = 0; k < 1024; k++) {
        if (data[k] != k % 256) {
          LOG(ERROR) << "Data error!";
          return;
        }
      }
    }
  }
  LOG(INFO) << "Check value done!";
}

int main(int argc, char** argv) {
  if (argc < 3) {
    LOG(ERROR) << "usage: " << argv[0] << " <rpc_endpoint>"
               << " <rdma_endpoint>";
    return 1;
  }
  remote_blobs.resize(parallel);

  std::string rpc_endpoint = std::string(argv[1]);
  std::string rdma_endpoint = std::string(argv[2]);
  std::vector<std::shared_ptr<RPCClient>> clients;
  for (int i = 0; i < parallel; i++) {
    std::shared_ptr<RPCClient> client = std::make_shared<RPCClient>();
    client->Connect(rpc_endpoint, "", "", rdma_endpoint);
    clients.push_back(client);
  }
  std::shared_ptr<RPCClient> client_2 = std::make_shared<RPCClient>();
  client_2->Connect(rpc_endpoint, "", "", rdma_endpoint);

  std::vector<ObjectID> ids;
  PrepareRemoteBlobs(clients[0], ids);
  std::vector<std::thread> threads;
  // threads.push_back(
  //     std::thread(TestDelObject, std::ref(client_2),
  //                 std::vector<ObjectID>(ids.begin() + 9, ids.end())));
  // sleep(2);
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < parallel; i++) {
    threads.push_back(
        std::thread(GetRemoteBlobs, std::ref(clients[i]),
                    std::vector<ObjectID>(ids.begin() + i % 9, ids.end() - 1), i));
  }
  // threads.push_back(
  //     std::thread(TestDelObject, std::ref(client_2), std::ref(ids)));
  // threads.push_back(
  //     std::thread(TestDelObject, std::ref(client_2), std::ref(ids)));
  // // sleep(2);
  // threads.push_back(
  //     std::thread(TestDelObject, std::ref(client_2), std::ref(ids)));
  // // sleep(2);
  // threads.push_back(
  //     std::thread(TestDelObject, std::ref(client_2), std::ref(ids)));

  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
  auto end = std::chrono::high_resolution_clock::now();
  LOG(INFO) << "Time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                     start)
                   .count()
            << "ms";
  LOG(INFO) << "Done.";
  CheckValue();

  for (int i = 0; i < parallel; i++) {
    clients[i]->Disconnect();
  }
  client_2->Disconnect();

  LOG(INFO) << "Passed test!";

  return 0;
}
