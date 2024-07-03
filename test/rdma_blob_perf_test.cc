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
#include <chrono>

#include "arrow/api.h"
#include "arrow/io/api.h"

#include "basic/ds/dataframe.h"
#include "client/client.h"
#include "client/ds/object_meta.h"
#include "client/rpc_client.h"
#include "common/util/logging.h"

using namespace vineyard;  // NOLINT(build/namespaces)

// constexpr uint64_t iterator = 10000;
constexpr uint64_t total_mem = 1024UL * 1024 * 1088;
constexpr uint64_t warm_up = 1;

void TestCreateBlob(RPCClient& client, std::vector<ObjectID> &warm_up_ids, std::vector<ObjectID> &ids, size_t size) {
  std::vector<std::shared_ptr<RemoteBlobWriter>> warm_upremote_blob_writers;
  std::vector<std::shared_ptr<RemoteBlobWriter>> remote_blob_writers;
  uint64_t iterator = total_mem / size - warm_up;
  
  for (size_t i = 0; i < warm_up; i++) {
    auto remote_blob_writer = std::make_shared<RemoteBlobWriter>(size, (uint64_t)client.GetBuffer(size));
    uint8_t *data = reinterpret_cast<uint8_t *>(remote_blob_writer->data());
    for (size_t j = 0; j < size; j++) {
      data[j] = j % 256;
    }
    warm_upremote_blob_writers.push_back(remote_blob_writer);
  }

  for (size_t i = 0; i < iterator; i++) {
    auto remote_blob_writer = std::make_shared<RemoteBlobWriter>(size, (uint64_t)client.GetBuffer(size));
    uint8_t *data = reinterpret_cast<uint8_t *>(remote_blob_writer->data());
    for (size_t j = 0; j < size; j++) {
      data[j] = j % 256;
    }
    remote_blob_writers.push_back(remote_blob_writer);
  }

  std::vector<ObjectMeta> warm_up_metas;
  std::vector<ObjectMeta> metas;
  warm_up_metas.reserve(warm_up);
  metas.reserve(iterator);
  LOG(INFO) << "Creating remote blobs...";
  VINEYARD_CHECK_OK(client.CreateRemoteBlobs(warm_upremote_blob_writers, warm_up_metas));

  auto start = std::chrono::high_resolution_clock::now();

  VINEYARD_CHECK_OK(client.CreateRemoteBlobs(remote_blob_writers, metas));

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  LOG(INFO) << "Iterator: " << iterator << ", Size: " << size << ", Time: " << duration.count() << "ms" << " average time:" << (double)duration.count() / iterator << "ms";
  LOG(INFO) << "Speed:" << (double)(iterator * size) / 1024 / 1024 / ((double)duration.count() / 1000) << "MB/s\n";

  for (size_t i = 0; i < warm_up; i++) {
    warm_up_ids.push_back(warm_up_metas[i].GetId());
  }
  for (size_t i = 0; i < iterator; i++) {
    ids.push_back(metas[i].GetId());
  }
}

void TestGetBlob(RPCClient& client, std::vector<ObjectID> &warm_up_ids, std::vector<ObjectID> &ids,
                 size_t size, std::vector<std::shared_ptr<RemoteBlob>> &warm_up_local_buffers,
                 std::vector<std::shared_ptr<RemoteBlob>> &local_buffers) {
  LOG(INFO) << "Getting remote blobs...";
  uint64_t iterator = total_mem / size - warm_up;

  warm_up_local_buffers.reserve(warm_up);
  local_buffers.reserve(iterator);
  VINEYARD_CHECK_OK(client.GetRemoteBlobs(warm_up_ids, warm_up_local_buffers));

  auto start = std::chrono::high_resolution_clock::now();
  VINEYARD_CHECK_OK(client.GetRemoteBlobs(ids, local_buffers));
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  LOG(INFO) << "Iterator: " << iterator << ", Size: " << size << ", Time: " << duration.count() << "ms" << " average time:" << (double)duration.count() / iterator << "ms";
  LOG(INFO) << "Speed:" << (double)(iterator * size) / 1024 / 1024 / ((double)duration.count() / 1000) << "MB/s\n";
}

void CheckBlobValue(std::vector<std::shared_ptr<RemoteBlob>> &warm_up_local_buffers,
                    std::vector<std::shared_ptr<RemoteBlob>> &local_buffers) {
  for (size_t i = 0; i < warm_up_local_buffers.size(); i++) {
    const uint8_t *data = reinterpret_cast<const uint8_t *>(warm_up_local_buffers[i]->data());
    for (size_t j = 0; j < warm_up_local_buffers[i]->size(); j++) {
      CHECK_EQ(data[j], j % 256);
    }
  }
  for (size_t i = 0; i < local_buffers.size(); i++) {
    const uint8_t *data = reinterpret_cast<const uint8_t *>(local_buffers[i]->data());
    for (size_t j = 0; j < local_buffers[i]->size(); j++) {
      CHECK_EQ(data[j], j % 256);
    }
  }
}

// Test 512K~512M blob
int main(int argc, const char** argv) {
  if (argc < 6) {
    LOG(ERROR) << "usage: " << argv[0] << " <rpc_endpoint>" << " <rdma_endpoint>" << " <rkey>" << " <min_size>" << " <max_size>";
    return -1;
  }
  std::string rpc_endpoint = std::string(argv[1]);
  std::string rdma_endpoint = std::string(argv[2]);
  RPCClient client;
  VINEYARD_CHECK_OK(client.Connect(rpc_endpoint, "", "", rdma_endpoint));
  uint64_t rkey = std::stoull(argv[3]);
  client.SetRkey(rkey);

  uint64_t min_size = 1024 * 1024 * 2; // 512K
  uint64_t max_size = 1024 * 1024 * 2; // 64M
  min_size = std::stoull(argv[4]) * 1024 * 1024;
  max_size = std::stoull(argv[5]) * 1024 * 1024;
  if (min_size == 0) {
    min_size = 1024 * 512;
  }
  if (max_size == 0) {
    max_size = 1024 * 512;
  }
  std::vector<std::vector<ObjectID>> blob_ids_list;
  std::vector<std::vector<ObjectID>> warm_up_blob_ids_list;
  std::vector<size_t> sizes;
  // blob_ids_list.reserve(16);
  // for (size_t i = 0; i < 16; i++) {
  //   uint64_t iterator = total_mem / size - warm_up;
  //   blob_ids_list[i].reserve(iterator + warm_up);
  // }

  LOG(INFO) << "Test Create Blob(RDMA write)";
  LOG(INFO) << "----------------------------";
  for (size_t size = min_size; size <= max_size; size *= 2) {
    std::vector<ObjectID> ids;
    std::vector<ObjectID> warm_up_ids;
    TestCreateBlob(client, warm_up_ids, ids, size);
    blob_ids_list.push_back(ids);
    warm_up_blob_ids_list.push_back(warm_up_ids);
    sizes.push_back(size);
  }

  std::vector<std::shared_ptr<RemoteBlob>> warm_up_local_buffers;
  std::vector<std::shared_ptr<RemoteBlob>> local_buffers;
  LOG(INFO) << "Test Get Blob(RDMA read)";
  LOG(INFO) << "----------------------------";
  for (size_t i = 0; i < blob_ids_list.size(); i++) {
    TestGetBlob(client, warm_up_blob_ids_list[i], blob_ids_list[i], sizes[i], warm_up_local_buffers, local_buffers);
  }

  CheckBlobValue(warm_up_local_buffers, local_buffers);

  LOG(INFO) << "Clean all object";
  for (size_t i = 0; i < blob_ids_list.size(); i++) {
    for (size_t j = 0; j < blob_ids_list[i].size(); j++) {
      VINEYARD_CHECK_OK(client.DelData(blob_ids_list[i][j]));
    }
  }
  for (size_t i = 0; i < warm_up_blob_ids_list.size(); i++) {
    for (size_t j = 0; j < warm_up_blob_ids_list[i].size(); j++) {
      VINEYARD_CHECK_OK(client.DelData(warm_up_blob_ids_list[i][j]));
    }
  }

  LOG(INFO) << "Passed blob rdma performance test.";

  return 0;
}