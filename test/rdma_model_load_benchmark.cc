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

#include <algorithm>
#include <chrono>
#include <fstream>
#include <limits>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/io/api.h"

#include "client/client.h"
#include "client/ds/object_meta.h"
#include "client/rpc_client.h"
#include "common/util/logging.h"
#include "llm-cache/thread_group.h"

using namespace vineyard;

// size number

void GenerateBlobs(std::vector<std::shared_ptr<RemoteBlobWriter>>& data_list,
                   std::vector<size_t>& size_list, size_t& total_size) {
  LOG(INFO) << "Generating blobs...";
  total_size = 0;
  for (size_t i = 0; i < size_list.size(); i++) {
    std::shared_ptr<RemoteBlobWriter> writer =
        std::make_shared<RemoteBlobWriter>(size_list[i]);
    uint8_t* data = reinterpret_cast<uint8_t*>(writer->data());
    for (size_t j = 0; j < size_list[i]; j++) {
      data[j] = j % 256;
    }
    data_list.push_back(writer);
    total_size += size_list[i];
  }
}

void GenerateBlobsForMultiModel(std::vector<std::vector<std::shared_ptr<RemoteBlobWriter>>>& multi_data_list,
                   std::vector<size_t>& size_list, size_t& total_size, int parallel) {
  LOG(INFO) << "Generating blobs...";
  total_size = 0;
  for (int index = 0; index < parallel; index++) {
    std::vector<std::shared_ptr<RemoteBlobWriter>> data_list;
    for (size_t i = 0; i < size_list.size(); i++) {
      std::shared_ptr<RemoteBlobWriter> writer =
          std::make_shared<RemoteBlobWriter>(size_list[i]);
      uint8_t* data = reinterpret_cast<uint8_t*>(writer->data());
      for (size_t j = 0; j < size_list[i]; j++) {
        data[j] = j % 256;
      }
      data_list.push_back(writer);
      total_size += size_list[i];
    }
    multi_data_list.push_back(data_list);
  }
}

void CreateBlobFunc(std::shared_ptr<RPCClient>& client,
                    std::shared_ptr<RemoteBlobWriter>& data, ObjectID& id) {
  ObjectMeta meta;
  VINEYARD_CHECK_OK(client->CreateRemoteBlob(data, meta));
  id = meta.GetId();
}

void CreateBlobTests(std::vector<std::shared_ptr<RPCClient>>& client_list,
                     std::vector<std::shared_ptr<RemoteBlobWriter>>& data_list,
                     std::vector<ObjectID>& ids, int thread_num,
                     size_t total_size) {
  auto fn = [&client_list, &data_list, &ids, &thread_num](size_t i) -> Status {
    CreateBlobFunc(client_list[i % thread_num], data_list[i], ids[i]);
    return Status::OK();
  };

  LOG(INFO) << "Thread num:" << thread_num;
  parallel::ThreadGroup tg(thread_num);
  std::vector<parallel::ThreadGroup::tid_t> tids(data_list.size());
  auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < data_list.size(); i++) {
    tids[i] = tg.AddTask(fn, i);
  }
  std::vector<Status> taskResult(data_list.size(), Status::OK());
  for (size_t i = 0; i < data_list.size(); i++) {
    taskResult[i] = tg.TaskResult(tids[i]);
    VINEYARD_CHECK_OK(taskResult[i]);
  }
  auto end = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  LOG(INFO) << "Time: " << duration.count() << "us"
            << " average time: "
            << static_cast<double>(duration.count()) / data_list.size() << "us";
  LOG(INFO) << "Bandwidth: "
            << static_cast<double>(total_size) / 1024 / 1024 /
                   (static_cast<double>(duration.count()) / 1000 / 1000)
            << "MB/s with " << thread_num << " threads";
}

void CreateBlobTests_2(
    std::vector<std::shared_ptr<RPCClient>>& client_list,
    std::vector<std::shared_ptr<RemoteBlobWriter>>& data_list,
    std::vector<std::vector<ObjectID>>& ids, int thread_num,
    size_t total_size) {
  LOG(INFO) << "Thread num:" << thread_num;
  std::vector<std::thread> threads;
  std::vector<std::vector<std::shared_ptr<RemoteBlobWriter>>> data_lists(
      thread_num);
  for (int i = 0; i < thread_num; i++) {
    if (i == thread_num - 1) {
      data_lists[i].resize(data_list.size() / thread_num +
                           data_list.size() % thread_num);
      for (size_t j = 0;
           j < data_list.size() / thread_num + data_list.size() % thread_num;
           j++) {
        data_lists[i][j] = data_list[i * (data_list.size() / thread_num) + j];
      }
    } else {
      data_lists[i].resize(data_list.size() / thread_num);
      for (size_t j = 0; j < data_list.size() / thread_num; j++) {
        data_lists[i][j] = data_list[i * (data_list.size() / thread_num) + j];
      }
    }
  }

  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < thread_num; i++) {
    threads.push_back(std::thread(
        [&client_list, &data_lists, &ids, thread_num, total_size, i]() {
          std::vector<ObjectMeta> meta_list;
          VINEYARD_CHECK_OK(
              client_list[i]->CreateRemoteBlobs(data_lists[i], meta_list));
          ids[i].resize(meta_list.size());
          for (size_t j = 0; j < meta_list.size(); j++) {
            ids[i][j] = meta_list[j].GetId();
          }
        }));
  }
  for (int i = 0; i < thread_num; i++) {
    threads[i].join();
  }

  auto end = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  LOG(INFO) << "Time: " << duration.count() << "us"
            << " average time: "
            << static_cast<double>(duration.count()) / data_list.size() << "us";
  LOG(INFO) << "Bandwidth: "
            << static_cast<double>(total_size) / 1024 / 1024 /
                   (static_cast<double>(duration.count()) / 1000 / 1000)
            << "MB/s with " << thread_num << " threads";
}

void GetBlobTest(std::vector<std::shared_ptr<RPCClient>>& client_list,
                 std::vector<ObjectID>& ids, int thread_num,
                 size_t total_size) {
  LOG(INFO) << "Getting remote blobs...";
  auto fn = [&client_list, &ids, &thread_num](size_t i) -> Status {
    std::shared_ptr<RemoteBlob> remote_buffer;
    VINEYARD_CHECK_OK(
        client_list[i % thread_num]->GetRemoteBlob(ids[i], remote_buffer));
    return Status::OK();
  };

  parallel::ThreadGroup tg(thread_num);
  std::vector<parallel::ThreadGroup::tid_t> tids(ids.size());
  auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < ids.size(); i++) {
    tids[i] = tg.AddTask(fn, i);
  }
  std::vector<Status> taskResult(ids.size(), Status::OK());
  for (size_t i = 0; i < ids.size(); i++) {
    taskResult[i] = tg.TaskResult(tids[i]);
    VINEYARD_CHECK_OK(taskResult[i]);
  }
  auto end = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  LOG(INFO) << "Time: " << duration.count() << "us"
            << " average time: "
            << static_cast<double>(duration.count()) / ids.size() << "us";
  LOG(INFO) << "Bandwidth: "
            << static_cast<double>(total_size) / 1024 / 1024 /
                   (static_cast<double>(duration.count()) / 1000 / 1000)
            << "MB/s with " << thread_num << " threads";
}

void GetBlobTest_2(std::vector<std::shared_ptr<RPCClient>>& client_list,
                   std::vector<std::vector<ObjectID>>& ids, int thread_num,
                   size_t total_size) {
  LOG(INFO) << "Getting remote blobs...";
  std::vector<std::thread> threads;
  auto start = std::chrono::steady_clock::now();

  for (int i = 0; i < thread_num; i++) {
    threads.push_back(std::thread([&client_list, &ids, thread_num, total_size,
                                   i]() {
      std::vector<std::shared_ptr<RemoteBlob>> remote_buffers;
      VINEYARD_CHECK_OK(client_list[i]->GetRemoteBlobs(ids[i], remote_buffers));
    }));
  }
  for (int i = 0; i < thread_num; i++) {
    threads[i].join();
  }

  auto end = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  LOG(INFO) << "Time: " << duration.count() << "us"
            << " average time: "
            << static_cast<double>(duration.count()) / ids.size() << "us";
  LOG(INFO) << "Bandwidth: "
            << static_cast<double>(total_size) / 1024 / 1024 /
                   (static_cast<double>(duration.count()) / 1000 / 1000)
            << "MB/s with " << thread_num << " threads";
}

void CleanAllObject(std::shared_ptr<RPCClient>& client,
                    std::vector<ObjectID>& ids) {
  VINEYARD_CHECK_OK(client->DelData(ids));
}

void CleanAllObject_2(std::vector<std::shared_ptr<RPCClient>>& client,
                      std::vector<std::vector<ObjectID>>& ids) {
  for (size_t i = 0; i < ids.size(); i++) {
    VINEYARD_CHECK_OK(client[i]->DelData(ids[i]));
  }
}

void CreateBlobTests_3(
    std::vector<std::shared_ptr<RPCClient>>& client_list,
    std::vector<std::vector<std::shared_ptr<RemoteBlobWriter>>>& data_list,
    std::vector<std::vector<ObjectID>>& ids, int thread_num,
    size_t total_size) {
  std::vector<std::thread> threads;
  ids.resize(thread_num);
  
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < thread_num; i++) {
    threads.push_back(std::thread(
        [&client_list, &data_list, &ids, thread_num, total_size, i]() {
          std::vector<ObjectMeta> meta_list;
          VINEYARD_CHECK_OK(
              client_list[i]->CreateRemoteBlobs(data_list[i], meta_list));
          ids[i].resize(meta_list.size());
          for (size_t j = 0; j < meta_list.size(); j++) {
            ids[i][j] = meta_list[j].GetId();
          }
        }));
  }
  for (int i = 0; i < thread_num; i++) {
    threads[i].join();
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  LOG(INFO) << "Create time: " << duration.count() << "us.";
  LOG(INFO) << "Bandwidth: "
            << static_cast<double>(total_size) / 1024 / 1024 /
                   (static_cast<double>(duration.count()) / 1000 / 1000)
            << "MB/s with " << thread_num << " threads";
}

void GetBlobTest_3(std::vector<std::shared_ptr<RPCClient>>& client_list,
                   std::vector<std::vector<ObjectID>>& ids, int thread_num,
                   size_t total_size) {
  std::vector<std::thread> threads;
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < thread_num; i++) {
    threads.push_back(std::thread([&client_list, &ids, thread_num, total_size,
                                   i]() {
      std::vector<std::shared_ptr<RemoteBlob>> remote_buffers;
      VINEYARD_CHECK_OK(client_list[i]->GetRemoteBlobs(ids[i], remote_buffers));
    }));
  }
  for (int i = 0; i < thread_num; i++) {
    threads[i].join();
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  LOG(INFO) << "Get time: " << duration.count() << "us.";
  LOG(INFO) << "Bandwidth: "
            << static_cast<double>(total_size) / 1024 / 1024 /
                   (static_cast<double>(duration.count()) / 1000 / 1000)
            << "MB/s with " << thread_num << " threads";
}

void CleanAllObject_3(std::vector<std::shared_ptr<RPCClient>>& client,
                      std::vector<std::vector<ObjectID>>& ids) {
  for (size_t i = 0; i < ids.size(); i++) {
    VINEYARD_CHECK_OK(client[i]->DelData(ids[i]));
  }
}

int main(int argc, const char** argv) {
  if (argc < 6) {
    printf(
        "usage: %s <model_size_path> <rpc_endpoint> <rdma_endpoint> "
        "<min_threads> <max_threads>\n",
        argv[0]);
    return 1;
  }

  std::string model_size_path = std::string(argv[1]);
  std::string rpc_endpoint = std::string(argv[2]);
  std::string rdma_endpoint = std::string(argv[3]);
  int min_threads = std::stoi(argv[4]);
  int max_threads = std::stoi(argv[5]);

  std::vector<size_t> size_list;

  std::ifstream model_size_file(model_size_path);
  if (!model_size_file.is_open()) {
    printf("failed to open model size file: %s\n", model_size_path.c_str());
    return 1;
  }

  std::string line;
  while (std::getline(model_size_file, line)) {
    size_t size = std::stoul(line);
    size_list.push_back(size);
  }

  size_t total_size;
  // std::vector<std::shared_ptr<RemoteBlobWriter>> data_list;
  // GenerateBlobs(data_list, size_list, total_size);
  std::vector<std::vector<std::shared_ptr<RemoteBlobWriter>>> data_list;
  GenerateBlobsForMultiModel(data_list, size_list, total_size, max_threads);
  LOG(INFO) << "Read lines: " << size_list.size()
            << " total size:" << total_size << " bytes.";

  std::vector<std::shared_ptr<RPCClient>> rpc_client_list;
  
  std::vector<std::thread> threads;
  rpc_client_list.resize(max_threads);
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < max_threads; i++) {
    // rpc_client_list.push_back(std::make_shared<RPCClient>());
    // VINEYARD_CHECK_OK(
    //     rpc_client_list.back()->Connect(rpc_endpoint, "", "", rdma_endpoint));
    threads.push_back(std::thread([&rpc_client_list, &rpc_endpoint, &rdma_endpoint, i]() {
      rpc_client_list[i] = std::make_shared<RPCClient>();
      VINEYARD_CHECK_OK(
          rpc_client_list[i]->Connect(rpc_endpoint, "", "", rdma_endpoint));
    }));
  }
  for (int i = 0; i < max_threads; i++) {
    threads[i].join();
  }
  auto conn_end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(conn_end - start);
  LOG(INFO) << "Connect time: " << duration.count() << "us.";

  std::vector<std::vector<std::vector<ObjectID>>> ids_3_list(max_threads);
  ids_3_list.resize(max_threads - min_threads + 1);
  for (int i = min_threads; i <= max_threads; i++) {
    // std::vector<ObjectID> ids(data_list.size());
    // CreateBlobTests(rpc_client_list, data_list, ids, i, total_size);
    // GetBlobTest(rpc_client_list, ids, i, total_size);
    // CleanAllObject(rpc_client_list.back(), ids);

    // std::vector<std::vector<ObjectID>> ids_2(max_threads);
    // CreateBlobTests_2(rpc_client_list, data_list, ids_2, i, total_size);
    // GetBlobTest_2(rpc_client_list, ids_2, i, total_size);
    // CleanAllObject_2(rpc_client_list, ids_2);

    std::vector<std::vector<ObjectID>> ids_3;
    CreateBlobTests_3(rpc_client_list, data_list, ids_3, i, total_size);
    GetBlobTest_3(rpc_client_list, ids_3, i, total_size);
  }
  auto end = std::chrono::high_resolution_clock::now();
  duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  LOG(INFO) << "Total time: " << duration.count() << "us.";
  LOG(INFO) << "Average bandwitdh(put/get)" << static_cast<double>(total_size) * 2 / 1024 / 1024 /
                   (static_cast<double>(duration.count()) / 1000 / 1000) << "MB/s.";

  for (size_t i = 0; i < ids_3_list.size(); i++) {
      CleanAllObject_3(rpc_client_list, ids_3_list[i]);
  }

  LOG(INFO) << "Pass all model loading benchmark tests.";

  return 0;
}
