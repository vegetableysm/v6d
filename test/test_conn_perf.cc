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

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "arrow/api.h"
#include "arrow/io/api.h"

#include "client/client.h"
#include "client/ds/object_meta.h"
#include "client/rpc_client.h"
#include "common/util/logging.h"

using namespace vineyard;  // NOLINT(build/namespaces)

void TestConnRDMA(int round, std::string rpc_endpoint, std::string rdma_endpoint) {
  std::vector<std::shared_ptr<RPCClient>> clients;
  std::vector<std::thread> threads;
  for (int i = 0; i < round; i++) {
    auto client = std::make_shared<RPCClient>();
    clients.push_back(client);
  }
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < round; i++) {
    threads.push_back(std::thread([i, &clients, rpc_endpoint, rdma_endpoint]() {
      auto client = clients[i];
      client->Connect(rpc_endpoint, "", "", rdma_endpoint);
    }));
  }
  for (int i = 0; i < round; i++) {
    threads[i].join();
  }
  auto end = std::chrono::high_resolution_clock::now();
  LOG(INFO) << "RDMA connection time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
            << "ms";
}

void TestConnTCP(int round, std::string rpc_endpoint) {
  std::vector<std::shared_ptr<RPCClient>> clients;
  std::vector<std::thread> threads;
  for (int i = 0; i < round; i++) {
    auto client = std::make_shared<RPCClient>();
    clients.push_back(client);
  }
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < round; i++) {
    threads.push_back(std::thread([i, &clients, rpc_endpoint]() {
      auto client = clients[i];
      client->Connect(rpc_endpoint);
    }));
  }
  for (int i = 0; i < round; i++) {
    threads[i].join();
  }
  auto end = std::chrono::high_resolution_clock::now();
  LOG(INFO) << "TCP connection time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
            << "ms";
}

void TestConnect(int round, std::string rpc_endpoint, std::string rdma_endpoint) {
  TestConnRDMA(round, rpc_endpoint, rdma_endpoint);
  TestConnTCP(round, rpc_endpoint);
}

int main(int argc, char** argv) {
  if (argc < 3) {
    printf("usage: %s <rpc_endpoint> <rdma_endpoint> <rounds>\n", argv[0]);
    return 1;
  }
  std::string rpc_endpoint = argv[1];
  std::string rdma_endpoint = argv[2];
  int rounds = std::stoi(argv[3]);

  TestConnect(rounds, rpc_endpoint, rdma_endpoint);

  LOG(INFO) << "Pass rpc conn perf tests...";
  return 0;
}