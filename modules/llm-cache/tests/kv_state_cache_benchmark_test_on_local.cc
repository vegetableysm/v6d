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
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "client/client.h"
#include "client/ds/object_meta.h"
#include "common/util/logging.h"

#include "llm-cache/ds/kv_state_cache_manager.h"

using namespace vineyard;  //  NOLINT(build/namespaces)

constexpr int TENSORBYTES = 8;
constexpr int MULTI = 2048 / TENSORBYTES;
constexpr int CAPACITY = 1000;
constexpr int LAYER = 64;
constexpr int THREAD_NUM = 50;
constexpr int TOKEN_LENGTH = 128;

std::shared_ptr<KVStateCacheManager> manager;
FileCacheConfig config(TENSORBYTES, CAPACITY, LAYER, 32);

void init() { VINEYARD_CHECK_OK(KVStateCacheManager::Make(manager, config)); }

std::vector<int> generate_random_tokens(size_t max_length) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(1, 10000);

  size_t length = max_length;
  std::vector<int> tokens(length);
  for (size_t i = 0; i < length; ++i) {
    tokens[i] = dist(gen);
  }
  return tokens;
}

std::map<int, std::pair<LLMKV, LLMKV>> generate_kv_state(int token) {
  std::map<int, std::pair<LLMKV, LLMKV>> kv_state;
  for (int currentLayer = 0; currentLayer < LAYER; currentLayer++) {
    LLMKV key_state;
    LLMKV value_state;
    key_state.data = malloc(TENSORBYTES);
    key_state.length = TENSORBYTES;
    value_state.data = malloc(TENSORBYTES);
    value_state.length = TENSORBYTES;

    kv_state.insert(
        std::make_pair(currentLayer, std::make_pair(key_state, value_state)));
  }
  return kv_state;
}

std::vector<std::vector<std::vector<int>>> all_token_lists;
std::vector<std::map<int, std::pair<LLMKV, LLMKV>>> prepared_kv_state[THREAD_NUM * MULTI];

// test the performance of Query and Update function
void benchmark_inference(int i, double& update) {
  uint64_t write_size = 0;
  update = 0;

  for (int j = 0; j < all_token_lists[i].size(); j++) {
    write_size = 0;
    manager->Update(all_token_lists[i][j], prepared_kv_state[j + i * MULTI], write_size);
    update += (double) write_size;
  }

}

void benchmark_inference_2(int i, double& query) {
  uint64_t read_size = 0;
  std::vector<std::map<int, std::pair<LLMKV, LLMKV>>> kv_state;
  query = 0;

  for (int j = 0; j < all_token_lists[i].size(); j++) {
    read_size = 0;
    manager->Query(all_token_lists[i][j], kv_state, read_size);
    query += (double) read_size;
  }

}

int main(int argc, char** argv) {
  init();

  for (size_t i = 0; i < THREAD_NUM; ++i) {
    std::vector<std::vector<int>> tokens;
    for (size_t j = 0; j < MULTI; ++j) {
      tokens.push_back(generate_random_tokens(TOKEN_LENGTH));
    }
    all_token_lists.push_back(tokens);
  }

  std::vector<std::thread> threads;
  double update_size[THREAD_NUM];
  double query_size[THREAD_NUM];
  double total_update_size = 0;
  double total_query_size = 0;
  for (int i = 0; i < THREAD_NUM; i++) {
    for (int j = 0; j < all_token_lists[i].size(); j++) {
      for (int k = 0; k < all_token_lists[i][j].size(); k++) {
        std::map<int, std::pair<LLMKV, LLMKV>> kv;
        kv = generate_kv_state(all_token_lists[i][j][k]);
        prepared_kv_state[i].push_back(kv);
      }
    }
  }

  std::chrono::high_resolution_clock::time_point start, end;
  std::chrono::duration<double> update_time(0);
  std::chrono::duration<double> query_time(0);
  LOG(INFO) << "test update";
  start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < THREAD_NUM; ++i) {
    threads.push_back(std::thread(benchmark_inference,
                                  i,
                                  std::ref(update_size[i])));
  }

  for (size_t i = 0; i < THREAD_NUM; ++i) {
    threads[i].join();
    total_update_size += update_size[i];
  }
  end = std::chrono::high_resolution_clock::now();
  update_time = end - start;

  threads.clear();
  LOG(INFO) << "test query";
  start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < THREAD_NUM; ++i) {
    threads.push_back(std::thread(benchmark_inference_2,
                                  i,
                                  std::ref(query_size[i])));
  }

  for (size_t i = 0; i < THREAD_NUM; ++i) {
    threads[i].join();
    total_query_size += query_size[i];
  }
  end = std::chrono::high_resolution_clock::now();
  query_time = end - start;

  LOG(INFO) << "Total write size:" << total_update_size / 1024 / 1024 / 1024 << "GB."
            << " Total read size:" << total_query_size / 1024 / 1024 / 1024 << "GB."
            << " Total update time:" << update_time.count() << " S."
            << " Total query time:" << query_time.count() << " S.";

  LOG(INFO) << "Total update speed: "
            << (total_update_size / 1024 / 1024 / 1024) / update_time.count()
            << "G/s "
            << "Total query speed: "
            << (total_query_size / 1024 / 1024 / 1024) / query_time.count()
            << "G/s ";
  return 0;
}
