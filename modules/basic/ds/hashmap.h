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

#ifndef MODULES_BASIC_DS_HASHMAP_H_
#define MODULES_BASIC_DS_HASHMAP_H_

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"
#include "wyhash/wyhash.hpp"

#include "basic/ds/array.h"
#include "basic/ds/hashmap.vineyard.h"
#include "basic/utils.h"
#include "client/ds/blob.h"
#include "client/ds/i_object.h"
#include "common/util/arrow.h"
#include "common/util/uuid.h"

#include "BBHash/BooPHF.h"

namespace vineyard {

/**
 * @brief HashmapBuilder is used for constructing hashmaps that supported by
 * vineyard.
 *
 * @tparam K The type for the key.
 * @tparam V The type for the value.
 * @tparam std::hash<K> The hash function for the key.
 * @tparam std::equal_to<K> The compare function for the key.
 */
template <typename K, typename V, typename H = prime_number_hash_wy<K>,
          typename E = std::equal_to<K>>
class HashmapBuilder : public HashmapBaseBuilder<K, V, H, E> {
 public:
  explicit HashmapBuilder(Client& client)
      : HashmapBaseBuilder<K, V, H, E>(client) {}

  explicit HashmapBuilder(Client& client,
                          ska::flat_hash_map<K, V, H, E>&& hashmap)
      : HashmapBaseBuilder<K, V, H, E>(client), hashmap_(std::move(hashmap)) {}

  /**
   * @brief Get the mapping value of the given key.
   *
   */
  inline V& operator[](const K& key) { return hashmap_[key]; }

  /**
   * @brief Get the mapping value of the given key.
   *
   */
  inline V& operator[](K&& key) { return hashmap_[std::move(key)]; }

  /**
   * @brief Emplace key-value pair into the hashmap.
   *
   */
  template <class... Args>
  inline bool emplace(Args&&... args) {
    return hashmap_.emplace(std::forward<Args>(args)...).second;
  }

  /**
   * @brief Get the mapping value of the given key.
   *
   */
  V& at(const K& key) { return hashmap_.at(key); }

  /**
   * @brief Get the const mapping value of the given key.
   *
   */
  const V& at(const K& key) const { return hashmap_.at(key); }

  /**
   * @brief Get the size of the hashmap.
   *
   */
  size_t size() const { return hashmap_.size(); }

  /**
   * @brief Reserve the size for the hashmap.
   *
   */
  void reserve(size_t size) { hashmap_.reserve(size); }

  /**
   * @brief Return the maximum possible size of the HashMap, i.e., the number
   * of elements that can be stored in the HashMap.
   *
   */
  size_t bucket_count() const { return hashmap_.bucket_count(); }

  /**
   * @brief Return the load factor of the HashMap.
   *
   */
  float load_factor() const { return hashmap_.load_factor(); }

  /**
   * @brief Check whether the hashmap is empty.
   *
   */
  bool empty() const { return hashmap_.empty(); }

  /**
   * @brief Return the beginning iterator.
   *
   */
  typename ska::flat_hash_map<K, V, H, E>::iterator begin() {
    return hashmap_.begin();
  }

  /**
   * @brief Return the const beginning iterator.
   *
   */
  typename ska::flat_hash_map<K, V, H, E>::const_iterator begin() const {
    return hashmap_.begin();
  }

  /**
   * @brief Return the const beginning iterator.
   *
   */
  typename ska::flat_hash_map<K, V, H, E>::const_iterator cbegin() const {
    return hashmap_.cbegin();
  }

  /**
   * @brief Return the ending iterator
   *
   */
  typename ska::flat_hash_map<K, V, H, E>::iterator end() {
    return hashmap_.end();
  }

  /**
   * @brief Return the const ending iterator.
   *
   */
  typename ska::flat_hash_map<K, V, H, E>::const_iterator end() const {
    return hashmap_.end();
  }

  /**
   * @brief Return the const ending iterator.
   *
   */
  typename ska::flat_hash_map<K, V, H, E>::const_iterator cend() const {
    return hashmap_.cend();
  }

  /**
   * @brief Find the value by key.
   *
   */
  typename ska::flat_hash_map<K, V, H, E>::iterator find(const K& key) {
    return hashmap_.find(key);
  }

  /**
   * @brief Associated with a given data buffer
   */
  void AssociateDataBuffer(std::shared_ptr<Blob> data_buffer) {
    this->data_buffer_ = data_buffer;
  }

  /**
   * @brief Build the hashmap object.
   *
   */
  Status Build(Client& client) override {
    using entry_t = typename Hashmap<K, V, H, E>::Entry;

    // shrink the size of hashmap
    hashmap_.shrink_to_fit();

    size_t entry_size =
        hashmap_.get_num_slots_minus_one() + hashmap_.get_max_lookups() + 1;
    auto entries_builder = std::make_shared<ArrayBuilder<entry_t>>(
        client, hashmap_.get_entries(), entry_size);

    this->set_num_slots_minus_one_(hashmap_.get_num_slots_minus_one());
    this->set_max_lookups_(hashmap_.get_max_lookups());
    this->set_num_elements_(hashmap_.size());
    this->set_entries_(std::static_pointer_cast<ObjectBase>(entries_builder));

    if (this->data_buffer_ != nullptr) {
      this->set_data_buffer_(
          reinterpret_cast<uintptr_t>(this->data_buffer_->data()));
      this->set_data_buffer_mapped_(this->data_buffer_);
    } else {
      this->set_data_buffer_(reinterpret_cast<uintptr_t>(nullptr));
      this->set_data_buffer_mapped_(Blob::MakeEmpty(client));
    }
    return Status::OK();
  }

 private:
  ska::flat_hash_map<K, V, H, E> hashmap_;
  std::shared_ptr<Blob> data_buffer_;
};

template <typename K, typename V>
class PerfectHashmapBuilder : public PerfectHashmapBaseBuilder<K, V> {
 public:
  typedef boomphf::SingleHashFunctor<K> hasher_t;

  explicit PerfectHashmapBuilder(Client& client)
      : PerfectHashmapBaseBuilder<K, V>(client) {}

  inline bool emplace(K key, V value) {
    vec_kv_.push_back(std::pair<K, V>(key, value));
    n_elements_++;
    return true;
  }

  /**
   * @brief Get the size of the hashmap.
   *
   */
  size_t size() const { return vec_kv_.size(); }

  /**
   * @brief Reserve the size for the hashmap.
   *
   */
  void reserve(size_t size) { vec_kv_.reserve(size); }

  template <typename K_ = K>
  typename std::enable_if<std::is_integral<K_>::value, void>::type Construct() {
    size_t count = 0;
    vec_kv_.resize(n_elements_);
    vec_k_.resize(vec_kv_.size());
    uint64_t start_time = GetCurrentTime();
    for (auto& kv_ : vec_kv_) {
      vec_k_[count] = kv_.first;
      count++;
    }
    VLOG(100) << "Constructing the vec_k_ takes "
              << GetCurrentTime() - start_time << " s.";

    auto data_iterator = boomphf::range(vec_k_.begin(), vec_k_.end());
    auto bphf = boomphf::mphf<K, hasher_t>(vec_k_.size(), data_iterator,
                                           concurrency_, 2.5f);

    vec_v_.resize(count);
    count = vec_k_.size() / concurrency_;
    start_time = GetCurrentTime();
    parallel_for(
        0, concurrency_,
        [&](const int i) {
          if (unlikely(i == concurrency_ - 1)) {
            for (size_t j = i * count; j < vec_v_.size(); j++) {
              vec_v_[bphf.lookup(vec_k_[j])] = vec_kv_[j].second;
            }
          } else {
            for (size_t j = i * count; j < (i + 1) * count; j++) {
              vec_v_[bphf.lookup(vec_k_[j])] = vec_kv_[j].second;
            }
          }
        },
        concurrency_);
    VLOG(100) << "Parallel for constructing the vec_v_ takes "
              << GetCurrentTime() - start_time << " s.";
  }

  template <typename K_ = K>
  typename std::enable_if<!std::is_integral<K_>::value, void>::type
  Construct() {
    VINEYARD_ASSERT(false, "Unsupported key type with perfect hash map.");
  }

  /**
   * @brief Build the hashmap object.
   *
   */
  Status Build(Client& client) override {
    Construct();

    auto ph_values_builder =
        std::make_shared<ArrayBuilder<V>>(client, vec_v_.data(), vec_v_.size());

    this->set_num_elements_(vec_kv_.size());

    this->set_ph_values_(
        std::static_pointer_cast<ObjectBase>(ph_values_builder));

    return Status::OK();
  }

 private:
  std::vector<V> vec_v_;
  std::vector<K> vec_k_;
  std::vector<std::pair<K, V>> vec_kv_;
  uint64_t n_elements_ = 0;

  int concurrency_ = std::thread::hardware_concurrency();
};

}  // namespace vineyard

#endif  // MODULES_BASIC_DS_HASHMAP_H_
