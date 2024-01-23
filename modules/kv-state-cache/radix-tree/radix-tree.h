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

#ifndef RADIX_TREE_H
#define RADIX_TREE_H

#include "radix.h"

#include "common/util/base64.h"
#include "common/util/logging.h"
#include "kv-state-cache/strategy/LRU_strategy.h"
#include "lz4.h"

#include <iomanip>
#include <map>
#include <memory>
#include <vector>

using namespace vineyard;

typedef struct nodeData {
  int data_length;
  void* data;
} nodeData;

class Node {
 private:
  nodeData* data;
  raxNode* node;

 public:
  Node(raxNode* node) {
    this->data = (nodeData*) raxGetData(node);
    this->node = node;
  }

  Node(nodeData* data) {
    this->data = data;
    this->node = NULL;
  }

  void set_data(void* data, int data_length) {
    if (this->node == NULL) {
      LOG(INFO) << "set data failed, node is null";
      return;
    }
    this->data->data = data;
    this->data->data_length = data_length;
    raxSetData(this->node, this->data);
  }

  void* get_data() { return this->data->data; }

  int get_data_length() { return this->data->data_length; }
};

class RadixTree;

class NodeWithTreeAttri {
 private:
  std::shared_ptr<Node> node;
  std::shared_ptr<RadixTree> belong_to;

 public:
  NodeWithTreeAttri(std::shared_ptr<Node> node,
                    std::shared_ptr<RadixTree> belong_to) {
    this->node = node;
    this->belong_to = belong_to;
  }

  std::shared_ptr<Node> get_node() { return node; }

  std::shared_ptr<RadixTree> get_tree() { return belong_to; }
};

class RadixTree : public std::enable_shared_from_this<RadixTree> {
 private:
  void* custom_data;
  int custom_data_length;
  // the whole radix tree for prefix match
  rax* tree;
  int cache_capacity;
  int node_count;

 public:
  RadixTree(int cache_capacity) {
    LOG(INFO) << "init radix tree";
    this->tree = raxNew();
    this->custom_data = NULL;
    this->custom_data_length = 0;
    this->cache_capacity = cache_capacity;
    this->node_count = 0;
  }

  RadixTree(void* custom_data, int custom_data_length, int cache_capacity) {
    LOG(INFO) << "init radix tree with custom data";
    this->tree = raxNew();
    this->custom_data = custom_data;
    this->custom_data_length = custom_data_length;
    this->cache_capacity = cache_capacity;
    this->node_count = 0;
  }

  ~RadixTree() {
    // TBD
    // free all the node and the whole tree.
  }

  std::shared_ptr<NodeWithTreeAttri> Insert(
      std::vector<int> tokens,
      std::shared_ptr<NodeWithTreeAttri>& evicted_node) {
    // get the sub vector of the tokens
    // TBD
    // change the api to Insert(prefix, tokens, evicted_node);
    std::vector<int> prefix =
        std::vector<int>(tokens.begin(), tokens.end() - 1);
    if (prefix.size() > 0 && Query(prefix) == nullptr) {
      return nullptr;
    }

    // insert the token vector to the radix tree
    int* insert_tokens_array = tokens.data();
    size_t insert_tokens_array_len = tokens.size();
    nodeData* dummy_data = new nodeData();
    nodeData* old_data;
    raxNode* dataNode = NULL;
    int retval = raxInsertAndReturnDataNode(
        this->tree, insert_tokens_array, insert_tokens_array_len, dummy_data,
        (void**) &dataNode, (void**) &old_data);
    if (dataNode == NULL) {
      LOG(INFO) << "insert failed";
      return NULL;
    }
    LOG(INFO) << "insert success";
    if (retval == 1) {
      node_count++;
    }

    raxShow(this->tree);
    if (this->node_count > this->cache_capacity) {
      LOG(INFO) << "cache capacity is full, evict the last recent node";
      // evict the last recent node (the node with the largest lru index-
      std::vector<int> evicted_tokens;
      raxFindLastRecentNode(this->tree->head, evicted_tokens);
      std::string evicted_str = "";
      for (int i = 0; i < evicted_tokens.size(); i++) {
        evicted_str += std::to_string(evicted_tokens[i]);
      }
      this->Delete(evicted_tokens, evicted_node);
    }
    dataNode = raxFindAndReturnDataNode(this->tree, insert_tokens_array,
                                        insert_tokens_array_len);
    /**
     * if the data node is null, it means the evicted node is the same node as
     * the inserted node.
     */
    if (dataNode == NULL) {
      LOG(INFO) << "get failed";
      return NULL;
    }

    return std::make_shared<NodeWithTreeAttri>(std::make_shared<Node>(dataNode),
                                               shared_from_this());
  }

  void Delete(std::vector<int> tokens,
              std::shared_ptr<NodeWithTreeAttri>& evicted_node) {
    // remove the token vector from the radix tree
    int* delete_tokens_array = tokens.data();
    size_t delete_tokens_array_len = tokens.size();

    nodeData* old_data;
    int retval = raxRemove(this->tree, delete_tokens_array,
                           delete_tokens_array_len, (void**) &old_data);
    if (retval == 1) {
      LOG(INFO) << "remove success";
      std::shared_ptr<Node> node = std::make_shared<Node>(old_data);
      evicted_node =
          std::make_shared<NodeWithTreeAttri>(node, shared_from_this());
      node_count--;
    } else {
      LOG(INFO) << "remove failed";
    }
  }

  std::shared_ptr<NodeWithTreeAttri> Query(std::vector<int> key) {
    LOG(INFO) << "Query";
    int* tokens = key.data();
    size_t tokens_len = key.size();

    LOG(INFO) << "Query with tokens_len:" << tokens_len;
    if (this->tree == nullptr) {
      LOG(INFO) << "WTF!";
      return NULL;
    }

    raxNode* dataNode =
        raxFindAndReturnDataNode(this->tree, tokens, tokens_len);
    if (dataNode == NULL) {
      LOG(INFO) << "get failed";
      return NULL;
    }
    LOG(INFO) << "get success";

    // refresh the lru cache
    std::shared_ptr<Node> node = std::make_shared<Node>(dataNode);

    return std::make_shared<NodeWithTreeAttri>(node, shared_from_this());
  }

  std::string Serialize() {
    raxShow(this->tree);
    std::vector<std::vector<int>> token_list;
    std::vector<void*> data_list;
    raxSerialize(this->tree, token_list, data_list);

    // the string format is:
    // [token list] [data hex string]\n
    // E.g
    // tokens_len | tokens | data | tokens_len | tokens | data .....
    std::string serialized_str;
    std::ostringstream oss;
    for (size_t i = 0; i < token_list.size(); i++) {
      int tokens_len = (int) token_list[i].size();
      oss << ((char*) &tokens_len)[0] << ((char*) &tokens_len)[1]
          << ((char*) &tokens_len)[2] << ((char*) &tokens_len)[3];
      LOG(INFO) << "tokens:";
      for (size_t j = 0; j < token_list[i].size(); j++) {
        int token = token_list[i][j];
        oss << ((char*) &token)[0] << ((char*) &token)[1] << ((char*) &token)[2]
            << ((char*) &token)[3];
        LOG(INFO) << token_list[i][j];
      }

      int data_len = ((nodeData*) data_list[i])->data_length;
      oss << ((char*) &data_len)[0] << ((char*) &data_len)[1]
          << ((char*) &data_len)[2] << ((char*) &data_len)[3];
      for (int j = 0; j < ((nodeData*) data_list[i])->data_length; ++j) {
        oss << ((char*) ((nodeData*) data_list[i])->data)[j];
      }
    }
    serialized_str = oss.str();
    for (int i = 0; i < serialized_str.length(); i++) {
      LOG(INFO) << (int) serialized_str[i];
    }

    // use LZ4 to compress the serialized string
    const char* const src = serialized_str.c_str();
    const int src_size = serialized_str.size();
    const int max_dst_size = LZ4_compressBound(src_size);
    char* compressed_data = new char[max_dst_size];
    if (compressed_data == NULL) {
      LOG(INFO) << "Failed to allocate memory for *compressed_data.";
    }

    const int compressed_data_size =
        LZ4_compress_default(src, compressed_data, src_size, max_dst_size);
    if (compressed_data_size <= 0) {
      LOG(INFO) << "A 0 or negative result from LZ4_compress_default() "
                   "indicates a failure trying to compress the data. ";
    }

    if (compressed_data_size > 0) {
      LOG(INFO) << "We successfully compressed some data! Ratio: "
                << ((float) compressed_data_size / src_size);
    }

    if (compressed_data == NULL) {
      LOG(INFO) << "Failed to re-alloc memory for compressed_data.  Sad :(";
    }

    std::string compressed_str =
        std::string(compressed_data, compressed_data_size);
    std::string result =
        std::string((char*) &src_size, sizeof(int)) + compressed_str;
    delete[] compressed_data;
    return result;
  }

  static std::shared_ptr<RadixTree> Deserialize(std::string data_str) {
    // use LZ4 to decompress the serialized string
    int src_size = *(int*) data_str.c_str();
    data_str.erase(0, sizeof(int));
    char* const decompress_buffer = new char[src_size];
    if (decompress_buffer == NULL) {
      LOG(INFO) << "Failed to allocate memory for *decompress_buffer.";
    }

    const int decompressed_size = LZ4_decompress_safe(
        data_str.c_str(), decompress_buffer, data_str.size(), src_size);
    if (decompressed_size < 0) {
      LOG(INFO) << "A negative result from LZ4_decompress_safe indicates a "
                   "failure trying to decompress the data.  See exit code "
                   "(echo $?) for value returned.";
    }
    if (decompressed_size >= 0) {
      LOG(INFO) << "We successfully decompressed some data!";
    }
    data_str = std::string(decompress_buffer, decompressed_size);
    delete[] decompress_buffer;

    for (int i = 0; i < data_str.length(); i++) {
      LOG(INFO) << (int) data_str[i];
    }

    std::vector<std::vector<int>> token_list;
    std::vector<void*> data_list;
    std::vector<size_t> data_size_list;

    // tokens_len | tokens | data | tokens_len | tokens | data .....
    void* data = (void*) data_str.c_str();
    int data_length = data_str.length();
    int i = 0;
    while (i < data_length) {
      int token_len = *(int*) ((char*) data + i);
      i += sizeof(int);
      std::vector<int> tokens;
      for (int j = 0; j < token_len; j++) {
        int token = *(int*) ((char*) data + i);
        i += sizeof(int);
        tokens.push_back(token);
      }

      int data_len = *(int*) ((char*) data + i);
      i += sizeof(int);
      void* node_data = new char[data_len];
      memcpy(node_data, (char*) data + i, data_len);
      i += data_len;

      token_list.push_back(tokens);
      data_list.push_back(node_data);
      data_size_list.push_back(data_len);
    }

    // This pointer will be freed by upper layer. Because this data
    // is created by upper layer. Here just recover it from serialized
    // string.
    std::shared_ptr<RadixTree> radix_tree = std::make_shared<RadixTree>(10);
    for (int i = 0; i < token_list.size(); i++) {
      for (int j = 0; j < token_list[i].size(); j++) {
        LOG(INFO) << token_list[i][j];
      }
    }
    for (int i = 0; i < token_list.size(); i++) {
      std::shared_ptr<NodeWithTreeAttri> evicted_node;
      std::shared_ptr<NodeWithTreeAttri> node =
          radix_tree->Insert(token_list[i], evicted_node);
      node->get_node()->set_data(data_list[i], data_size_list[i]);
    }
    return radix_tree;
  }

  std::shared_ptr<RadixTree> Split(std::vector<int> tokens) {
    nodeData* dummy_data = new nodeData();
    raxNode* sub_tree_root_node =
        raxSplit(this->tree, tokens.data(), tokens.size(), dummy_data);

    std::shared_ptr<RadixTree> sub_tree =
        std::make_shared<RadixTree>(this->cache_capacity);
    sub_tree->tree = this->tree;
    rax* sub_rax = raxNew();
    sub_rax->head = sub_tree_root_node;
    return sub_tree;
  }

  // Get child node list from this tree.
  static std::vector<std::shared_ptr<NodeWithTreeAttri>>
  TraverseTreeWithoutSubTree(std::shared_ptr<RadixTree> radix_tree) {
    std::vector<std::shared_ptr<NodeWithTreeAttri>> nodes;
    if (radix_tree == NULL) {
      LOG(INFO) << "traverse failed";
      return nodes;
    }

    std::vector<std::shared_ptr<raxNode>> dataNodeList;
    raxNode* headNode = radix_tree->tree->head;
    raxTraverseSubTree(headNode, dataNodeList);
    for (size_t i = 0; i < dataNodeList.size(); i++) {
      nodes.push_back(std::make_shared<NodeWithTreeAttri>(
          std::make_shared<Node>(dataNodeList[i].get()), radix_tree));
    }
    return nodes;
  }

  void* GetCustomData() { return custom_data; }

  void SetCustomData(void* custom_data, int custom_data_length) {
    this->custom_data = custom_data;
    this->custom_data_length = custom_data_length;
  }
};

#endif
