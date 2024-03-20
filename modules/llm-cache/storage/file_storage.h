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

#ifndef MODULES_LLM_CACHE_STORAGE_FILE_STORAGE_H_
#define MODULES_LLM_CACHE_STORAGE_FILE_STORAGE_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/util/status.h"
#include "llm-cache/storage/storage.h"

namespace vineyard {

struct FileDescriptor {};

struct FileHeader {
  int prefixNum;
  int layer;
  int kvStateSize;
};

enum FilesystemType {
  LOCAL,
};

enum FileOperationType {
  READ = 1,
  WRITE = 1 << 1,
};

class FileStorage : public IStorage {
 private:
  Status DefaultGetPathFromPrefix(const std::vector<int>& tokenList,
                                  int batchSize,
                                  std::vector<std::string>& path);

  bool CompareTokenList(const std::vector<int>& tokenList,
                        const std::vector<int>& tokenList2, size_t length);

  void CloseCache() override {}

  virtual Status Open(std::string path, std::shared_ptr<FileDescriptor>& fd,
                      FileOperationType fileOperationType) = 0;

  virtual Status Seek(std::shared_ptr<FileDescriptor>& fd, size_t offset) = 0;

  virtual Status Read(std::shared_ptr<FileDescriptor>& fd, void* data,
                      size_t size) = 0;

  virtual Status Write(std::shared_ptr<FileDescriptor>& fd, const void* data,
                       size_t size) = 0;

  virtual Status GetFileSize(std::shared_ptr<FileDescriptor>& fd,
                             size_t& size) = 0;

  virtual Status GetCurrentPos(std::shared_ptr<FileDescriptor>& fd,
                               size_t& pos) = 0;

  virtual Status Flush(std::shared_ptr<FileDescriptor>& fd) = 0;

  virtual Status Close(std::shared_ptr<FileDescriptor>& fd) = 0;

 public:
  FileStorage() = default;

  FileStorage(size_t batchSize, std::string rootPath)
      : batchSize(batchSize), rootPath(rootPath) {}

  ~FileStorage() = default;

  Status Update(const std::vector<int>& tokenList,
                const std::vector<std::map<int, std::pair<LLMKV, LLMKV>>>&
                    kvStateList) override;

  Status Update(const std::vector<int>& tokenList, int nextToken,
                const std::map<int, std::pair<LLMKV, LLMKV>>& kvState) override;

  Status Query(const std::vector<int>& tokenList,
               std::vector<std::map<int, std::pair<LLMKV, LLMKV>>>& kvStateList)
      override;

  Status Query(const std::vector<int>& tokenList, int nextToken,
               std::map<int, std::pair<LLMKV, LLMKV>>& kvState) override;

 protected:
  size_t batchSize;
  std::string rootPath;
};

}  // namespace vineyard
#endif  // MODULES_LLM_CACHE_STORAGE_FILE_STORAGE_H_
