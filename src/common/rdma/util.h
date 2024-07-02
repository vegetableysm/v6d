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

#ifndef SRC_COMMON_RDMA_UTIL_H_
#define SRC_COMMON_RDMA_UTIL_H_

#ifdef VINEYARD_WITH_RDMA

#include <rdma/fabric.h>
#include <string>
#include <chrono>
#include <rdma/fi_eq.h>

#include "common/util/logging.h"
#include "common/util/status.h"

extern "C" {

#define CHECK_ERROR(condition, message) \
  if (!(condition)) {                   \
    return Status::Invalid(message);    \
  }

#define POST(post_fn, op_str, ...)                                 \
  do {                                                             \
    int ret;                                                       \
    while (1) {                                                    \
      ret = post_fn(__VA_ARGS__);                                  \
      if (!ret) {                                                  \
        return Status::OK();                                       \
      }                                                            \
                                                                   \
      if (ret != -FI_EAGAIN) {                                     \
        LOG(ERROR) << op_str << " " << ret;                        \
        std::string msg = "Failed to post " + std::string(op_str); \
        return Status::Invalid(msg);                               \
      }                                                            \
      usleep(1000);                                                \
    }                                                              \
  } while (0)

enum VINEYARD_MSG_OPT {
  VINEYARD_MSG_CONNECT,
  VINEYARD_MSG_EXCHANGE_KEY,
  VINEYARD_MSG_REQUEST_MEM,
  VINEYARD_RELEASE_MEM,
  VINEYARD_MSG_CLOSE,
};

struct VineyardMsg {
  union {
    struct {
      uint64_t remote_address;
      uint64_t len;
      uint64_t key;
    } remoteMemInfo;
    struct {
      bool isReady;
    } ConnectState;
  };
  int type;
};

struct VineyardRecvContext {
  uint64_t rdma_conn_id;
  struct {
    void* msg_buffer;
  } attr;
};

struct VineyardSendContext {
  struct {
    void* msg_buffer;
  } attr;
};

struct RegisterMemInfo {
  uint64_t address;
  size_t size;
  uint64_t rkey;
  void* mr_desc;
  // TODO:
  // Work around for rpc client. Remove this field in the future
  fid_mr* mr;
};

struct CompareRegisterMemInfo {
  bool operator()(const RegisterMemInfo& a, const RegisterMemInfo& b) const {
    return a.address < b.address;
  }
};

enum RDMA_STATE {
  INIT,
  READY,
  STOPED,
};

struct RDMARemoteNodeInfo {
  fi_info* fi;
  fid_fabric* fabric;
  fid_domain* domain;
};

#define VINEYARD_FIVERSION FI_VERSION(1, 20)

static int ft_spin_for_comp_(struct fid_cq *cq, uint64_t *cur,
			    uint64_t total, int timeout)
{
	struct fi_cq_err_entry comp;
	struct timespec a, b;
	int ret;

	if (timeout >= 0)
		clock_gettime(CLOCK_MONOTONIC, &a);

  auto start_ = std::chrono::high_resolution_clock::now();
	do {
		ret = fi_cq_read(cq, &comp, 1);
		if (ret > 0) {
			if (timeout >= 0)
				clock_gettime(CLOCK_MONOTONIC, &a);
			(*cur)++;
		} else if (ret < 0 && ret != -FI_EAGAIN) {
			return ret;
		} else if (timeout >= 0) {
			clock_gettime(CLOCK_MONOTONIC, &b);
			if ((b.tv_sec - a.tv_sec) > timeout) {
				fprintf(stderr, "%ds timeout expired\n", timeout);
				return -FI_ENODATA;
			}
		}
	} while (total - *cur > 0);
    auto end_ = std::chrono::high_resolution_clock::now();
    auto duration_ = std::chrono::duration_cast<std::chrono::microseconds>(end_ - start_);
    LOG(INFO) << "fi_cq_read duration: " << duration_.count() << "us";

	return 0;
}

static int ft_read_cq_(struct fid_cq *cq, uint64_t *cur,
		uint64_t total, int timeout)
{
	return ft_spin_for_comp_(cq, cur, total, timeout);
}

static int ft_get_cq_comp_(struct fid_cq *cq, uint64_t *cur,
		    uint64_t total, int timeout)
{
	int ret;

	ret = ft_read_cq_(cq, cur, total, timeout);

	if (ret) {
		if (ret == -FI_EAVAIL) {
      return -1;
			(*cur)++;
		} else {
			return -1;
		}
	}
	return ret;
}

}  // extern "C"

#endif

#endif  // SRC_COMMON_RDMA_UTIL_H_
