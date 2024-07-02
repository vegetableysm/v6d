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

#ifdef VINEYARD_WITH_RDMA

#include <sys/mman.h>

#include "common/rdma/rdma.h"
#include "common/rdma/util.h"

namespace vineyard {

size_t IRDMA::max_register_size_ = 0;
constexpr size_t min_l_size = 1;
constexpr size_t max_r_size = 64;

size_t IRDMA::GetMaxRegisterSizeImpl(void* addr, size_t min_size,
                                     size_t max_size, fid_domain* domain) {
  size_t l_size = min_size == 0 ? min_l_size : min_size;
  size_t r_size = max_size == 0 ? max_r_size : max_size;
  fid_mr* mr = nullptr;
  void* mr_desc = nullptr;
  uint64_t rkey = 0;
  void* buffer = addr;
  size_t register_size = 0;
  size_t max_buffer_size = r_size * 1024 * 1024 * 1024;

  if (addr == nullptr) {
    do {
      buffer = mmap(NULL, max_buffer_size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
      if (buffer == MAP_FAILED) {
        VLOG(100) << "map failed!";
        r_size /= 2;
        max_buffer_size = r_size * 1024 * 1024 * 1024;
      } else {
        break;
      }
    } while (max_buffer_size > 0);
    if (max_buffer_size == 0) {
      return 0;
    }
  }

  size_t size_ = (r_size + l_size) / 2;
  while (l_size < r_size - 1) {
    size_t buffer_size = size_ * 1024 * 1024 * 1024;
    VLOG(100) << "Register size: "
              << static_cast<double>(buffer_size) / 1024 / 1024 / 1024 << "GB";
    Status status =
        RegisterMemory(&mr, domain, buffer, buffer_size, rkey, mr_desc);
    if (status.ok()) {
      VLOG(100) << "Register memory size: " << buffer_size / 1024 / 1024 / 1024
                << "GB success";
      register_size = buffer_size;
      VINEYARD_CHECK_OK(CloseResource(mr, "memory region"));
      l_size = size_;
      size_ = (size_ + r_size) / 2;
    } else {
      VLOG(100) << "Register failed:" << status.message();
      r_size = size_;
      size_ = (size_ + l_size) / 2;
    }
  }

  if (addr == nullptr) {
    munmap(buffer, max_buffer_size);
  }

  /**
   * The memory registered by the rpc client may be not page aligned. So we need
   * to subtract the page size from the registered memory size to avoid the
   * memory registration failure.
   */
  return register_size - 4096;
}

Status IRDMA::RegisterMemory(fid_mr** mr, fid_domain* domain, void* address,
                             size_t size, uint64_t& rkey, void*& mr_desc) {
  struct fi_mr_attr mr_attr = {0};
  struct iovec iov = {0};
  iov.iov_base = address;
  iov.iov_len = size;
  mr_attr.mr_iov = &iov;
  mr_attr.iov_count = 1;
  // mr_attr.access = FI_REMOTE_READ | FI_REMOTE_WRITE | FI_READ | FI_WRITE;
  mr_attr.access = FI_REMOTE_WRITE | FI_WRITE | FI_RECV | FI_SEND;
  mr_attr.offset = 0;
  mr_attr.iface = FI_HMEM_SYSTEM;
  mr_attr.context = NULL;
	printf("attr->oiv_count:%d\n", mr_attr.iov_count);
	printf("attr->access:%llu\n", mr_attr.access);
	printf("attr->offset:%llu\n", mr_attr.offset);
	printf("attr->requested_key:%llu\n", mr_attr.requested_key);
	printf("attr->iface:%d\n", mr_attr.iface);
	printf("attr->context:%p\n", mr_attr.context);
  VLOG(100) << "Try to register memory region: size = " << size;

  int ret = fi_mr_regattr(domain, &mr_attr, FI_HMEM_SYSTEM, mr);
  if (ret == -FI_EIO) {
    return Status::IOError("Failed to register memory region:" +
                           std::to_string(ret));
  }
  CHECK_ERROR(!ret, "Failed to register memory region:" + std::to_string(ret));

  mr_desc = fi_mr_desc(*mr);

  rkey = fi_mr_key(*mr);

  return Status::OK();
}

Status IRDMA::Send(fid_ep* ep, fi_addr_t remote_fi_addr, void* buf, size_t size,
                   void* mr_desc, void* ctx) {
  POST(fi_send, "send", ep, buf, size, mr_desc, remote_fi_addr, ctx);
}

Status IRDMA::Recv(fid_ep* ep, fi_addr_t remote_fi_addr, void* buf, size_t size,
                   void* mr_desc, void* ctx) {
  POST(fi_recv, "receive", ep, buf, size, mr_desc, remote_fi_addr, ctx);
}

Status IRDMA::Read(fid_ep* ep, fi_addr_t remote_fi_addr, void* buf, size_t size,
                   uint64_t remote_address, uint64_t key, void* mr_desc,
                   void* ctx) {
  POST(fi_read, "read", ep, buf, size, mr_desc, remote_fi_addr, remote_address,
       key, ctx);
}

Status IRDMA::Write(fid_ep* ep, fi_addr_t remote_fi_addr, void* buf,
                    size_t size, uint64_t remote_address, uint64_t key,
                    void* mr_desc, void* ctx) {
  POST(fi_write, "write", ep, buf, size, mr_desc, remote_fi_addr,
       remote_address, key, ctx);
}

int IRDMA::GetCompletion(fid_cq* cq, int timeout, void** context) {
  fi_cq_err_entry err;
  timespec start, end;
  int ret;

  if (timeout > 0) {
    clock_gettime(CLOCK_REALTIME, &start);
  }

  while (true) {
    ret = fi_cq_read(cq, &err, 1);
    if (ret > 0) {
      break;
    } else if (ret < 0 && ret != -FI_EAGAIN) {
      return ret;
    } else if (timeout > 0) {
      clock_gettime(CLOCK_REALTIME, &end);
      if ((end.tv_sec - start.tv_sec) * 1000 +
              (end.tv_nsec - start.tv_nsec) / 1000000 >
          timeout) {
        return -FI_ETIMEDOUT;
      }
    }
  }
  if (context) {
    *context = err.op_context;
  }

  return 0;
}

void IRDMA::FreeInfo(fi_info* info) {
  if (info) {
    fi_freeinfo(info);
  }
}

}  // namespace vineyard

#endif
