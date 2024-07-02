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

#ifndef SRC_SERVER_ASYNC_RPC_SERVER_H_
#define SRC_SERVER_ASYNC_RPC_SERVER_H_

#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include "common/rdma/rdma_server.h"
#include "common/rdma/util.h"
#include "common/util/asio.h"  // IWYU pragma: keep
#include "common/util/env.h"
#include "server/async/socket_server.h"

namespace vineyard {

class VineyardServer;

/**
 * @brief A kind of server that supports remote procedure call (RPC)
 *
 */
class RPCServer : public SocketServer,
                  public std::enable_shared_from_this<RPCServer> {
 public:
  explicit RPCServer(std::shared_ptr<VineyardServer> vs_ptr);

  ~RPCServer() override;

  void Start() override;

  std::string Endpoint() {
    return get_hostname() + ":" + json_to_string(rpc_spec_["port"]);
  }

  std::string RDMAEndpoint() {
    std::string rdma_endpoint = json_to_string(rpc_spec_["rdma_endpoint"]);
    rdma_endpoint.erase(
        std::remove(rdma_endpoint.begin(), rdma_endpoint.end(), '\"'),
        rdma_endpoint.end());
    return std::string(rdma_endpoint);
  }

  Status Register(std::shared_ptr<SocketConnection> conn,
                  const SessionID session_id) override;

 private:
  asio::ip::tcp::endpoint getEndpoint(asio::io_context&);

  void doAccept() override;

  void doRDMAAccept();

  void doRDMARecv();

  void doRDMASend();

  Status InitRDMA();

#ifdef VINEYARD_WITH_RDMA
  void doVineyardRequestMemory(VineyardRecvContext* recv_context,
                               VineyardMsg* recv_msg);

  void doVineyardReleaseMemory(VineyardRecvContext* recv_context,
                               VineyardMsg* recv_msg);

  void doVineyardClose(VineyardRecvContext* recv_context);
#endif

  const json rpc_spec_;
  asio::ip::tcp::acceptor acceptor_;
  asio::ip::tcp::socket socket_;

  // connection id to rdma server
#ifdef VINEYARD_WITH_RDMA
  std::unordered_map<uint64_t,
                     std::set<RegisterMemInfo, CompareRegisterMemInfo>>
      remote_mem_infos_;
  std::shared_ptr<RDMAServer> rdma_server_;
  mutable std::recursive_mutex rdma_mutex_;  // protect `rdma_servers_`

  std::thread rdma_listen_thread_;
  std::thread rdma_recv_thread_;
  std::thread rdma_send_thread_;
  bool rdma_stop_ = true;
  RegisterMemInfo info;
#endif
};

}  // namespace vineyard

#endif  // SRC_SERVER_ASYNC_RPC_SERVER_H_
