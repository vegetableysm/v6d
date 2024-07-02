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
#include "server/async/rpc_server.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <utility>

#include "common/util/json.h"
#include "common/util/logging.h"  // IWYU pragma: keep
#include "server/server/vineyard_server.h"

namespace vineyard {

RPCServer::RPCServer(std::shared_ptr<VineyardServer> vs_ptr)
    : SocketServer(vs_ptr),
      rpc_spec_(vs_ptr_->GetSpec()["rpc_spec"]),
      acceptor_(vs_ptr_->GetContext()),
      socket_(vs_ptr_->GetContext()) {
  auto endpoint = getEndpoint(vs_ptr_->GetContext());
  acceptor_.open(endpoint.protocol());
  using reuse_port =
      asio::detail::socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>;
  // reuse address and port for rpc service.
  acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
  acceptor_.set_option(reuse_port(true));
  acceptor_.bind(endpoint);
  acceptor_.listen();
}

RPCServer::~RPCServer() {
  if (acceptor_.is_open()) {
    acceptor_.close();
  }

#ifdef VINEYARD_WITH_RDMA
  if (rdma_stop_) {
    return;
  }

  VINEYARD_DISCARD(rdma_server_->Stop());
  if (rdma_listen_thread_.joinable()) {
    rdma_listen_thread_.join();
  }
  if (rdma_recv_thread_.joinable()) {
    rdma_recv_thread_.join();
  }
  if (rdma_send_thread_.joinable()) {
    rdma_send_thread_.join();
  }
  VINEYARD_DISCARD(rdma_server_->Close());
  rdma_stop_ = true;
#endif
}

Status RPCServer::InitRDMA() {
#ifdef VINEYARD_WITH_RDMA
  std::string rdma_endpoint = RDMAEndpoint();
  size_t pos = rdma_endpoint.find(':');
  if (pos == std::string::npos) {
    return Status::Invalid("Invalid RDMA endpoint: " + rdma_endpoint);
  }
  uint32_t rdma_port = std::stoi(rdma_endpoint.substr(pos + 1));

  Status status = RDMAServer::Make(this->rdma_server_, rdma_port);
  if (status.ok()) {
    rdma_stop_ = false;
    rdma_listen_thread_ = std::thread([this]() { this->doRDMAAccept(); });
    rdma_recv_thread_ = std::thread([this]() { this->doRDMARecv(); });
    rdma_send_thread_ = std::thread([this]() { this->doRDMASend(); });
  } else {
    return Status::Invalid("Create rdma server failed! Error:" +
                           status.message());
  }

  info.address = reinterpret_cast<uint64_t>(vs_ptr_->GetBulkStore()->GetBasePointer());
  info.size = vs_ptr_->GetBulkStore()->GetBaseSize();
  status = rdma_server_->RegisterMemory(info);
  LOG(INFO) << "rkey:" << info.rkey;
  if (!status.ok()) {
    return Status::Invalid("Register memory failed! Error:" + status.message());
  }

  return Status::OK();
#else
  return Status::Invalid("RDMA is not supported in this build.");
#endif
}

void RPCServer::Start() {
  vs_ptr_->RPCReady();
  SocketServer::Start();
  LOG(INFO) << "Vineyard will listen on 0.0.0.0:"
            << rpc_spec_["port"].get<uint32_t>() << " for RPC";
  Status status = InitRDMA();
  if (status.ok()) {
    LOG(INFO) << "Vineyard will listen on " << RDMAEndpoint() << " for RDMA";
  } else {
    LOG(INFO) << "Init RDMA failed!" + status.message() + " Fall back to TCP.";
  }
}

asio::ip::tcp::endpoint RPCServer::getEndpoint(asio::io_context&) {
  uint32_t port = rpc_spec_["port"].get<uint32_t>();
  return asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port);
}

Status RPCServer::Register(std::shared_ptr<SocketConnection> conn,
                           const SessionID session_id) {
  if (session_id == RootSessionID()) {
    conn->registered_.store(true);
    return Status::OK();
  }
  std::shared_ptr<VineyardServer> session;
  auto status = this->vs_ptr_->GetRunner()->Get(session_id, session);
  if (status.ok()) {
    conn->switchSession(session);
    conn->registered_.store(true);
  }
  return status;
}

void RPCServer::doAccept() {
  if (!acceptor_.is_open()) {
    return;
  }
  auto self(shared_from_this());
  acceptor_.async_accept(socket_, [self](boost::system::error_code ec) {
    if (!ec) {
      std::lock_guard<std::recursive_mutex> scope_lock(
          self->connections_mutex_);
      if (self->stopped_.load() || self->closable_.load()) {
        return;
      }
      std::shared_ptr<SocketConnection> conn =
          std::make_shared<SocketConnection>(std::move(self->socket_),
                                             self->vs_ptr_, self,
                                             self->next_conn_id_);
      conn->Start();
      self->connections_.emplace(self->next_conn_id_, conn);
      ++self->next_conn_id_;
    }
    // don't continue when the iocontext being cancelled.
    if (!ec || ec != boost::system::errc::operation_canceled) {
      if (!self->stopped_.load() || !self->closable_.load()) {
        self->doAccept();
      }
    }
  });
}

void RPCServer::doRDMASend() {
#ifdef VINEYARD_WITH_RDMA
  while (1) {
    void* context = nullptr;
    Status status = rdma_server_->GetTXCompletion(-1, &context);
    if (!status.ok()) {
      if (rdma_server_->IsStopped()) {
        VLOG(100) << "RDMA server stopped!";
        return;
      }
      VLOG(100) << "Get TX completion failed! Error:" << status.message();
      VLOG(100) << "Retry...";
    } else {
      // handle message
      VineyardSendContext* send_context =
          reinterpret_cast<VineyardSendContext*>(context);
      if (!send_context) {
        LOG(ERROR) << "Bad send context! Discard msg!";
        continue;
      }
      if (send_context->attr.msg_buffer) {
        rdma_server_->ReleaseTXBuffer(send_context->attr.msg_buffer);
      }
      delete send_context;
    }
  }
#endif
}

#ifdef VINEYARD_WITH_RDMA
void RPCServer::doVineyardRequestMemory(VineyardRecvContext* recv_context,
                                        VineyardMsg* recv_msg) {
  VLOG(100) << "Receive vineyard request mem!";
  RegisterMemInfo remote_request_mem_info;
  remote_request_mem_info.address = recv_msg->remoteMemInfo.remote_address;
  uint64_t max_register_size = recv_msg->remoteMemInfo.len;
  remote_request_mem_info.size = recv_msg->remoteMemInfo.len;
  VLOG(100) << "Receive remote request address: "
            << reinterpret_cast<void*>(remote_request_mem_info.address)
            << " size: " << remote_request_mem_info.size;

  // Register mem
  // Status status;
  // while (true) {
  //   status = rdma_server_->RegisterMemory(remote_request_mem_info);
  //   if (status.ok()) {
  //     break;
  //   }
  //   if (status.IsIOError()) {
  //     // probe the max register size again
  //     VLOG(100) << "Probe the max register size again.";
  //     while (true) {
  //       size_t size = rdma_server_->GetServerMaxRegisterSize(
  //           reinterpret_cast<void*>(remote_request_mem_info.address), 1,
  //           max_register_size / 1024 / 1024 / 1024);
  //       if (size > 0) {
  //         max_register_size = size;
  //         break;
  //       }
  //     }
  //     remote_request_mem_info.size =
  //         std::min(recv_msg->remoteMemInfo.len, max_register_size);
  //   } else {
  //     break;
  //   }
  // }
  // if (!status.ok()) {
  //   LOG(ERROR) << "Failed to register mem.";
  //   void* msg = nullptr;
  //   rdma_server_->GetTXFreeMsgBuffer(msg);
  //   VineyardMsg* send_msg = reinterpret_cast<VineyardMsg*>(msg);
  //   send_msg->type = VINEYARD_MSG_REQUEST_MEM;
  //   send_msg->remoteMemInfo.remote_address = 0;
  //   send_msg->remoteMemInfo.key = -1;
  //   send_msg->remoteMemInfo.len = 0;

  //   VineyardSendContext* send_context = new VineyardSendContext();
  //   memset(&send_context->attr, 0, sizeof(send_context->attr));
  //   send_context->attr.msg_buffer = msg;
  //   rdma_server_->Send(recv_context->rdma_conn_id,
  //                      recv_context->attr.msg_buffer, sizeof(VineyardMsg),
  //                      recv_context);
  //   rdma_server_->Recv(recv_context->rdma_conn_id,
  //                      reinterpret_cast<void*>(recv_msg), sizeof(VineyardMsg),
  //                      reinterpret_cast<void*>(recv_context));
  //   return;
  // }
  remote_request_mem_info.rkey = info.rkey;

  VLOG(100) << "Register memory"
            << " address: "
            << reinterpret_cast<void*>(remote_request_mem_info.address)
            << " size: " << remote_request_mem_info.size
            << " rkey: " << remote_request_mem_info.rkey
            << " mr_desc: " << remote_request_mem_info.mr_desc
            << " fid_mr:" << remote_request_mem_info.mr;

  void* msg = nullptr;
  rdma_server_->GetTXFreeMsgBuffer(msg);
  VineyardMsg* send_msg = reinterpret_cast<VineyardMsg*>(msg);
  send_msg->type = VINEYARD_MSG_REQUEST_MEM;
  send_msg->remoteMemInfo.remote_address =
      (uint64_t) remote_request_mem_info.address;
  send_msg->remoteMemInfo.key = remote_request_mem_info.rkey;
  send_msg->remoteMemInfo.len = remote_request_mem_info.size;

  VineyardSendContext* send_context = new VineyardSendContext();
  memset(&send_context->attr, 0, sizeof(send_context->attr));
  send_context->attr.msg_buffer = msg;

  rdma_server_->Send(recv_context->rdma_conn_id, msg, sizeof(VineyardMsg),
                     send_context);
  VLOG(100) << "Send key:" << remote_request_mem_info.rkey << " send address:"
            << reinterpret_cast<void*>(remote_request_mem_info.address)
            << " size: " << remote_request_mem_info.size;

  std::lock_guard<std::recursive_mutex> scope_lock(this->rdma_mutex_);
  remote_mem_infos_[recv_context->rdma_conn_id].insert(remote_request_mem_info);
  rdma_server_->Recv(recv_context->rdma_conn_id,
                     reinterpret_cast<void*>(recv_msg), sizeof(VineyardMsg),
                     reinterpret_cast<void*>(recv_context));
}

void RPCServer::doVineyardReleaseMemory(VineyardRecvContext* recv_context,
                                        VineyardMsg* recv_msg) {
  VLOG(100) << "Receive release msg!";
  // RegisterMemInfo remote_request_mem_info;
  // remote_request_mem_info.address = recv_msg->remoteMemInfo.remote_address;
  // // remote_register_mem_info.rkey = recv_msg->remoteMemInfo.key;
  // remote_request_mem_info.size = recv_msg->remoteMemInfo.len;
  // VLOG(100) << "Receive release address: "
  //           << reinterpret_cast<void*>(remote_request_mem_info.address);
  // if (remote_mem_infos_.find(recv_context->rdma_conn_id) ==
  //     remote_mem_infos_.end()) {
  //   LOG(ERROR) << "Receive release mem info from unknown connection!";
  //   return;
  // }
  // if (remote_mem_infos_[recv_context->rdma_conn_id].find(
  //         remote_request_mem_info) ==
  //     remote_mem_infos_[recv_context->rdma_conn_id].end()) {
  //   LOG(ERROR) << "Receive release mem info from unknown address!";
  //   rdma_server_->Recv(recv_context->rdma_conn_id,
  //                      reinterpret_cast<void*>(recv_msg), sizeof(VineyardMsg),
  //                      reinterpret_cast<void*>(recv_context));
  //   return;
  // }

  // remote_request_mem_info = *remote_mem_infos_[recv_context->rdma_conn_id].find(
  //     remote_request_mem_info);

  // // Deregister mem
  // VLOG(100) << "Deregister memory"
  //           << " address: "
  //           << reinterpret_cast<void*>(remote_request_mem_info.address)
  //           << " size: " << remote_request_mem_info.size
  //           << " rkey: " << remote_request_mem_info.rkey
  //           << " mr_desc: " << remote_request_mem_info.mr_desc
  //           << " fid_mr:" << remote_request_mem_info.mr;
  // VINEYARD_CHECK_OK(rdma_server_->DeregisterMemory(remote_request_mem_info));
  // remote_mem_infos_[recv_context->rdma_conn_id].erase(remote_request_mem_info);
  // VLOG(100) << "Wait next request.";
  rdma_server_->Recv(recv_context->rdma_conn_id,
                     reinterpret_cast<void*>(recv_msg), sizeof(VineyardMsg),
                     recv_context);
}

void RPCServer::doVineyardClose(VineyardRecvContext* recv_context) {
  VLOG(100) << "Receive close msg!";
  rdma_server_->CloseConnection(recv_context->rdma_conn_id);

  std::lock_guard<std::recursive_mutex> scope_lock(this->rdma_mutex_);
  remote_mem_infos_.erase(recv_context->rdma_conn_id);
  rdma_server_->ReleaseRXBuffer(recv_context->attr.msg_buffer);

  delete recv_context;
}
#endif

void RPCServer::doRDMARecv() {
#ifdef VINEYARD_WITH_RDMA
  while (1) {
    void* context = nullptr;
    Status status = rdma_server_->GetRXCompletion(-1, &context);
    if (!status.ok()) {
      if (rdma_server_->IsStopped()) {
        VLOG(100) << "RDMA server stopped!";
        return;
      }
      VLOG(100) << "Get RX completion failed! Error:" << status.message();
      VLOG(100) << "Retry...";
    } else {
      // handle message
      VineyardRecvContext* recv_context =
          reinterpret_cast<VineyardRecvContext*>(context);
      if (!recv_context) {
        LOG(ERROR) << "Bad recv context! Discard msg!";
        continue;
      }

      VineyardMsg* recv_msg =
          reinterpret_cast<VineyardMsg*>(recv_context->attr.msg_buffer);
      if (recv_msg->type == VINEYARD_MSG_REQUEST_MEM) {
        boost::asio::post(vs_ptr_->GetContext(),
                          [this, recv_context, recv_msg] {
                            doVineyardRequestMemory(recv_context, recv_msg);
                          });
      } else if (recv_msg->type == VINEYARD_RELEASE_MEM) {
        boost::asio::post(vs_ptr_->GetContext(),
                          [this, recv_context, recv_msg] {
                            doVineyardReleaseMemory(recv_context, recv_msg);
                          });
      } else if (recv_msg->type == VINEYARD_MSG_CLOSE) {
        boost::asio::post(vs_ptr_->GetContext(), [this, recv_context] {
          doVineyardClose(recv_context);
        });
      } else {
        LOG(ERROR) << "Unknown message type: " << recv_msg->type;
      }
    }
  }
#endif
}

void RPCServer::doRDMAAccept() {
#ifdef VINEYARD_WITH_RDMA
  while (1) {
    uint64_t rdma_conn_id;
    Status status = rdma_server_->WaitConnect(rdma_conn_id);
    if (!status.ok()) {
      VLOG(100) << "Wait rdma connect failed! Close! Error:"
                << status.message();
      return;
    }
    VineyardRecvContext* recv_context = new VineyardRecvContext();
    memset(&recv_context->attr, 0, sizeof(recv_context->attr));
    recv_context->rdma_conn_id = rdma_conn_id;
    void* context = reinterpret_cast<void*>(recv_context);
    void* msg = nullptr;
    rdma_server_->GetRXFreeMsgBuffer(msg);
    recv_context->attr.msg_buffer = msg;
    rdma_server_->Recv(rdma_conn_id, msg, sizeof(VineyardMsg), context);
  }
#endif
}

}  // namespace vineyard
