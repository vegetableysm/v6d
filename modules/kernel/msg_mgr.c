#include <linux/stat.h>
#include <linux/atmioc.h>
#include <linux/slab.h>
#include <linux/vmalloc.h>
#include <linux/netlink.h>
#include <net/sock.h>
#include "vineyard_fs.h"
#include "vineyard_i.h"
#include "msg_mgr.h"

MODULE_LICENSE("GPL");
MODULE_AUTHOR("yuansm");
MODULE_DESCRIPTION("Vineyard filesystem for Linux.");
MODULE_VERSION("0.01");

#define NETLINK_VINEYARD 22
#define USER_PORT        100

struct sock *nl_socket = NULL;
extern struct net init_net;

DECLARE_WAIT_QUEUE_HEAD(vineyard_msg_wait);

void vineyard_spin_lock(volatile int *addr)
{
    while(!__sync_bool_compare_and_swap(addr, 0, 1));
}

void vineyard_spin_unlock(volatile int *addr)
{
    *addr = 0;
}

int send_msg(void *pbuf, uint16_t len)
{
    struct sk_buff *nl_skb;
    struct nlmsghdr *nlh;
    int ret;

    nl_skb = nlmsg_new(len, GFP_ATOMIC);
    if (!nl_skb) {
        printk(KERN_INFO PREFIX "Netlink alloc failure\n");
    }

    nlh = nlmsg_put(nl_skb, 0, 0, NETLINK_VINEYARD, len, 0);
    if (!nlh) {
        printk(KERN_INFO PREFIX "nlmsg_put failure\n");
        nlmsg_free(nl_skb);
        return -1;
    }
    memcpy(nlmsg_data(nlh), pbuf, len);
    ret = netlink_unicast(nl_socket, nl_skb, USER_PORT, MSG_DONTWAIT);

    return ret;
}

static inline int msg_empty(int head, int tail)
{
    return head == tail;
}

static inline int msg_full(int head, int tail, int capacity)
{
    if (tail == capacity) {
        return head == 0;
    }
    return head == (tail - 1);
} 

static void handle_init(void)
{
    struct vineyard_kern_user_msg msg;

    msg.opt = VSET;
    msg.request_mem = (unsigned long)vineyard_msg_mem_header;
    msg.result_mem = (unsigned long)vineyard_result_mem_header;

    send_msg(&msg, sizeof(msg));
}

static void handle_wait(void)
{
    int ret;
    struct vineyard_kern_user_msg msg;

    // TODO: add signal handler
    do {
        ret = wait_event_interruptible(vineyard_msg_wait, (!msg_empty(vineyard_msg_mem_header->head_point, vineyard_msg_mem_header->tail_point)) | vineyard_msg_mem_header->close);
    } while (ret != 0);

    if (vineyard_msg_mem_header->close) {
        msg.opt = VEXIT;
    } else {
        msg.opt = VFOPT;
    }
    send_msg(&msg, sizeof(msg));
    printk(KERN_INFO PREFIX "wait end!\n");
}

static void handle_fopt(void)
{
    printk(KERN_INFO PREFIX "fopt is not support now!\n");
    //1. send handler result to user

    //2. call handler_wait
    handle_wait();
}

static void netlink_rcv_msg(struct sk_buff *skb)
{
    struct nlmsghdr *nlh = NULL;
    struct vineyard_kern_user_msg *umsg = NULL;

    // wake_up(&vineyard_fs_wait);

    // printk(KERN_INFO PREFIX "vineyardd thread wait\n");
    // wait_event_interruptible(vineyard_msg_wait, vineyard_msg_mem_header->has_msg);
    // printk(KERN_INFO PREFIX "vineyardd thread wake\n");

    // printk(KERN_INFO PREFIX "process pid:%d\n", current->pid);

    // vineyard_spin_lock(&vineyard_msg_mem_header->lock);
    // vineyard_msg_mem_header->has_msg = 0;

    // if (vineyard_msg_mem_header->close) {
    //     vineyard_spin_lock(&vineyard_result_mem_header->lock);
    //     vineyard_result_mem_header->has_msg = 1;
    //     vineyard_spin_unlock(&vineyard_result_mem_header->lock);
    //     send_msg("bye user", strlen("bye user"));
    //     return;
    // }
    // vineyard_spin_unlock(&vineyard_msg_mem_header->lock);

    if (skb->len >= nlmsg_total_size(0)) {
        nlh = nlmsg_hdr(skb);
        umsg = (struct vineyard_kern_user_msg *)NLMSG_DATA(nlh);
        if (umsg)
        {
            printk(KERN_INFO PREFIX "Kernel recv from user %s\n", (char *)umsg);

            switch(umsg->opt) {
            case VINIT:
                printk(KERN_INFO PREFIX "Receive opt: VINT\n");
                handle_init();
                break;
            case VWAIT:
                printk(KERN_INFO PREFIX "Receive opt: VWAIT\n");
                handle_wait();
                break;
            case VFOPT:
                printk(KERN_INFO PREFIX "Receive opt: VFOPT\n");
                handle_fopt();
                break;
            default:
                printk(KERN_INFO PREFIX "error!\n");
                break;
            }
        }
    }

    // vineyard_spin_lock(&vineyard_result_mem_header->lock);
    // vineyard_result_mem_header->has_msg = 1;
    // vineyard_spin_unlock(&vineyard_result_mem_header->lock);
}

struct netlink_kernel_cfg cfg = {
    .input = netlink_rcv_msg,
};

int net_link_init(void)
{
    nl_socket = (struct sock *)netlink_kernel_create(&init_net, NETLINK_VINEYARD, &cfg);
    if (!nl_socket) {
        printk(KERN_INFO PREFIX "Create netlink error!\n");
        return -1;
    }

    return 0;
}

void net_link_release(void)
{
    if (nl_socket) {
        netlink_kernel_release(nl_socket);
        nl_socket = NULL;
    }
}