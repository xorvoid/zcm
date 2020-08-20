#include "zcm/transport.h"
#include "zcm/transport_registrar.h"
#include "zcm/transport_register.hpp"
#include "zcm/util/debug.h"
#include "zcm/util/SharedPage.hpp"

#include <iostream>
#include <string>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>
#include <memory>

#define ZCM_TRANS_CLASSNAME TransportSharedMem
#define MTU (1<<28)
#define SHMEM_CHANNEL_NAME_PREFIX "zcm-channel-shmem-"

using namespace std;

struct ZCM_TRANS_CLASSNAME : public zcm_trans_t
{
    struct Msg
    {
        uint64_t pubId;
        size_t len;
        uint8_t buf[MTU];
        char channel[ZCM_CHANNEL_MAXLEN + 1];
    };

    unique_ptr<SharedPage> wildcardPage;
    Msg *wildcardMsg = nullptr;

    string subnet;

    uint64_t lastPubId = 0;
    uint64_t lastReceivedPubId = 0;

    char inFlightChanMem[ZCM_CHANNEL_MAXLEN + 1] = {};
    uint8_t inFlightDataMem[MTU] = {};

    ZCM_TRANS_CLASSNAME(zcm_url_t *url)
    {
        trans_type = ZCM_BLOCKING;
        vtbl = &methods;

        subnet = zcm_url_address(url);
        subnet = "zcm-shmem" + (subnet == "" ? "" : "-" + subnet);
        ZCM_DEBUG("Shmem Subnet Address: %s\n", subnet.c_str());

        wildcardPage.reset(SharedPage::newPage(subnet.c_str(), sizeof(Msg)));
        assert(wildcardPage);
        wildcardMsg = wildcardPage->getBuf<Msg>();
    }

    ~ZCM_TRANS_CLASSNAME() {}

    bool good() { return true; }

    /********************** METHODS **********************/
    size_t get_mtu() { return MTU; }

    int sendmsg(zcm_msg_t msg)
    {
        wildcardMsg->pubId = ++lastPubId;
        strncpy(wildcardMsg->channel, msg.channel, ZCM_CHANNEL_MAXLEN);
        wildcardMsg->len = msg.len;
        memcpy(wildcardMsg->buf, msg.buf, msg.len);
        return ZCM_EOK;
    }

    int recvmsg_enable(const char *channel, bool enable) { return ZCM_EOK; }

    int recvmsg(zcm_msg_t *msg, int timeout)
    {
        if (wildcardMsg->pubId == lastReceivedPubId) {
            usleep(1e4);
            return ZCM_EAGAIN;
        }
        lastReceivedPubId = wildcardMsg->pubId;
        msg->channel = inFlightChanMem;
        msg->buf = inFlightDataMem;
        strncpy(inFlightChanMem, wildcardMsg->channel, ZCM_CHANNEL_MAXLEN);
        msg->len = wildcardMsg->len;
        memcpy(inFlightDataMem, wildcardMsg->buf, wildcardMsg->len);
        return ZCM_EOK;
    }

    /********************** STATICS **********************/
    static zcm_trans_methods_t methods;
    static ZCM_TRANS_CLASSNAME *cast(zcm_trans_t *zt)
    {
        assert(zt->vtbl == &methods);
        return (ZCM_TRANS_CLASSNAME*)zt;
    }

    static size_t _get_mtu(zcm_trans_t *zt)
    { return cast(zt)->get_mtu(); }

    static int _sendmsg(zcm_trans_t *zt, zcm_msg_t msg)
    { return cast(zt)->sendmsg(msg); }

    static int _recvmsg_enable(zcm_trans_t *zt, const char *channel, bool enable)
    { return cast(zt)->recvmsg_enable(channel, enable); }

    static int _recvmsg(zcm_trans_t *zt, zcm_msg_t *msg, int timeout)
    { return cast(zt)->recvmsg(msg, timeout); }

    static void _destroy(zcm_trans_t *zt)
    { delete cast(zt); }

    static const TransportRegister regBlocking;
    static const TransportRegister regNonblocking;
};

zcm_trans_methods_t ZCM_TRANS_CLASSNAME::methods = {
    &ZCM_TRANS_CLASSNAME::_get_mtu,
    &ZCM_TRANS_CLASSNAME::_sendmsg,
    &ZCM_TRANS_CLASSNAME::_recvmsg_enable,
    &ZCM_TRANS_CLASSNAME::_recvmsg,
    NULL,
    &ZCM_TRANS_CLASSNAME::_destroy,
};

static zcm_trans_t *create(zcm_url_t *url)
{ return new ZCM_TRANS_CLASSNAME(url); }

#ifdef USING_TRANS_SHMEM
const TransportRegister ZCM_TRANS_CLASSNAME::regBlocking(
    "shmem", "Blocking shared memory transport (e.g. 'shmem://<namespace>')", create);
#endif
