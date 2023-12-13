#include "zcm/zcm.h"
#include "zcm/zcm_private.h"
#include "zcm/blocking.h"
#include "zcm/transport.h"
#include "zcm/zcm_coretypes.h"
#include "zcm/util/topology.hpp"

#include "util/TimeUtil.hpp"
#include "util/debug.h"

#include <cassert>
#include <cstring>
#include <utility>

#include <unordered_map>
#include <vector>
#include <string>
#include <iostream>
#include <thread>
#include <mutex>
#include <regex>
#include <atomic>
using namespace std;

#define RECV_TIMEOUT 100

// Define a macro to set thread names. The function call is
// different for some operating systems
#ifdef __linux__
    #define SET_THREAD_NAME(name) pthread_setname_np(pthread_self(),name)
#elif __FreeBSD__ || __OpenBSD__
    #include <pthread_np.h>
    #define SET_THREAD_NAME(name) pthread_set_name_np(pthread_self(),name)
#elif __APPLE__ || __MACH__
    #define SET_THREAD_NAME(name) pthread_setname_np(name)
#else
    // If the OS is not in this list, don't call anything
    #define SET_THREAD_NAME(name)
#endif

static bool isRegexChannel(const string& channel)
{
    // These chars are considered regex
    auto isRegexChar = [](char c) {
        return c == '(' || c == ')' || c == '|' ||
        c == '.' || c == '*' || c == '+';
    };

    for (auto& c : channel)
        if (isRegexChar(c))
            return true;

    return false;
}

struct zcm_blocking
{
  private:
    // XXX If we change this to a linked list implementation, we can probably
    //     support subscribing/unsubscribing from within subscription callback handlers
    using SubList = vector<zcm_sub_t*>;

  public:
    zcm_blocking(zcm_t* z, zcm_trans_t* zt_);
    ~zcm_blocking();

    void run();
    void start();
    int stop();
    int handle(int timeout);

    int publish(const string& channel, const uint8_t* data, uint32_t len);
    zcm_sub_t* subscribe(const string& channel, zcm_msg_handler_t cb, void* usr, bool block);
    int unsubscribe(zcm_sub_t* sub, bool block);
    int flush();

    int writeTopology(string name);

  private:
    void recvThreadFunc();

    bool startRecvThread();

    void dispatchMsg(const zcm_msg_t& msg);
    int dispatchOneMsg(int timeout);

    bool deleteSubEntry(zcm_sub_t* sub, size_t nentriesleft);
    bool deleteFromSubList(SubList& slist, zcm_sub_t* sub);

    zcm_t* z;
    zcm_trans_t* zt;
    unordered_map<string, SubList> subs;
    unordered_map<string, SubList> subsRegex;
    size_t mtu;

    mutex receivedTopologyMutex;
    zcm::TopologyMap receivedTopologyMap;
    mutex sentTopologyMutex;
    zcm::TopologyMap sentTopologyMap;

    // These 2 mutexes used to implement a read-write style infrastructure on the subscription
    // lists. Both the recvThread and the message dispatch may read the subscriptions
    // concurrently, but subscribe() and unsubscribe() need both locks in order to write to it.
    mutex subDispMutex;
    mutex subRecvMutex;

    typedef enum {
        RECV_MODE_NONE = 0,
        RECV_MODE_RUN,
        RECV_MODE_SPAWN,
    } RecvMode_t;
    RecvMode_t recvMode {RECV_MODE_NONE};
    thread recvThread;
    atomic_bool done { false };

    typedef enum {
        THREAD_STATE_STOPPED = 0,
        THREAD_STATE_RUNNING,
        THREAD_STATE_HALTING,
        THREAD_STATE_HALTED,
    } ThreadState_t;
};

zcm_blocking_t::zcm_blocking(zcm_t* z_, zcm_trans_t* zt_)
{
    ZCM_ASSERT(z_->type == ZCM_BLOCKING);
    z = z_;
    zt = zt_;
    mtu = zcm_trans_get_mtu(zt);
}

zcm_blocking_t::~zcm_blocking()
{
    // Shutdown all threads
    stop();

    // Destroy the transport
    zcm_trans_destroy(zt);

    // Need to delete all subs
    for (auto& it : subs) {
        for (auto& sub : it.second) {
            delete sub;
        }
    }
    for (auto& it : subsRegex) {
        for (auto& sub : it.second) {
            delete (regex*) sub->regexobj;
            delete sub;
        }
    }
}

void zcm_blocking_t::run()
{
    if (recvMode != RECV_MODE_NONE) {
        ZCM_DEBUG("Err: call to run() when 'recvMode != RECV_MODE_NONE'");
        return;
    }
    recvMode = RECV_MODE_RUN;

    recvThreadFunc();
}

// TODO: should this call be thread safe?
void zcm_blocking_t::start()
{
    if (recvMode != RECV_MODE_NONE) {
        ZCM_DEBUG("Err: call to start() when 'recvMode != RECV_MODE_NONE'");
        return;
    }
    recvMode = RECV_MODE_SPAWN;

    recvThread = thread{&zcm_blocking::recvThreadFunc, this};
}

int zcm_blocking_t::stop()
{
    ZCM_ASSERT(recvMode == RECV_MODE_SPAWN);
    done = true;
    recvThread.join();
    recvMode = RECV_MODE_NONE;
    return ZCM_EOK;
}

int zcm_blocking_t::handle(int timeout)
{
    if (recvMode != RECV_MODE_NONE) return ZCM_EINVALID;
    return dispatchOneMsg(timeout);
}

int zcm_blocking_t::publish(const string& channel, const uint8_t* data, uint32_t len)
{
    // Check the validity of the request
    if (len > mtu) return ZCM_EINVALID;
    if (channel.size() > ZCM_CHANNEL_MAXLEN) return ZCM_EINVALID;

    zcm_msg_t msg = {
        .utime = TimeUtil::utime(),
        .channel = channel.c_str(),
        .len = len,
        // const cast ok as sendmsg guaranteed to not modify data
        .buf = (uint8_t*)data,
    };
    int ret = zcm_trans_sendmsg(zt, msg);
    if (ret != ZCM_EOK) ZCM_DEBUG("zcm_trans_sendmsg() returned error, dropping the msg!");

#ifdef TRACK_TRAFFIC_TOPOLOGY
    int64_t hashBE = 0, hashLE = 0;
    if (__int64_t_decode_array(data, 0, len, &hashBE, 1) == 8 &&
        __int64_t_decode_little_endian_array(data, 0, len, &hashLE, 1) == 8) {
        unique_lock<mutex> lk(sentTopologyMutex, defer_lock);
        if (lk.try_lock()) {
            sentTopologyMap[channel].emplace(hashBE, hashLE);
        }
    }
#endif

    return ZCM_EOK;
}

// Note: We use a lock on subscribe() to make sure it can be
// called concurrently. Without the lock, there is a race
// on modifying and reading the 'subs' and 'subsRegex' containers
zcm_sub_t* zcm_blocking_t::subscribe(const string& channel,
                                     zcm_msg_handler_t cb, void* usr,
                                     bool block)
{
    unique_lock<mutex> lk1(subDispMutex, std::defer_lock);
    unique_lock<mutex> lk2(subRecvMutex, std::defer_lock);
    if (block) {
        // Intentionally locking in this order
        lk1.lock();
        lk2.lock();
    } else if (!lk1.try_lock() || !lk2.try_lock()) {
        return nullptr;
    }
    int rc;

    rc = zcm_trans_recvmsg_enable(zt, channel.c_str(), true);

    if (rc != ZCM_EOK) {
        ZCM_DEBUG("zcm_trans_recvmsg_enable() didn't return ZCM_EOK: %d", rc);
        return nullptr;
    }

    zcm_sub_t* sub = new zcm_sub_t();
    ZCM_ASSERT(sub);
    strncpy(sub->channel, channel.c_str(), ZCM_CHANNEL_MAXLEN);
    sub->channel[ZCM_CHANNEL_MAXLEN] = '\0';
    sub->callback = cb;
    sub->usr = usr;
    sub->regex = isRegexChannel(channel);
    if (sub->regex) {
        sub->regexobj = (void*) new std::regex(sub->channel);
        ZCM_ASSERT(sub->regexobj);
        subsRegex[channel].push_back(sub);
    } else {
        sub->regexobj = nullptr;
        subs[channel].push_back(sub);
    }

    return sub;
}

// Note: We use a lock on unsubscribe() to make sure it can be
// called concurrently. Without the lock, there is a race
// on modifying and reading the 'subs' and 'subsRegex' containers
int zcm_blocking_t::unsubscribe(zcm_sub_t* sub, bool block)
{
    unique_lock<mutex> lk1(subDispMutex, std::defer_lock);
    unique_lock<mutex> lk2(subRecvMutex, std::defer_lock);
    if (block) {
        // Intentionally locking in this order
        lk1.lock();
        lk2.lock();
    } else if (!lk1.try_lock() || !lk2.try_lock()) {
        return ZCM_EAGAIN;
    }

    auto& subsSelected = sub->regex ? subsRegex : subs;

    auto it = subsSelected.find(sub->channel);
    if (it == subsSelected.end()) {
        ZCM_DEBUG("failed to find the subscription channel in unsubscribe()");
        return ZCM_EINVALID;
    }

    bool success = deleteFromSubList(it->second, sub);
    if (!success) {
        ZCM_DEBUG("failed to find the subscription entry in unsubscribe()");
        return ZCM_EINVALID;
    }

    return ZCM_EOK;
}

int zcm_blocking_t::flush()
{
    switch (recvMode) {
        case RECV_MODE_NONE:
            while (dispatchOneMsg(0));
            return ZCM_EOK;
        case RECV_MODE_RUN:
            return ZCM_EINVALID;
        case RECV_MODE_SPAWN:
            // RRR (Bendes): don't know how to handle this yet
            return ZCM_EINVALID;
    }
    return ZCM_EINVALID;
}

void zcm_blocking_t::recvThreadFunc()
{
    SET_THREAD_NAME("ZeroCM_receiver");
    while (!done) dispatchOneMsg(RECV_TIMEOUT);
}

int zcm_blocking_t::dispatchOneMsg(int timeout)
{
    zcm_msg_t msg;
    int rc = zcm_trans_recvmsg(zt, &msg, timeout);
    if (done) return ZCM_EINTR;
    if (rc != ZCM_EOK) return rc;
    {
        unique_lock<mutex> lk(subRecvMutex);

        // Check if message matches a non regex channel
        auto it = subs.find(msg.channel);
        if (it == subs.end()) {
            // Check if message matches a regex channel
            bool foundRegex = false;
            for (auto& it : subsRegex) {
                for (auto& sub : it.second) {
                    regex* r = (regex*)sub->regexobj;
                    if (regex_match(msg.channel, *r)) {
                        foundRegex = true;
                        break;
                    }
                }
                if (foundRegex) break;
            }
            // No subscription actually wants the message
            if (!foundRegex) return ZCM_EOK;
        }
    }
    dispatchMsg(msg);
    return ZCM_EOK;
}

void zcm_blocking_t::dispatchMsg(const zcm_msg_t& msg)
{
    zcm_recv_buf_t rbuf;
    rbuf.recv_utime = msg.utime;
    rbuf.zcm = z;
    rbuf.data = msg.buf;
    rbuf.data_size = msg.len;

    // Note: We use a lock on dispatch to ensure there is not
    // a race on modifying and reading the 'subs' container.
    // This means users cannot call zcm_subscribe or
    // zcm_unsubscribe from a callback without deadlocking.
    bool wasDispatched = false;
    {
        unique_lock<mutex> lk(subDispMutex);

        // dispatch to a non regex channel
        auto it = subs.find(msg.channel);
        if (it != subs.end()) {
            for (zcm_sub_t* sub : it->second) {
                sub->callback(&rbuf, msg.channel, sub->usr);
                wasDispatched = true;
            }
        }

        // dispatch to any regex channels
        for (auto& it : subsRegex) {
            for (auto& sub : it.second) {
                regex* r = (regex*)sub->regexobj;
                if (regex_match(msg.channel, *r)) {
                    sub->callback(&rbuf, msg.channel, sub->usr);
                    wasDispatched = true;
                }
            }
        }
    }

#ifdef TRACK_TRAFFIC_TOPOLOGY
    if (wasDispatched) {
        int64_t hashBE = 0, hashLE = 0;
        if (__int64_t_decode_array(msg.buf, 0, msg.len, &hashBE, 1) == 8 &&
            __int64_t_decode_little_endian_array(msg.buf, 0, msg.len, &hashLE, 1) == 8) {
            if (lk.try_lock()) {
                receivedTopologyMap[msg.channel].emplace(hashBE, hashLE);
            }
        }
    }
#else
    (void)wasDispatched; // get rid of compiler warning
#endif
}

bool zcm_blocking_t::deleteSubEntry(zcm_sub_t* sub, size_t nentriesleft)
{
    int rc = ZCM_EOK;
    if (sub->regex) {
        delete (std::regex*) sub->regexobj;
    }
    if (nentriesleft == 0) {
        rc = zcm_trans_recvmsg_enable(zt, sub->channel, false);
    }
    delete sub;
    return rc == ZCM_EOK;
}

bool zcm_blocking_t::deleteFromSubList(SubList& slist, zcm_sub_t* sub)
{
    for (size_t i = 0; i < slist.size(); i++) {
        if (slist[i] == sub) {
            // shrink the array by moving the last element
            size_t last = slist.size() - 1;
            slist[i] = slist[last];
            slist.resize(last);
            // delete the element
            return deleteSubEntry(sub, slist.size());
        }
    }
    return false;
}

int zcm_blocking_t::writeTopology(string name)
{
    decltype(receivedTopologyMap) _receivedTopologyMap;
    {
        unique_lock<mutex> lk(receivedTopologyMutex);
        _receivedTopologyMap = receivedTopologyMap;
    }
    decltype(sentTopologyMap) _sentTopologyMap;
    {
        unique_lock<mutex> lk(sentTopologyMutex);
        _sentTopologyMap = sentTopologyMap;
    }
    return zcm::writeTopology(name, _receivedTopologyMap, _sentTopologyMap);
}

/////////////// C Interface Functions ////////////////
extern "C" {

int zcm_blocking_try_create(zcm_blocking_t** zcm, zcm_t* z, zcm_trans_t* trans)
{
    if (z->type != ZCM_BLOCKING) return ZCM_EINVALID;

    *zcm = new zcm_blocking_t(z, trans);
    if (!*zcm) return ZCM_EMEMORY;

    return ZCM_EOK;
}

void zcm_blocking_destroy(zcm_blocking_t* zcm)
{
    if (zcm) delete zcm;
}

int zcm_blocking_publish(zcm_blocking_t* zcm, const char* channel, const uint8_t* data, uint32_t len)
{
    return zcm->publish(channel, data, len);
}

zcm_sub_t* zcm_blocking_subscribe(zcm_blocking_t* zcm, const char* channel,
                                  zcm_msg_handler_t cb, void* usr)
{
    return zcm->subscribe(channel, cb, usr, true);
}

int zcm_blocking_unsubscribe(zcm_blocking_t* zcm, zcm_sub_t* sub)
{
    return zcm->unsubscribe(sub, true);
}

void zcm_blocking_flush(zcm_blocking_t* zcm)
{
    zcm->flush();
}

void zcm_blocking_run(zcm_blocking_t* zcm)
{
    return zcm->run();
}

void zcm_blocking_start(zcm_blocking_t* zcm)
{
    return zcm->start();
}

void zcm_blocking_stop(zcm_blocking_t* zcm)
{
    zcm->stop();
}

int zcm_blocking_handle(zcm_blocking_t* zcm, int timeout)
{
    return zcm->handle(timeout);
}

int zcm_blocking_write_topology(zcm_blocking_t* zcm, const char* name)
{
#ifdef TRACK_TRAFFIC_TOPOLOGY
    return zcm->writeTopology(string(name));
#endif
    return ZCM_EINVALID;
}



/****************************************************************************/
/*    NOT FOR GENERAL USE. USED FOR LANGUAGE-SPECIFIC BINDINGS WITH VERY    */
/*                     SPECIFIC THREADING CONSTRAINTS                       */
/****************************************************************************/
zcm_sub_t* zcm_blocking_try_subscribe(zcm_blocking_t* zcm, const char* channel,
                                      zcm_msg_handler_t cb, void* usr)
{
    return zcm->subscribe(channel, cb, usr, false);
}

int zcm_blocking_try_unsubscribe(zcm_blocking_t* zcm, zcm_sub_t* sub)
{
    return zcm->unsubscribe(sub, false);
}

int zcm_blocking_try_flush(zcm_blocking_t* zcm)
{
    return zcm->flush();
}
/****************************************************************************/

}
