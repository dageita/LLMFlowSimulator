#include <string>
#include <vector> 

#define DEBUG_LOG 1  // 控制调试日志开关

#if DEBUG_LOG
#define LOG_STATE_TRANSITION(rank, from, to, mb) \
    cout << "[STATE] Rank " << (rank) << " transition from " << (from) << " to " << (to) \
         << " at time " << simulator->globalTime << " microbatch " << (mb) << endl
#define LOG_EVENT(rank, event, mb) \
    cout << "[EVENT] Rank " << (rank) << " processing event " << (event) \
         << " at time " << simulator->globalTime << " microbatch " << (mb) << endl
#else
#define LOG_STATE_TRANSITION(rank, from, to, mb)
#define LOG_EVENT(rank, event, mb)
#endif

#ifndef COMMON_H
#define COMMON_H

enum NodeType {
    HOST,
    TOR,
    AGG,
    CORE,
    GPU,
    NVLINK_SWITCH,
    PCIE_SWITCH,
    NIC,
};

enum RankState {
    PP_WAIT,
    COMPUTE,
    TP_COMM,
    DP_WAIT,
    DP_COMM,
    DONE,
};

enum GroupType {
    TP,
    PP, 
    DP,
    NONE_GROUP  // 添加缺失的枚举值
};

enum EndpointType {
    SENT,
    RECV,
    NONE_ENDPOINT  // 修改名称避免冲突
};

enum EventType {
    COMPUTE_FWD,
    COMPUTE_BWD,
    TP_COMM_FWD,
    TP_COMM_BWD,
    PP_COMM_FWD, 
    PP_COMM_BWD,
    DP_COMM_EVENT
};

#endif