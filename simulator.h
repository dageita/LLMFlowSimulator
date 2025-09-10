#ifndef SIMULATOR_H
#define SIMULATOR_H

#include "workload.h"
#include "topology.h"

#include <vector>
#include <string>
#include <iostream>
#include <limits>
#include <map>
#include <tuple>
#include <set>
#include <unordered_set>

using namespace std;


class Task;
class GroupTask;
class RankTask;
class Collective;
class Flow;
class Simulator;
class Node;
class Link;
class Rank;
class Connection;
class Group;
class Workload;
class Topology;

// 全局microbatch状态管理器
class MicrobatchManager {
public:
    struct MicrobatchState {
        int mb;
        double globalTime; // 全局microbatch时间
        
        MicrobatchState() : mb(0), globalTime(0.0) {}
        MicrobatchState(int microbatch) : mb(microbatch), globalTime(0.0) {}
    };
    
    // 全局microbatch时间（用于全局同步）
    static map<int, MicrobatchState> microbatchStates;
    
    // 每个rank的microbatch完成时间（用于PP通信）
    static map<pair<int, int>, double> rankMicrobatchTimes; // <rank_id, mb> -> time
    
    // 全局microbatch时间管理
    static void updateMicrobatchGlobalTime(int mb, double time);
    static double getMicrobatchGlobalTime(int mb);
    
    // rank-specific microbatch时间管理
    static void updateRankMicrobatchTime(int rankId, int mb, double time);
    static double getRankMicrobatchTime(int rankId, int mb);
    
    static void printStates();
};


class Task {
public:
    virtual int handleEvents() = 0;
    virtual double stableTime() = 0;
    virtual void progress(double time) = 0;
    virtual void printStates() = 0;
};

class Flow {
public:
    Node* src;
    Node* dst;
    vector<Node*> path;
    vector<Link*> pathLinks;    
    Flow(Connection* connection);

    double remainingSize;
    double throughput;
    Collective* collective;

    double stableTime();
    void progress(double time);
};

class Collective {
public:
    vector<Flow*> flows;
    Group* group;

    int microbatch;
    int accumulatedInvocations;
    int accumulatedSize;

    double stableTime();
    void progress(double time);

    Collective(Group* group, int microbatch, int accumulatedSize);

    void printStates();
};

class GroupTask : public Task {
public:

    Group* group;
    GroupTask(Group* group) ;

    vector<RankTask*> senders;
    vector<RankTask*> receivers;
    Collective* activeCollective;
    vector<Collective*> waitingCollectives;
    map<int, Collective*> accumulatingCollectives; // from microbatchId to collective
    unordered_set<int> completedMbs;
    vector<tuple<int, int>> events;    // < From, MB >
    double groupGlobalTime; // group的全局时间，基于发送和接收方rank的时间

    int handleEvents();
    double stableTime();
    void progress(double time);
    void printStates() ;
    void addEvent(int from, int mb);
    void updateGroupGlobalTime(int mb = 0, int fromRank = -1);
};

class RankTask : public Task {
public:
    Rank* rank;
    RankTask(Rank* rank) ;

    GroupTask* ppFwdGroupTask;
    GroupTask* ppBwdGroupTask;
    GroupTask* dpGroupTask;
    GroupTask* tpFwdGroupTask;
    GroupTask* tpBwdGroupTask;

    RankState state;
    double remainingTime;
    double rankGlobalTime; // 每个rank的独立全局时间
    // 事件结构：< EP, EVENTTYPE, MB, STATE, REMAINING_TIME, START_TIME, END_TIME >
    vector<tuple<int, int, int, RankState, double, double, double>> events;

    int microbatch; // 当前处理的 microbatch（正值表示正向，负值表示反向）
    set<int> completedFwdMicrobatches;  // 已完成正向的 microbatch
    set<int> completedBwdMicrobatches;   // 已完成反向的 microbatch
    
    // 跟踪上一个处理的事件，用于时间步进决策
    int lastProcessedMb; // 上一个处理的microbatch

    int handleEvents();
    double stableTime();
    void progress(double time);
    void printStates();
    void addEvent(int ep, int eventType, int mb, RankState state = PP_WAIT, double remainingTime = 0, double startTime = 0, double endTime = 0);
    bool isFirstRankInPipeline() const;
    bool isLastRankInPipeline() const;
    int getNextMicrobatch();
    bool isAllMicrobatchesDone() const;
    void updateGlobalState();
    double calculateNewRankGlobalTime(double time, int mb, double eventStartTime);
    double calculateHandleEventsTime(int mb);
    void updateLastProcessedMb(int mb);
};

struct SimResult {
    // 全局时间统计
    double globalTime;  // timelineEvents的截止时间
    
    // 新的通信时间统计字段 - 按batch统计（考虑时间重叠）
    double batchTpFwCommTime;
    double batchTpBwCommTime;
    double batchPpFwCommTime;
    double batchPpBwCommTime;
    double batchDpCommTime;
    double batchTpCommTime;   // TP通信总时间（前向+后向，时间轴去重）
    double batchPpCommTime;   // PP通信总时间（前向+后向，时间轴去重）
    double totalCommTime;  // 所有3D并行通信的时间轴重叠去重总时间
    
    // 新的通信时间统计字段 - 按microbatch统计
    double microbatchTpFwCommTime;
    double microbatchTpBwCommTime;
    double microbatchPpFwCommTime;
    double microbatchPpBwCommTime;
    double microbatchDpCommTime;
    
    // Timeline事件列表，每个事件包含 [rank, event_type, microbatch, start_time, end_time]
    struct TimelineEventData {
        int rank;
        string eventType;
        int microbatch;
        double startTime;
        double endTime;
    };
    vector<TimelineEventData> timelineEvents;
};

class Simulator {
public:
    Workload* workload;
    Topology* topology;

    vector<Task*> tasks;

    void initialize();
    void updateStates(); // waiter filling
    void updateLinkStates(std::set<Flow*>& activeFlows, std::set<Link*>& activeLinks);
    bool isSimulationDone();
    // void run();
    SimResult py_run();

    void printStates();
    void print() ;
    string getTimelineJSON();

    // 时间戳记录
    struct CommEvent {
        int rankId;
        int microbatch;
        GroupType type;
        EndpointType endpoint;
        double startTime;
        double endTime;
        bool isForward() const { return microbatch > 0; }
    };
    vector<CommEvent> commEvents;


    struct TimelineEvent {
        int rank;
        GroupType groupType;  // 修改字段名避免冲突
        EndpointType endpoint;
        EventType eventType;  // 修改字段名避免冲突
        int microbatch;
        double startTime;
        double endTime;
        bool isForward() const { return microbatch > 0; }
        string info;
    };

    vector<TimelineEvent> timelineEvents; // 存储所有事件
    void recordTimelineEvent(int rank, GroupType groupType, EndpointType endpoint, EventType eventType, int microbatch, double startTime, double endTime, const std::string& info = "");

    string groupTypeToString(GroupType type);
    string eventTypeToString(EventType type);
    string endpointToString(EndpointType endpoint);
    string stateToString(RankState state);

};

// 通信时间分析器类
class CommunicationTimeAnalyzer {
public:
    struct MicrobatchStats {
        double tpForwardTime = 0;
        double tpBackwardTime = 0;
        double ppForwardTime = 0;
        double ppBackwardTime = 0;
        double dpTime = 0;
    };
    
    struct BatchStats {
        double tpForwardTime = 0;
        double tpBackwardTime = 0;
        double ppForwardTime = 0;
        double ppBackwardTime = 0;
        double dpTime = 0;
        double tpCommTime = 0;   // TP通信总时间（前向+后向，时间轴去重）
        double ppCommTime = 0;   // PP通信总时间（前向+后向，时间轴去重）
        // 添加每个microbatch的统计数据
        map<int, MicrobatchStats> microbatchStats;
    };
    
    // 按batch统计通信时间（考虑时间重叠），同时保留每个microbatch的统计数据
    BatchStats analyzePerBatch(const vector<Simulator::TimelineEvent>& events);
    
    // 计算所有3D并行通信的总时间（考虑时间重叠）
    double calculateTotalCommTime(const vector<Simulator::TimelineEvent>& events);
    
private:
    // 计算指定类型通信的非重叠时间
    double calculateNonOverlappingTime(const vector<Simulator::TimelineEvent>& events, GroupType type, bool isForward);
    
    // 辅助函数：合并重叠的时间区间并计算总时间
    double mergeIntervals(vector<pair<double, double>>& intervals);
};

#endif // SIMULATOR_H