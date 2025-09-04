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
    double globalTime;
    double pureTpCommTime;
    double pureTpFwCommTime;
    double pureTpBwCommTime;
    double purePpCommTime;
    double purePpFwCommTime;
    double purePpBwCommTime;
    double pureDpCommTime;
    double pureTotalCommTime;
    
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
    double globalTime;
    double pureTpCommTime;
    double pureTpFwCommTime;
    double pureTpBwCommTime;
    double purePpCommTime;
    double purePpFwCommTime;
    double purePpBwCommTime;
    double pureDpCommTime;
    double pureTotalCommTime;

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

    // 统计结果
    struct CommStats {
        double tpForward = 0;
        double tpBackward = 0;
        double ppForward = 0;
        double ppBackward = 0;
        double dpTotal = 0;
    };
    CommStats commStats;

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

#endif // SIMULATOR_H