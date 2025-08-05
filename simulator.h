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

    int handleEvents();
    double stableTime();
    void progress(double time);
    void printStates() ;
    void addEvent(int from, int mb);
};

class RankTask : public Task {
public:
    Rank* rank;
    RankTask(Rank* rank) ;

    GroupTask* ppFwdGroupTask;
    GroupTask* ppBwdGroupTask;
    GroupTask* dpGroupTask;
    GroupTask* tpGroupTask;

    RankState state;
    int microbatch;
    double remainingTime;
    vector<tuple<int, int, int>> events; // < EP, TYPE, MB >

    int handleEvents();
    double stableTime();
    void progress(double time);
    void printStates();
    void addEvent(int ep, int type, int mb);
    bool isFirstRankInPipeline() const;
    bool isLastRankInPipeline() const;
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
    void run() ;
    SimResult py_run() ;

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