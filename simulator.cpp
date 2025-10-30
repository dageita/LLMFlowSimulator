#include "common.h"
#include "simulator.h"

#include <limits>
#include <string>
#include <vector>
#include <iostream>
#include <cassert>
#include <typeinfo>

#include <nlohmann/json.hpp>
#include <algorithm>

using namespace std;

extern Topology* topology;
extern Workload* workload;
extern Simulator* simulator;

// CommunicationTimeAnalyzer 实现
CommunicationTimeAnalyzer::BatchStats CommunicationTimeAnalyzer::analyzePerBatch(const vector<Simulator::TimelineEvent>& events) {
    BatchStats batchStats;
    
    // 用于存储各种类型通信的时间区间，用于batch级别的去重计算
    vector<pair<double, double>> tpForwardIntervals;
    vector<pair<double, double>> tpBackwardIntervals;
    vector<pair<double, double>> ppForwardIntervals;
    vector<pair<double, double>> ppBackwardIntervals;
    vector<pair<double, double>> dpIntervals;
    
    // 用于存储每个microbatch的各种类型通信的时间区间
    map<int, vector<pair<double, double>>> mbTpForwardIntervals;
    map<int, vector<pair<double, double>>> mbTpBackwardIntervals;
    map<int, vector<pair<double, double>>> mbPpForwardIntervals;
    map<int, vector<pair<double, double>>> mbPpBackwardIntervals;
    map<int, vector<pair<double, double>>> mbDpIntervals;
    
    // 一次遍历完成所有统计
    for (const auto& event : events) {
        // 只处理通信事件
        if (event.groupType != GroupType::TP && event.groupType != GroupType::PP && event.groupType != GroupType::DP) {
            continue;
        }
        
        int mb = abs(event.microbatch);
        bool isForward = event.microbatch > 0;
        
        // 收集时间区间用于batch级别的去重计算
        pair<double, double> interval = {event.startTime, event.endTime};
        
        if (event.groupType == GroupType::TP) {
            if (isForward) {
                tpForwardIntervals.push_back(interval);
                mbTpForwardIntervals[mb].push_back(interval);
            } else {
                tpBackwardIntervals.push_back(interval);
                mbTpBackwardIntervals[mb].push_back(interval);
            }
        } else if (event.groupType == GroupType::PP) {
            if (isForward) {
                ppForwardIntervals.push_back(interval);
                mbPpForwardIntervals[mb].push_back(interval);
            } else {
                ppBackwardIntervals.push_back(interval);
                mbPpBackwardIntervals[mb].push_back(interval);
            }
        } else if (event.groupType == GroupType::DP) {
            dpIntervals.push_back(interval);
            mbDpIntervals[mb].push_back(interval);
        }
    }
    
    // 计算batch级别的去重时间
    batchStats.tpForwardTime = mergeIntervals(tpForwardIntervals);
    batchStats.tpBackwardTime = mergeIntervals(tpBackwardIntervals);
    batchStats.ppForwardTime = mergeIntervals(ppForwardIntervals);
    batchStats.ppBackwardTime = mergeIntervals(ppBackwardIntervals);
    batchStats.dpTime = mergeIntervals(dpIntervals);
    
    // 计算TP和PP的总通信时间（前向+后向，时间轴去重）
    vector<pair<double, double>> tpAllIntervals;
    tpAllIntervals.insert(tpAllIntervals.end(), tpForwardIntervals.begin(), tpForwardIntervals.end());
    tpAllIntervals.insert(tpAllIntervals.end(), tpBackwardIntervals.begin(), tpBackwardIntervals.end());
    batchStats.tpCommTime = mergeIntervals(tpAllIntervals);
    
    vector<pair<double, double>> ppAllIntervals;
    ppAllIntervals.insert(ppAllIntervals.end(), ppForwardIntervals.begin(), ppForwardIntervals.end());
    ppAllIntervals.insert(ppAllIntervals.end(), ppBackwardIntervals.begin(), ppBackwardIntervals.end());
    batchStats.ppCommTime = mergeIntervals(ppAllIntervals);
    
    // 计算每个microbatch的去重时间
    for (const auto& [mb, intervals] : mbTpForwardIntervals) {
        batchStats.microbatchStats[mb].tpForwardTime = mergeIntervals(const_cast<vector<pair<double, double>>&>(intervals));
    }
    for (const auto& [mb, intervals] : mbTpBackwardIntervals) {
        batchStats.microbatchStats[mb].tpBackwardTime = mergeIntervals(const_cast<vector<pair<double, double>>&>(intervals));
    }
    for (const auto& [mb, intervals] : mbPpForwardIntervals) {
        batchStats.microbatchStats[mb].ppForwardTime = mergeIntervals(const_cast<vector<pair<double, double>>&>(intervals));
    }
    for (const auto& [mb, intervals] : mbPpBackwardIntervals) {
        batchStats.microbatchStats[mb].ppBackwardTime = mergeIntervals(const_cast<vector<pair<double, double>>&>(intervals));
    }
    for (const auto& [mb, intervals] : mbDpIntervals) {
        batchStats.microbatchStats[mb].dpTime = mergeIntervals(const_cast<vector<pair<double, double>>&>(intervals));
    }
    
    return batchStats;
}

// 辅助函数：合并重叠的时间区间并计算总时间
double CommunicationTimeAnalyzer::mergeIntervals(vector<pair<double, double>>& intervals) {
    if (intervals.empty()) {
        return 0.0;
    }
    
    // 排序并合并重叠区间
    sort(intervals.begin(), intervals.end());
    
    vector<pair<double, double>> merged;
    for (const auto& interval : intervals) {
        if (merged.empty() || merged.back().second < interval.first) {
            merged.push_back(interval);
        } else { 
            merged.back().second = max(merged.back().second, interval.second);
        }
    }
    
    // 计算总时间
    double totalTime = 0;
    for (const auto& interval : merged) {
        totalTime += (interval.second - interval.first);
    }
    
    return totalTime;
}

double CommunicationTimeAnalyzer::calculateNonOverlappingTime(const vector<Simulator::TimelineEvent>& events, GroupType type, bool isForward) {
    vector<pair<double, double>> intervals;
    
    // 提取指定类型和方向的通信事件
    for (const auto& event : events) {
        if (event.groupType == type) {
            // 对于DP通信，不区分前后向
            if (type == GroupType::DP || (event.microbatch > 0) == isForward) {
                intervals.push_back({event.startTime, event.endTime});
            }
        }
    }
    
    if (intervals.empty()) {
        return 0.0;
    }
    
    // 排序并合并重叠区间
    sort(intervals.begin(), intervals.end());
    
    vector<pair<double, double>> merged;
    for (const auto& interval : intervals) {
        if (merged.empty() || merged.back().second < interval.first) {
            merged.push_back(interval);
        } else { 
            merged.back().second = max(merged.back().second, interval.second);
        }
    }
    
    // 计算总时间
    double totalTime = 0;
    for (const auto& interval : merged) {
        totalTime += (interval.second - interval.first);
    }
    
    return totalTime;
}

double CommunicationTimeAnalyzer::calculateTotalCommTime(const vector<Simulator::TimelineEvent>& events) {
    vector<pair<double, double>> intervals;
    
    // 一次遍历提取所有通信事件（TP、PP、DP）
    for (const auto& event : events) {
        if (event.groupType == GroupType::TP || event.groupType == GroupType::PP || event.groupType == GroupType::DP) {
            intervals.push_back({event.startTime, event.endTime});
        }
    }
    
    // 使用辅助函数合并区间并计算总时间
    return mergeIntervals(intervals);
}

string Simulator::groupTypeToString(GroupType type) {
    switch(type) {
        case GroupType::DP: return "DP";
        case GroupType::PP: return "PP";
        case GroupType::TP: return "TP";
        default: return "UNKNOWN";
    }
}

string Simulator::eventTypeToString(EventType type) {
    switch(type) {
        case EventType::COMPUTE_FWD: return "COMPUTE_FWD";
        case EventType::COMPUTE_BWD: return "COMPUTE_BWD";
        case EventType::TP_COMM_FWD: return "TP_COMM_FWD";
        case EventType::TP_COMM_BWD: return "TP_COMM_BWD";
        case EventType::PP_COMM_FWD: return "PP_COMM_FWD";
        case EventType::PP_COMM_BWD: return "PP_COMM_BWD";
        case EventType::DP_COMM_EVENT: return "DP_COMM_EVENT";
        default: return "UNKNOWN";
    }
}

string Simulator::endpointToString(EndpointType endpoint) {
    switch(endpoint) {
        case EndpointType::SENT: return "SENT";
        case EndpointType::RECV: return "RECV";
        case EndpointType::NONE_ENDPOINT: return "NONE";
        default: return "UNKNOWN_ENDPOINT";
    }
}

string Simulator::stateToString(RankState state) {
    switch(state) {
        case RankState::PP_WAIT: return "PP_WAIT";
        case RankState::COMPUTE: return "COMPUTE";
        case RankState::TP_COMM: return "TP_COMM";
        case RankState::DP_WAIT: return "DP_WAIT";
        case RankState::DP_COMM: return "DP_COMM";
        case RankState::DONE: return "DONE";
        default: return "UNKNOWN";
    }
}

void Simulator::print(){
    cout << "--------------------------" << endl;
    cout << "Simulator:" << endl;
    cout << "Tasks: " << tasks.size() << endl;
    for(auto task : tasks) {
        task->printStates();
    }
    cout << "--------------------------" << endl;
}

void Simulator::printStates(){
    cout << "---------------------------" << endl;
    cout << "Simulator:" << endl;
    for(auto task : tasks) {
        task->printStates();
    }
    cout << "---------------------------" << endl;
}

void Simulator::recordTimelineEvent(
    int rank, 
    GroupType groupType, 
    EndpointType endpoint,
    EventType eventType,
    int microbatch,
    double startTime, 
    double endTime,
    const std::string& info
) {
    TimelineEvent evt{
        rank,
        groupType,
        endpoint,
        eventType,
        microbatch,
        startTime,
        endTime,
        info
    };

    timelineEvents.push_back(evt);

    cout << "[Timeline-Record] " 
         << "Rank=" << rank << " | "
         << "GroupType=" << groupTypeToString(groupType) << " | "
         << "Event=" << eventTypeToString(eventType) << " | "
         << "Endpoint=" << endpointToString(endpoint) << " | "
         << "MB=" << microbatch << " | "
         << "Time=[" << startTime << "→" 
                    << (endTime > 0 ? to_string(endTime) : "pending") << "] | "
         << "Info=" << (info.empty() ? "-" : info)
         << endl;
}

void Simulator::initialize() {

    cout << "[INIT] Initializing simulator" << endl;
    // Clear existing data
    tasks.clear();
    timelineEvents.clear();
    commEvents.clear();
    
    // 初始化DP同步计数器
    completedLastBwdCount = 0;
    totalRanks = workload->ranks.size();
    
    // 清空microbatch状态，防止全局变量污染
    cout << "[INIT] Before clearing, microbatchStates size: " << MicrobatchManager::microbatchStates.size() << endl;
    MicrobatchManager::microbatchStates.clear();
    cout << "[INIT] After clearing, microbatchStates size: " << MicrobatchManager::microbatchStates.size() << endl;
    cout << "[INIT] Cleared microbatchStates to prevent global variable pollution" << endl;
    
    // 清空rank-specific microbatch时间
    cout << "[INIT] Before clearing, rankMicrobatchTimes size: " << MicrobatchManager::rankMicrobatchTimes.size() << endl;
    MicrobatchManager::rankMicrobatchTimes.clear();
    cout << "[INIT] After clearing, rankMicrobatchTimes size: " << MicrobatchManager::rankMicrobatchTimes.size() << endl;
    cout << "[INIT] Cleared rankMicrobatchTimes to prevent global variable pollution" << endl;

    // Create group tasks
    for (auto group : workload->groups) {
        GroupTask* task = new GroupTask(group);
        tasks.push_back(task);
    }

    // Create rank tasks
    for (auto rank : workload->ranks) {
        RankTask* task = new RankTask(rank);
        task->microbatch = 1;  // Start with first forward microbatch
        tasks.push_back(task);
    }

    // Associate tasks and setup communication paths
    for (auto rank : workload->ranks) {
        RankTask* rankTask = rank->rankTask;
        GroupTask* tpFwdGroupTask = rank->tpFwdGroup->groupTask;
        GroupTask* tpBwdGroupTask = rank->tpBwdGroup->groupTask;
        GroupTask* dpGroupTask = rank->dpGroup->groupTask;

        // 设置rank与TP前向组的双向连接
        rankTask->tpFwdGroupTask = tpFwdGroupTask;
        tpFwdGroupTask->senders.push_back(rankTask);    // rank可发送TP前向通信
        tpFwdGroupTask->receivers.push_back(rankTask);  // rank可接收TP前向通信

        // 设置rank与TP后向组的双向连接
        rankTask->tpBwdGroupTask = tpBwdGroupTask;
        tpBwdGroupTask->senders.push_back(rankTask);    // rank可发送TP后向通信
        tpBwdGroupTask->receivers.push_back(rankTask);  // rank可接收TP后向通信

        // 设置rank与DP组的双向连接
        rankTask->dpGroupTask = dpGroupTask;
        dpGroupTask->senders.push_back(rankTask);
        dpGroupTask->receivers.push_back(rankTask);

        // PP forward connections (if exists)
        if (rank->ppFwdGroup != nullptr) {
            // The receiver is the next stage's rank
            Rank* nextStageRank = rank->ppFwdGroup->ranks[1];
            GroupTask* ppFwdGroupTask = rank->ppFwdGroup->groupTask;
            rankTask->ppFwdGroupTask = ppFwdGroupTask;
            ppFwdGroupTask->senders.push_back(rankTask);
            ppFwdGroupTask->receivers.push_back(nextStageRank->rankTask);
        }

        // PP backward connections (if exists)
        if (rank->ppBwdGroup != nullptr) {
            // The receiver is the previous stage's rank
            Rank* prevStageRank = rank->ppBwdGroup->ranks[1];
            GroupTask* ppBwdGroupTask = rank->ppBwdGroup->groupTask;
            rankTask->ppBwdGroupTask = ppBwdGroupTask;
            ppBwdGroupTask->senders.push_back(rankTask);
            ppBwdGroupTask->receivers.push_back(prevStageRank->rankTask);
        }
    }

    // Initialize rank states and events
    cout << "[INIT-DEBUG] Starting rank initialization..." << endl;
    for (auto rank : workload->ranks) {
        RankTask* task = rank->rankTask;
        // 初始化图驱动 expectedToken：找到本 stage 的起始 token（优先 FWD=1）
        {
            int stage = rank->pp;
            // 起点：优先尝试 token=1（FWD），若存在 next，则设预期为1，否则设为0等待 RECV
            task->expectedToken = 1;
            // stage0 的 FWD(1) 可直接就绪
            if (rank->pp == 0) {
                task->onTokenReady(1);
            }
        }
        if (rank->pp == 0) { // 第一个pipeline stage的rank
            cout << "[INIT-DEBUG] Initializing first pipeline stage rank " << rank->id << endl;
            cout << "[INIT-DEBUG] Initial rankGlobalTime=" << task->rankGlobalTime << endl;
            task->state = RankState::COMPUTE;
            task->remainingTime = workload->fwdCompTime;
            task->microbatch = 1; // 确保microbatch正确设置
            
            // 图驱动机制：不直接记录初始事件，完全依赖onTokenReady(1)触发
            // 初始化时只设置状态，让图驱动机制自然推进
            cout << "[INIT-DEBUG] Graph-driven initialization: relying on onTokenReady(1) for first FWD" << endl;
            
            // 修复：不要预调度所有microbatch的前向计算，让1F1B流水线自然推进
            // 1F1B流水线应该是一个microbatch的FWD完成后立即执行其BWD，然后再处理下一个microbatch
            // 预调度所有FWD会破坏1F1B的正确顺序，导致所有FWD先执行完再执行所有BWD
            
            // 对于需要通信的场景（TP>1或PP>1），也不应该预调度所有microbatch
            // 而是让1F1B流水线通过事件驱动的方式自然推进
            cout << "[INIT] 1F1B pipeline: microbatches will be scheduled in 1F1B order (FWD->BWD->FWD->BWD) for rank " << rank->id << endl;
            
            // 图驱动机制：不预调度任何事件，完全依赖onTokenReady(1)触发
            // 图驱动机制会在COMPUTE_FWD完成后自动调度TP通信事件
            if (workload->TP > 1) {
                cout << "[INIT] TP>1: TP events will be triggered by graph-driven mechanism after COMPUTE_FWD completion for rank " << rank->id << endl;
            } else {
                cout << "[INIT] TP=1, no tensor parallelism communication needed for rank " << rank->id << endl;
            }
            
            // 2. PP_FWD事件现在由图驱动机制调度，不需要预调度
            // 图驱动机制会在COMPUTE_FWD完成后自动调度PP_COMM_FWD
            if (workload->PP > 1) {
                cout << "[INIT] PP>1: PP events will be triggered by graph-driven mechanism after COMPUTE_FWD completion for rank " << rank->id << endl;
            } else {
                cout << "[INIT] PP=1, no pipeline communication needed for rank " << rank->id << endl;
            }

            // DP场景：不需要预调度BWD，图驱动机制会自然处理1F1B流水线
            if (workload->TP <= 1 && workload->PP <= 1 && workload->DP > 1) {
                cout << "[INIT] DP-only path: will use graph-driven mechanism for 1F1B pipeline on rank " << rank->id << endl;
            }
            
        } else {
            // 其他pipeline stage的rank保持等待状态，事件将由前一阶段触发
            task->state = PP_WAIT;
            task->microbatch = 0; // 初始化为0，等待事件触发
            cout << "[INIT] Rank " << rank->id << " (pp=" << rank->pp << ") is not the first pipeline stage, events will be triggered by previous stage" << endl;
        }
    }


    cout << "Simulator initialized with " << tasks.size() << " tasks" << endl;
    cout << "  - " << workload->ranks.size() << " rank tasks" << endl;
    cout << "  - " << workload->groups.size() << " group tasks" << endl;
}

void Simulator::updateStates(){
    // collective active flows
    set<Flow*> activeFlows;
    for(auto task : tasks){

        if(dynamic_cast<GroupTask*>(task) != nullptr) {
            GroupTask* groupTask = dynamic_cast<GroupTask*>(task);
            if(groupTask->activeCollective != nullptr) {
                for(auto flow : groupTask->activeCollective->flows) {
                    activeFlows.insert(flow);
                }
            }
        }
    }
    // update flow throughput
    for(auto flow : activeFlows){
        flow->throughput = 0;
    }

    // collective active links
    set<Link*> activeLinks;
    for(auto flow : activeFlows){
        for(auto link : flow->pathLinks){
            activeLinks.insert(link);
        }
    }

    // update link throughput
    for(auto link : activeLinks){
        link->throughput = 0;
        link->flows.clear();
    }

    // update link flows
    for(auto flow : activeFlows){
        for(auto link : flow->pathLinks){
            link->flows.insert(flow);
        }
    }

    // update throughput
    while(!activeFlows.empty() && !activeLinks.empty()) { // water filling
        // iterate links to get minimum throughput
        double minAug = numeric_limits<double>::infinity();
        for(auto link : activeLinks) {
            double aug = (link->capacity - link->throughput)/link->flows.size();
            if(aug < minAug) {
                minAug = aug;
            }
        }
        // update flows 
        for(auto flow : activeFlows) {
            flow->throughput += minAug;
        }
        // update links
        for(auto link : activeLinks) {
            link->throughput += minAug * link->flows.size();
        } 
        // freeze link
        set<Link*> frozenLinks;
        for(auto link : activeLinks) {
            if(link->throughput >= link->capacity - 1e-6) {
                frozenLinks.insert(link);
            }
        }
        // freeze flows
        set<Flow*> frozenFlows;
        for(auto link : frozenLinks) {
            for(auto flow : link->flows) {
                frozenFlows.insert(flow);
            }
        }
        // freeze flows in the same collective 
        for(auto flow : frozenFlows) {
            for(auto other : flow->collective->flows) {
                if(other != flow) {
                    frozenFlows.insert(other);
                }
            }
        }
        // remove frozen flows
        for(auto flow : frozenFlows) {
            activeFlows.erase(flow);
        }
        // remove frozen links
        for(auto link : frozenLinks) {
            activeLinks.erase(link);
        }
    }

    // if active flows is not empty, it is internal, it completes immediately
    for(auto flow : activeFlows) {
        if (flow->collective->group->type == GroupType::DP && 
            flow->throughput < 1e-9) {
            flow->throughput = 1.0;  // 保证至少有非零带宽
        }
        flow->throughput = numeric_limits<double>::infinity();
        // flow->remainingSize = 0;
    }
}

string Simulator::getTimelineJSON() {
    nlohmann::json j;
    for (const auto& event : timelineEvents) {
        j.push_back(nlohmann::json{
            {"rank", event.rank},
            {"groupType", groupTypeToString(event.groupType)},
            {"eventType", eventTypeToString(event.eventType)},
            {"microbatch", event.microbatch},
            {"start", event.startTime},
            {"end", event.endTime}
        });
    }
    return j.dump();
}

bool Simulator::isSimulationDone() {
    // 检查所有 RankTask
    for (auto task : tasks) {
        if (auto* rankTask = dynamic_cast<RankTask*>(task)) {  // 正确声明rankTask变量
            cout << "Rank " << rankTask->rank->id 
                << " check isSimulationDone state: " << stateToString(rankTask->state)
                << " completedFwdMicrobatches: " << rankTask->completedFwdMicrobatches.size()
                << "/" << workload->microbatches
                << " completedBwdMicrobatches: " << rankTask->completedBwdMicrobatches.size()
                << "/" << workload->microbatches
                << endl;

            if (!rankTask->isAllMicrobatchesDone()) {
                cout << "isSimulationDone 1" << endl;
                return false;
            }
            if (rankTask->isFirstRankInPipeline() && rankTask->state != DONE) {
                cout << "isSimulationDone 2" << endl;
                return false;
            }
            cout << "isSimulationDone 3" << endl;
        }
    }

    // 检查所有 GroupTask 是否空闲
    for (auto task : tasks) {
        if (auto groupTask = dynamic_cast<GroupTask*>(task)) {
            if (groupTask->activeCollective != nullptr ||
                !groupTask->waitingCollectives.empty() ||
                !groupTask->accumulatingCollectives.empty()) {
                return false;
            }
        }
    }

    return true;
}

SimResult Simulator::py_run(bool enableTimeline){
    cout << "===========================" << endl;
    int round = 0;
    int targetRound = -1;   // 用于周期性打印states 

    cout << "\n[PYINIT] Checking initial events for all tasks:" << endl;
    for (auto task : tasks) {
        if (auto rankTask = dynamic_cast<RankTask*>(task)) {
            cout << "  RankTask " << rankTask->rank->id 
                << ", events size=" << rankTask->events.size() << endl;
            for (auto& event : rankTask->events) {
                cout << " Event: EP=" << simulator->endpointToString(static_cast<EndpointType>(get<0>(event))) 
                    << ", Type=" << simulator->eventTypeToString(static_cast<EventType>(get<1>(event)))
                    << ", MB=" << get<2>(event) << endl;
            }
        }
        else if (auto groupTask = dynamic_cast<GroupTask*>(task)) {
            cout << "  GroupTask " << groupTask->group->id
                << ", events size=" << groupTask->events.size() << endl;
        }
    }

    while(true){
        cout << "\n=== Round " << round << " ===" << endl;
        if(round==targetRound) printStates(); // !!!!!!!!!!!!!!
        // 1. 处理通信事件（不处理计算事件）
        int eventsProcessed;
        do {
            eventsProcessed = 0;
            for (auto task : tasks) {
                eventsProcessed += task->handleEvents();
            }
        } while (eventsProcessed > 0);  // 直到没有新的通信事件产生

        if(round==targetRound) printStates(); // !!!!!!!!!!!!!!
        // 2. 更新网络状态（计算各 Flow 的吞吐量）
        updateStates();

        // 3. 计算最短稳定时间（所有任务中最小的 stableTime）
        double minStableTime = numeric_limits<double>::infinity();
        for (auto task : tasks) {
            minStableTime = min(minStableTime, task->stableTime());
        }
        
        // 4. 如果所有任务都已完成，退出循环
        if (isSimulationDone() || minStableTime == numeric_limits<double>::infinity()) {
            break;
        }

        cout << "[TIME-PRE] MinStableTime=" << minStableTime << endl;

        // 5. 推进时间并更新所有任务状态（每个任务独立更新时间）
        for (auto task : tasks) {
            task->progress(minStableTime);
        }

        cout << "[TIME-POST] All tasks progressed by " << minStableTime << endl;

        if(round==targetRound )printStates(); // !!!!!!!!!!!!!!!
        round++;
    }

    // 6. 确保所有未完成的事件标记为完成（防止遗漏）
    // 计算最终的全局时间（使用所有rank的最大时间）
    double finalGlobalTime = 0;
    for (auto task : tasks) {
        if (dynamic_cast<RankTask*>(task) != nullptr) {
            RankTask* rankTask = dynamic_cast<RankTask*>(task);
            finalGlobalTime = std::max(finalGlobalTime, rankTask->rankGlobalTime);
        } else if (dynamic_cast<GroupTask*>(task) != nullptr) {
            GroupTask* groupTask = dynamic_cast<GroupTask*>(task);
            finalGlobalTime = std::max(finalGlobalTime, groupTask->groupGlobalTime);
        }
    }
    
    for (auto& evt : commEvents) {
        if (evt.endTime < 0) {
            evt.endTime = finalGlobalTime;  // 标记为最终全局时间完成
            cout << "[STAT-Final] Type=" << groupTypeToString(evt.type)
                 << " MB=" << evt.microbatch
                 << " Duration=" << (evt.endTime - evt.startTime)
                 << " From=" << evt.startTime
                 << " To=" << evt.endTime << endl;
        }
    }
    cout << "===========================" << endl;

    cout << "Recorded " << timelineEvents.size() << " timeline events\n";
    cout << "# 格式: [rank, event_type, microbatch, start_time, end_time]\n";
    
    SimResult result;
    result.globalTime = finalGlobalTime;
    
    // 使用通信时间分析器进行详细统计
    CommunicationTimeAnalyzer analyzer;
    
    // 按batch统计（考虑时间重叠），同时获取每个microbatch的统计数据
    CommunicationTimeAnalyzer::BatchStats batchStats = analyzer.analyzePerBatch(timelineEvents);
    result.batchTpFwCommTime = batchStats.tpForwardTime;
    result.batchTpBwCommTime = batchStats.tpBackwardTime;
    result.batchPpFwCommTime = batchStats.ppForwardTime;
    result.batchPpBwCommTime = batchStats.ppBackwardTime;
    result.batchDpCommTime = batchStats.dpTime;
    result.batchTpCommTime = batchStats.tpCommTime;
    result.batchPpCommTime = batchStats.ppCommTime;
    
    // 计算所有3D并行通信的总时间（考虑时间重叠）
    result.totalCommTime = analyzer.calculateTotalCommTime(timelineEvents);
    
    // 从batchStats中获取第1个microbatch的通信时间作为代表
    result.microbatchTpFwCommTime = 0;
    result.microbatchTpBwCommTime = 0;
    result.microbatchPpFwCommTime = 0;
    result.microbatchPpBwCommTime = 0;
    
    if (batchStats.microbatchStats.find(1) != batchStats.microbatchStats.end()) {
        const auto& firstMbStats = batchStats.microbatchStats[1];
        result.microbatchTpFwCommTime = firstMbStats.tpForwardTime;
        result.microbatchTpBwCommTime = firstMbStats.tpBackwardTime;
        result.microbatchPpFwCommTime = firstMbStats.ppForwardTime;
        result.microbatchPpBwCommTime = firstMbStats.ppBackwardTime;
    }
    
    // 融合打印和收集timeline事件：在遍历过程中同时打印和收集数据
    if (enableTimeline) {
        for (size_t i = 0; i < timelineEvents.size(); ++i) {
            const auto& e = timelineEvents[i];
            
            // 打印事件信息
            cout << "    [" << e.rank << ", \"" 
                << eventTypeToString(e.eventType) 
                << "\", "        
                << e.microbatch << ", " 
                << e.startTime << ", " << e.endTime << "]";
                    
            // 如果不是最后一个元素，添加逗号
            if (i < timelineEvents.size() - 1) {
                cout << ",";
            }
            cout << "\n";
            
            // 同时收集事件数据到结果中
            SimResult::TimelineEventData eventData;
            eventData.rank = e.rank;
            eventData.eventType = eventTypeToString(e.eventType);
            eventData.microbatch = e.microbatch;
            eventData.startTime = e.startTime;
            eventData.endTime = e.endTime;
            result.timelineEvents.push_back(eventData);
        }
    } else {
        cout << "[TIMELINE] Timeline data collection disabled, skipping timeline event collection" << endl;
    }

    cout << "Simulation finished" << endl;
    cout << "Final Global Time: " << finalGlobalTime << endl;
    
    // 输出新的通信时间统计结果
    cout << "=== Communication Time Analysis ===" << endl;
    cout << "Global Time: " << result.globalTime << endl;
    cout << "Total Communication Time (with overlap deduplication): " << result.totalCommTime << endl;
    cout << "Batch-level (with overlap deduplication):" << endl;
    cout << "  Batch TP Forward Time: " << result.batchTpFwCommTime << endl;
    cout << "  Batch TP Backward Time: " << result.batchTpBwCommTime << endl;
    cout << "  Batch PP Forward Time: " << result.batchPpFwCommTime << endl;
    cout << "  Batch PP Backward Time: " << result.batchPpBwCommTime << endl;
    cout << "  Batch DP Time: " << result.batchDpCommTime << endl;
    cout << "  Batch TP Total Time: " << result.batchTpCommTime << endl;
    cout << "  Batch PP Total Time: " << result.batchPpCommTime << endl;
    cout << "Microbatch-level (mb=1 representative):" << endl;
    cout << "  Microbatch TP Forward Time: " << result.microbatchTpFwCommTime << endl;
    cout << "  Microbatch TP Backward Time: " << result.microbatchTpBwCommTime << endl;
    cout << "  Microbatch PP Forward Time: " << result.microbatchPpFwCommTime << endl;
    cout << "  Microbatch PP Backward Time: " << result.microbatchPpBwCommTime << endl;
    cout << "---------------------------" << endl;

    return result;
}
