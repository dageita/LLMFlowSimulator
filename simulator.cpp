#include "common.h"
#include "simulator.h"


#include <limits>
#include <string>
#include <vector>
#include <iostream>
#include <cassert>
#include <typeinfo>

#include <nlohmann/json.hpp>

using namespace std;

extern Topology* topology;
extern Workload* workload;
extern Simulator* simulator;

// 全局microbatch状态管理器的静态成员定义
map<int, MicrobatchManager::MicrobatchState> MicrobatchManager::microbatchStates;


Flow::Flow(Connection* connection){
    src = connection->src->host;
    dst = connection->dst->host;
    path = connection->path;
    pathLinks = connection->pathLinks;
}

void Flow::progress(double time){
    if(remainingSize<1e-6) {
        remainingSize = 0;
    }
    else{
        remainingSize -= throughput * time;
    }
}

double Flow::stableTime(){
    return remainingSize/throughput;
}

Collective::Collective(Group* group, int microbatch, int accumulatedSize) : 
        group(group), microbatch(microbatch), accumulatedSize(accumulatedSize) { 

    if (group->type == GroupType::TP && group->ranks.size() <= 1) {
    // 不生成任何 TP 流
        return;
    }

    accumulatedInvocations = 1;
    this->flows.clear();
    if (group->type == GroupType::TP || group->type == GroupType::DP) {
        for (auto connection : group->connections) {
            Flow* flow = new Flow(connection);
            if (group->type == GroupType::TP) {
                flow->remainingSize = microbatch > 0 ? workload->fwdTPSize : workload->bwdTPSize;
            } else {
                flow->remainingSize = workload->dpSize;
            }
            // TP 需要 all-to-all 放大，DP 只有一次，无放大
            double factor = (group->type == GroupType::TP) 
                                ? 2.0 * (group->ranks.size()-1) / group->ranks.size() 
                                : 1.0;
            flow->remainingSize *= factor;
            flow->collective = this;
            flows.push_back(flow);
        }
    }
    else { // PP, generate one connection
        Flow* flow = new Flow(group->connections[0]);
        flow->remainingSize = microbatch > 0 ? workload->fwdPPSize : workload->bwdPPSize;
        this->flows.push_back(flow);
        flow->collective = this;
    }
}

void Collective::printStates(){
    cout << "Collective:" ;
    cout << " Group: " << group->id <<", Group Size: " << group->ranks.size() ;
    cout << " Microbatch: " << microbatch ;
    cout << " Accumulated invocations: " << accumulatedInvocations ;
    cout << endl;
    for(auto flow : flows) {
        cout << "Flow: " ;
        cout << flow->src->id << "->" << flow->dst->id ;
        cout << ", Remaining size: " << flow->remainingSize ;
        cout << ", Throughput: " << flow->throughput ;
        cout << endl;
    }
}

double Collective::stableTime(){
    double time = numeric_limits<double>::infinity();
    for(auto flow : flows){
        double t = flow->stableTime();
        if(t < time) time = t;
    }
    return time;
}

void Collective::progress(double time){
    for(auto flow : flows){
        flow->progress(time);
    }
}

void RankTask::addEvent(int ep, int eventType, int mb, RankState state, double remainingTime) {
    events.emplace_back(ep, eventType, mb, state, remainingTime);
    cout << "[RankTask Event Added] Rank=" << rank->id 
            << " | EP=" << simulator->endpointToString(static_cast<EndpointType>(ep))
            << " | EventType=" << simulator->eventTypeToString(static_cast<EventType>(eventType))
            << " | MB=" << mb 
            << " | State=" << simulator->stateToString(state)
            << " | RemainingTime=" << remainingTime
            << " | Total Events=" << events.size() << endl;
}

RankTask::RankTask(Rank* rank) : rank(rank), state(PP_WAIT), remainingTime(0), rankGlobalTime(0), microbatch(1), lastProcessedMb(1) {
    cout << "[RANKTASK-CONSTRUCTOR] Rank " << rank->id << " created with rankGlobalTime=" << rankGlobalTime << endl;
    rank->rankTask = this;
    this->rank = rank;
    this->dpGroupTask = nullptr; // corrected assignment
    this->tpFwdGroupTask = nullptr;  // corrected assignment
    this->tpBwdGroupTask = nullptr;  // corrected assignment
    this->ppFwdGroupTask = nullptr; // corrected assignment
    this->ppBwdGroupTask = nullptr; // corrected assignment
}

bool RankTask::isFirstRankInPipeline() const {
    // 如果 ppBwdGroupTask 不存在或没有发送方，则是初始 Rank
    if (!ppBwdGroupTask || ppBwdGroupTask->senders.empty()) {
        cout << "Rank " << rank->id << " is the fist in pipeline (no PP backward group task or no senders)." << endl;
        return true;
    }
    return false;
}

bool RankTask::isLastRankInPipeline() const {
    // 检查是否存在有效的 ppFwdGroupTask 和接收方
    if (!ppFwdGroupTask || ppFwdGroupTask->receivers.empty()) {
        cout << "Rank " << rank->id << " is the last in pipeline (no PP forward group task or no receivers)." << endl;
        return true;  // 无下游接收方，是最后一个Rank
    }
    return false;
}

void RankTask::printStates(){
    cout << "---------------------------" << endl;
    cout << "RankTask:" ;
    cout << " Rank: " << rank->id ;
    cout << ", State: " ;
    switch(state) {
        case PP_WAIT:
            cout << "PP_WAIT";
            break;
        case COMPUTE:
            cout << "COMPUTE";
            break;
        case TP_COMM:
            cout << "TP_COMM";
            break;
        case DP_WAIT:
            cout << "DP_WAIT";
            break;
        case DP_COMM:
            cout << "DP_COMM";
            break;
        case DONE:
            cout << "DONE";
            break;
    }
    cout << ", Microbatch: " << microbatch ;
    cout << ", Remaining time: " << remainingTime ;
    cout << ", RankGlobalTime: " << rankGlobalTime ;
    cout << ", LastProcessed: mb=" << lastProcessedMb;
    cout << ", MicrobatchStates: ";
    if (MicrobatchManager::microbatchStates.empty()) {
        cout << "empty";
    } else {
        for (const auto& pair : MicrobatchManager::microbatchStates) {
            cout << "mb" << pair.first << "(time=" << pair.second.globalTime << ") ";
        }
    }
    cout << ", Events: " << events.size() << ": ";
    for(auto event : events) {
        string event_str = "<";
        switch(get<0>(event)) {
            case EndpointType::SENT:
                event_str += "SENT, ";
                break;
            case EndpointType::RECV:
                event_str += "RECV, ";
                break;
            case EndpointType::NONE_ENDPOINT:
                event_str += "NONE, ";
                break;
        }
        switch(get<1>(event)) {
            case EventType::COMPUTE_FWD:
                event_str += "COMP_FWD, ";
                break;
            case EventType::COMPUTE_BWD:
                event_str += "COMP_BWD, ";
                break;
            case EventType::PP_COMM_FWD:
                event_str += "PP_FWD, ";
                break;
            case EventType::PP_COMM_BWD:
                event_str += "PP_BWD, ";
                break;
            case EventType::TP_COMM_FWD:
                event_str += "TP_FWD, ";
                break;
            case EventType::TP_COMM_BWD:
                event_str += "TP_BWD, ";
                break;
            case EventType::DP_COMM_EVENT:
                event_str += "DP, ";
                break;
        }
        event_str += to_string(get<2>(event)) + ", " + simulator->stateToString(get<3>(event)) + ", " + to_string(get<4>(event)) + ">";
        cout << event_str << " ";
    }
    cout << endl;
}

double RankTask::stableTime(){
    // 检查所有事件中是否有需要计算的事件
    for (const auto& event : events) {
        int ep = get<0>(event);
        int eventType = get<1>(event);
        RankState eventState = get<3>(event);
        double eventRemainingTime = get<4>(event);
        
        if (eventState == COMPUTE && eventRemainingTime > 1e-6) {
            return eventRemainingTime;
        }
        
        // 检查RECV事件，其stableTime由对应的GroupTask决定
        if (ep == EndpointType::RECV && eventState == PP_WAIT) {
            if (eventType == PP_COMM_FWD && ppFwdGroupTask) {
                // 查找对应的GroupTask的stableTime
                for (auto task : simulator->tasks) {
                    if (dynamic_cast<GroupTask*>(task) != nullptr) {
                        GroupTask* groupTask = dynamic_cast<GroupTask*>(task);
                        if (groupTask->group->id == ppFwdGroupTask->group->id) {
                            double groupStableTime = groupTask->stableTime();
                            if (groupStableTime < numeric_limits<double>::infinity()) {
                                return groupStableTime;
                            }
                            break;
                        }
                    }
                }
            }
            else if (eventType == PP_COMM_BWD && ppBwdGroupTask) {
                // 查找对应的GroupTask的stableTime
                for (auto task : simulator->tasks) {
                    if (dynamic_cast<GroupTask*>(task) != nullptr) {
                        GroupTask* groupTask = dynamic_cast<GroupTask*>(task);
                        if (groupTask->group->id == ppBwdGroupTask->group->id) {
                            double groupStableTime = groupTask->stableTime();
                            if (groupStableTime < numeric_limits<double>::infinity()) {
                                return groupStableTime;
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
    
    // 如果没有计算事件，检查当前状态
    switch(state) {
        case COMPUTE:
            return remainingTime;
        default:
            return numeric_limits<double>::infinity();
    }
}

int RankTask::getNextMicrobatch() {
    // 对于第一个rank，优先从事件队列中获取下一个COMPUTE_FWD事件
    if (isFirstRankInPipeline()) {
        for (const auto& event : events) {
            int ep = get<0>(event);
            int eventType = get<1>(event);
            int mb = get<2>(event);
            if (ep == EndpointType::NONE_ENDPOINT && eventType == COMPUTE_FWD && mb > 0) {
                cout << "[getNextMicrobatch] Found COMPUTE_FWD event for mb=" << mb << endl;
                return mb;
            }
        }
    }
    
    // 1. 优先处理未完成的反向批次
    for (int mb = 1; mb <= workload->microbatches; ++mb) {
        if (completedFwdMicrobatches.count(mb) && !completedBwdMicrobatches.count(mb)) {
            return -mb; // 返回负数批次
        }
    }

    // 2. 处理未完成的正向批次（仅限非最后一个rank）
    if (!isLastRankInPipeline()) {
        for (int mb = 1; mb <= workload->microbatches; ++mb) {
            if (!completedFwdMicrobatches.count(mb)) {
                return mb; // 返回正数批次
            }
        }
    }

    return 0; // 无任务
}

bool RankTask::isAllMicrobatchesDone() const {
    std::cout << "[DEBUG] Rank " << rank->id << ":\n"
              << "  completedFwdMicrobatches=[";
    for (int mb : completedFwdMicrobatches) std::cout << mb << ",";
    std::cout << "]\n"
              << "  completedBwdMicrobatches=[";
    for (int mb : completedBwdMicrobatches) std::cout << mb << ",";
    std::cout << "]\n";  // 修复：添加分号

    bool forward_ok = (completedFwdMicrobatches.size() >= workload->microbatches);
    bool backward_ok = false;
    if (isLastRankInPipeline()) {
        backward_ok = (completedBwdMicrobatches.size() >= workload->microbatches) && (state == DONE);
    } else {
        backward_ok = (completedBwdMicrobatches.size() >= workload->microbatches);
    }

    std::cout << "  Evaluation: forward_ok=" << forward_ok 
              << ", backward_ok=" << backward_ok << "\n";
    return forward_ok && backward_ok;
}

int RankTask::handleEvents(){   // < EP, TYPE, MB >
    int processed = 0;
    cout << "handleEvents for RankTask: " << rank->id << ", events size: " 
        << events.size() <<  ", microbatch: " << microbatch << endl;
    
    // 在handleEvents开始时更新当前microbatch的全局时间
    // if (microbatch != 0) {
    //     MicrobatchManager::updateMicrobatchGlobalTime(microbatch, rankGlobalTime);
    // }

    // for (const auto& event : events) {
    //     cout << "[Pending Event]: EP=" << simulator->endpointToString(static_cast<EndpointType>(get<0>(event)))
    //         << ", Type=" << simulator->eventTypeToString(static_cast<EventType>(get<1>(event)))
    //         << ", MB=" << get<2>(event) << endl;
    // }
    std::vector<std::tuple<int, int, int, RankState, double>> pending_events; // 临时存储新事件

    for(auto it = events.begin(); it != events.end(); ) {
        int ep = get<0>(*it); // 端点
        int eventType = get<1>(*it);  // 事件类型
        int mb = get<2>(*it); // microbatch
        RankState& eventState = get<3>(*it); // 事件状态
        double& eventRemainingTime = get<4>(*it); // 事件剩余时间

        bool shouldErase = false; // 标记是否应删除该事件

        cout << "[HANDLE EVENT] Rank " << rank->id 
            << ", EP=" << simulator->endpointToString(static_cast<EndpointType>(ep))
            << ", EventType=" << simulator->eventTypeToString(static_cast<EventType>(eventType))
            << ", MB=" << mb 
            << ", CurrentState=" << simulator->stateToString(state) << endl;

        // 为当前处理的事件更新microbatch的全局时间
        // if (mb != 0) {
        //     MicrobatchManager::updateMicrobatchGlobalTime(mb, rankGlobalTime);
        // }

        // 只处理当前 microbatch 或特殊事件
        // bool isCurrentMB = (mb == microbatch) || (mb == -microbatch) || (mb == 0); // DP事件
        // if (!isCurrentMB) {
        //     ++it;
        //     continue;
        // }

        if (ep == EndpointType::SENT) {
            shouldErase = true;
        }

        switch (eventType) {
            case PP_COMM_FWD:
                if (ep == EndpointType::RECV && mb > 0) {
                    // 接收到PP前向通信，开始计算
                    state = COMPUTE;
                    remainingTime = workload->fwdCompTime;
                    shouldErase = true;
                    
                    // 注意：PP通信事件已经在GroupTask::progress中记录，这里不需要重复记录
                    // 只记录计算开始事件
                    // 使用智能时间步进计算开始时间和结束时间
                    cout << "[HANDLE-EVENTS-DEBUG-PP] Before calculate HandleEventsTime: rankGlobalTime=" << rankGlobalTime << endl;
                    double startTime = calculateHandleEventsTime(mb); // 计算开始时间
                    double endTime = startTime + workload->fwdCompTime; // 计算结束时间
                    cout << "[HANDLE-EVENTS-DEBUG-PP] startTime=" << startTime << ", endTime=" << endTime << ", workload->fwdCompTime=" << workload->fwdCompTime << endl;
                    simulator->recordTimelineEvent(
                        rank->id, NONE_GROUP, NONE_ENDPOINT,
                        COMPUTE_FWD, mb, startTime, endTime,
                        "COMPUTE_FWD started after PP_FWD RECV"
                    );
                    
                    // 更新microbatch的全局时间
                    MicrobatchManager::updateMicrobatchGlobalTime(mb, endTime);
                    // 更新rankGlobalTime为microbatch的最新时间
                    rankGlobalTime = endTime;
                    
                    // 更新上一个处理的microbatch
                    updateLastProcessedMb(mb);
                    
                    // 对于最后一个rank，预调度反向计算事件（1F1B流水线）
                    if (isLastRankInPipeline()) {
                        pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_BWD, -mb, COMPUTE, workload->bwdCompTime);
                        cout << "[1F1B] Last rank " << rank->id << " scheduled COMPUTE_BWD for mb=" << (-mb) 
                             << " after PP_FWD RECV for mb=" << mb << endl;
                    }
                }
                else if (ep == EndpointType::SENT) {
                    // 处理PP发送事件
                    if (isFirstRankInPipeline()) {
                        // 第一个rank发送完PP通信，准备下一个microbatch的计算
                        if (mb < workload->microbatches) {
                            cout << "Rank 0 completed PP send for mb=" << mb << ", scheduling next mb=" << (mb + 1) << endl;
                            
                            // 注意：不再在这里预调度PP发送事件，因为已经在COMPUTE_FWD中处理了
                            // 这里只需要处理状态转换
                    } else {
                            // 所有正向批次完成，等待反向批次
                            shouldErase = false;
                            cout << "Rank " << rank->id << " all FWD done, waiting for BWD\n";
                            // 注意：不在这里调用getNextMicrobatch，因为completedFwdMicrobatches还没有更新
                            // 让progress函数来处理下一个microbatch的获取
                        }
                        } else {
                        // 非第一个rank发送完PP通信，通过GroupTask管理
                        shouldErase = true;
                        cout << "Rank " << rank->id << " completed PP send for mb=" << mb << endl;
                        
                        // 通过GroupTask管理PP通信，而不是直接添加RECV事件
                        if (ppFwdGroupTask) {
                            ppFwdGroupTask->addEvent(rank->id, mb);
                            cout << "[HANDLE-EVENT] Added PP_FWD event to GroupTask " << ppFwdGroupTask->group->id 
                                 << " for mb=" << mb << endl;
                        }
                    }
                }
                break;

            case PP_COMM_BWD:
                cout << "[DEBUG] Rank " << rank->id 
                    << " microbatch=" << microbatch << endl;
                if (ep == EndpointType::RECV && mb < 0) {
                    state = COMPUTE;
                    remainingTime = workload->bwdCompTime;
                    shouldErase = true;
                    
                    // 注意：PP通信事件已经在GroupTask::progress中记录，这里不需要重复记录
                    // 只记录计算开始事件
                    // 使用智能时间步进计算开始时间和结束时间
                    double startTime = calculateHandleEventsTime(mb); // 计算开始时间
                    double endTime = startTime + workload->bwdCompTime; // 计算结束时间
                    
                    simulator->recordTimelineEvent(
                        rank->id, NONE_GROUP, NONE_ENDPOINT,
                        COMPUTE_BWD, mb, startTime, endTime,
                        "COMPUTE_BWD started after PP_BWD RECV"
                    );
                    
                    // 更新microbatch的全局时间
                    MicrobatchManager::updateMicrobatchGlobalTime(mb, endTime);
                    // 更新rankGlobalTime为microbatch的最新时间
                    rankGlobalTime = endTime;
                    
                    // 更新上一个处理的microbatch
                    updateLastProcessedMb(mb);
                    
                        cout << "[STATE] Rank " << rank->id 
                        << " switched to BACKWARD, microbatch=" << mb << endl;
                }
                else if (ep == EndpointType::SENT) {
                    // 处理PP反向发送事件，通过GroupTask管理通信
                    shouldErase = true;
                    cout << "Rank " << rank->id << " completed PP backward send for mb=" << mb << endl;
                    
                    if (ppBwdGroupTask) {
                        // 通过GroupTask管理PP通信，而不是直接添加RECV事件
                        ppBwdGroupTask->addEvent(rank->id, mb);
                        cout << "[HANDLE-EVENT] Added PP_BWD event to GroupTask " << ppBwdGroupTask->group->id 
                             << " for mb=" << mb << endl;
                    }
                }
                break;

            case TP_COMM_FWD:
            case TP_COMM_BWD:
                if (ep == EndpointType::RECV) {
                    // 记录TP通信接收事件
                    // simulator->recordTimelineEvent(
                    //     rank->id, GroupType::TP, EndpointType::RECV,
                    //     static_cast<EventType>(eventType), mb, rankGlobalTime,
                    //     rankGlobalTime, // TP通信接收是瞬间完成的
                    //     "TP_COMM received"
                    // );
                    
                    // 更新microbatch的全局时间
                    MicrobatchManager::updateMicrobatchGlobalTime(mb, rankGlobalTime);
                    
                    // TP通信接收完成，根据事件类型和PP配置决定后续调度
                    if (eventType == TP_COMM_FWD) {
                        // TP前向通信完成，需要决定后续流程
                        if (workload->PP > 1) {
                            // 存在PP组，触发PP_WAIT
                            pending_events.emplace_back(EndpointType::SENT, PP_COMM_FWD, mb, PP_WAIT, 0);
                            cout << "[TP_FWD->PP] Rank " << rank->id << " scheduled PP_COMM_FWD for mb=" << mb 
                                 << " after TP_FWD RECV (PP>1)" << endl;
                        } else {
                            // 无PP组，直接触发反向计算
                            pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_BWD, -mb, COMPUTE, workload->bwdCompTime);
                            cout << "[TP_FWD->COMP_BWD] Rank " << rank->id << " scheduled COMPUTE_BWD for mb=" << (-mb) 
                                 << " after TP_FWD RECV (PP=1)" << endl;
                        }
                    } else if (eventType == TP_COMM_BWD) {
                        // TP后向通信完成，需要决定后续流程
                        if (workload->PP > 1) {
                            // 存在PP组，触发PP_WAIT
                            pending_events.emplace_back(EndpointType::SENT, PP_COMM_BWD, mb, PP_WAIT, 0);
                            cout << "[TP_BWD->PP] Rank " << rank->id << " scheduled PP_COMM_BWD for mb=" << mb 
                                 << " after TP_BWD RECV (PP>1)" << endl;
                        } else {
                            // 无PP组，不调度新事件，因为反向计算事件应该已经存在
                            // 避免死循环：TP_BWD -> COMP_BWD -> TP_BWD
                            cout << "[TP_BWD->COMP_BWD] Rank " << rank->id << " TP_BWD completed for mb=" << mb 
                                 << ", continuing with existing COMPUTE_BWD (PP=1)" << endl;
                        }
                    }
                    
                    // TP通信接收完成，删除事件
                    shouldErase = true;
                    cout << "Rank " << rank->id << " completed TP communication RECV for mb=" << mb 
                         << " (current state: " << simulator->stateToString(state) << ")" << endl;
                }
                else if (ep == EndpointType::SENT) {
                    // 注意：TP通信发送事件由GroupTask::progress记录，这里不重复记录
                    
                    // 通过相应的GroupTask管理TP通信
                    if (eventType == TP_COMM_FWD && tpFwdGroupTask) {
                        // 向TP前向GroupTask添加事件，让它管理通信流程
                        tpFwdGroupTask->addEvent(rank->id, mb);
                        cout << "[HANDLE-EVENT] Added TP_FWD event to GroupTask " << tpFwdGroupTask->group->id 
                             << " for mb=" << mb << endl;
                    } else if (eventType == TP_COMM_BWD && tpBwdGroupTask) {
                        // 向TP后向GroupTask添加事件，让它管理通信流程
                        tpBwdGroupTask->addEvent(rank->id, mb);
                        cout << "[HANDLE-EVENT] Added TP_BWD event to GroupTask " << tpBwdGroupTask->group->id 
                             << " for mb=" << mb << endl;
                    }
                    
                    // TP通信发送完成，可以删除事件
                    shouldErase = true;
                    cout << "Rank " << rank->id << " completed TP communication for mb=" << mb << endl;
                }
                break;

            case COMPUTE_FWD:
                // 计算事件由progress处理，这里记录完整的计算时间
                if (ep == EndpointType::NONE_ENDPOINT) {
                    cout << "Rank " << rank->id << " scheduling COMPUTE_FWD for mb=" << mb 
                         << " (previous microbatch=" << microbatch << ")" << endl;
                    
                    // 使用智能时间步进计算开始时间和结束时间
                    cout << "[HANDLE-EVENTS-DEBUG-COMPUTE] Before calculate HandleEventsTime: rankGlobalTime=" << rankGlobalTime << endl;
                    double startTime = calculateHandleEventsTime(mb); // 计算开始时间
                    double endTime = startTime + workload->fwdCompTime; // 计算结束时间
                    cout << "[HANDLE-EVENTS-DEBUG-COMPUTE] startTime=" << startTime << ", endTime=" << endTime << ", workload->fwdCompTime=" << workload->fwdCompTime << endl;
                    
                    // 更新事件状态为COMPUTE
                    eventState = COMPUTE;
                    eventRemainingTime = workload->fwdCompTime;
                    shouldErase = true; // 删除事件，避免死循环
                    
                    // 记录完整的计算事件（包括结束时间）
                    simulator->recordTimelineEvent(
                        rank->id, NONE_GROUP, NONE_ENDPOINT,
                        COMPUTE_FWD, mb, startTime, endTime,
                        "COMPUTE_FWD scheduled"
                    );
                    
                    // 更新microbatch的全局时间
                    MicrobatchManager::updateMicrobatchGlobalTime(mb, endTime);
                    // 更新rankGlobalTime为microbatch的最新时间
                    rankGlobalTime = endTime;
                    
                    // 更新上一个处理的microbatch
                    updateLastProcessedMb(mb);
                    
                    // 为第一个rank，预调度计算完成后的通信事件（TP优先级高于PP）
                    if (isFirstRankInPipeline()) {
                        // 1. 首先预调度TP_FWD事件（如果TP>1）
                        if (workload->TP > 1) {
                            pending_events.emplace_back(EndpointType::SENT, TP_COMM_FWD, mb, TP_COMM, 0);
                            cout << "Rank " << rank->id << " scheduled TP_COMM_FWD for mb=" << mb << endl;
                        }
                        
                        // 2. 然后预调度PP_FWD事件（如果PP>1）
                        if (workload->PP > 1) {
                            pending_events.emplace_back(EndpointType::SENT, PP_COMM_FWD, mb, PP_WAIT, 0);
                            cout << "Rank " << rank->id << " scheduled PP_COMM_FWD for mb=" << mb << endl;
                        }
                    }

                    // 纯DP场景（TP<=1, PP<=1, DP>1）：当前FWD完成后，立即调度对应的BWD
                    if (workload->TP <= 1 && workload->PP <= 1 && workload->DP > 1 && mb <= workload->microbatches) {
                        pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_BWD, -mb, COMPUTE, workload->bwdCompTime);
                        cout << "[DP-ONLY-1F1B] Rank " << rank->id << " scheduled COMPUTE_BWD for mb=" << -mb
                             << " after COMPUTE_FWD for mb=" << mb << endl;
                    }
                }
                break;

            case COMPUTE_BWD:
                // 反向计算事件由progress处理，这里记录完整的计算时间
                if (ep == EndpointType::NONE_ENDPOINT) {
                    cout << "Rank " << rank->id << " scheduling COMPUTE_BWD for mb=" << mb 
                         << " (previous microbatch=" << microbatch << ")" << endl;
                    
                    // 使用智能时间步进计算开始时间和结束时间
                    double startTime = calculateHandleEventsTime(mb); // 计算开始时间
                    double endTime = startTime + workload->bwdCompTime; // 计算结束时间
                    
                    // 更新事件状态为COMPUTE
                    eventState = COMPUTE;
                    eventRemainingTime = workload->bwdCompTime;
                    shouldErase = true; // 删除事件，避免死循环
                    
                    // 记录完整的反向计算事件（包括结束时间）
                    simulator->recordTimelineEvent(
                        rank->id, NONE_GROUP, NONE_ENDPOINT,
                        COMPUTE_BWD, mb, startTime, endTime,
                        "COMPUTE_BWD scheduled"
                    );
                    
                    // 更新microbatch的全局时间
                    MicrobatchManager::updateMicrobatchGlobalTime(mb, endTime);
                    // 更新rankGlobalTime为microbatch的最新时间
                    rankGlobalTime = endTime;
                    
                    // 更新上一个处理的microbatch
                    updateLastProcessedMb(mb);
                    
                    // 对于反向传播，需要触发通信事件（TP优先级高于PP）
                    // 注意：只有从COMPUTE_FWD转换来的COMPUTE_BWD才需要触发TP_BWD
                    // 从TP_BWD转换来的COMPUTE_BWD不应该再次触发TP_BWD
                    
                    // 1. 首先预调度TP_BWD事件（如果TP>1且不是从TP_BWD转换来的）
                    if (workload->TP > 1 && mb < 0) {
                        // 反向计算完成后才触发TP_BWD（mb < 0表示反向microbatch）
                        pending_events.emplace_back(EndpointType::SENT, TP_COMM_BWD, mb, TP_COMM, 0);
                        cout << "[1F1B] Rank " << rank->id << " scheduled TP_BWD SENT for mb=" << mb 
                             << " after COMPUTE_BWD scheduling" << endl;
                    }
                    
                    // 2. 然后预调度PP_BWD事件（如果PP>1且不是第一个rank）
                    if (workload->PP > 1 && !isFirstRankInPipeline()) {
                        pending_events.emplace_back(EndpointType::SENT, PP_COMM_BWD, mb, PP_WAIT, 0);
                        cout << "[1F1B] Rank " << rank->id << " scheduled PP_BWD SENT for mb=" << mb 
                             << " after COMPUTE_BWD scheduling" << endl;
                    }
                    
                    // 3a. 纯DP场景（TP<=1, PP<=1, DP>1）：在非最后一个microbatch时，继续调度下一个FWD
                    if (workload->TP <= 1 && workload->PP <= 1 && workload->DP > 1) {
                        int currentAbsMb = std::abs(mb);
                        if (currentAbsMb < workload->microbatches) {
                            int nextMb = currentAbsMb + 1;
                            pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_FWD, nextMb, COMPUTE, workload->fwdCompTime);
                            cout << "[DP-ONLY-1F1B] Rank " << rank->id << " scheduled COMPUTE_FWD for mb=" << nextMb
                                 << " after completing COMPUTE_BWD for mb=" << mb << endl;
                        }
                    }

                    // 3b. 检查是否需要触发DP通信（在最后一个rank完成最后一个microbatch的反向计算后）
                    // 只有当DP>1且是最后一个rank且已完成最后一个microbatch时才需要DP通信
                    if (workload->DP > 1 && isLastRankInPipeline() && std::abs(mb) == workload->microbatches) {
                        // 最后一个rank完成反向计算后，直接向DP GroupTask添加事件
                        if (dpGroupTask) {
                            dpGroupTask->addEvent(rank->id, mb);
                            cout << "[DP-SYNC] Last rank " << rank->id << " completed COMPUTE_BWD for mb=" << mb 
                                 << ", added DP communication event to GroupTask " << dpGroupTask->group->id 
                                 << " for microbatch " << mb << " (gradient sync)" << endl;
                        }
                    }
                }
                break;

            case DP_COMM_EVENT:
                if (ep == EndpointType::RECV && state == DP_WAIT) {
                    state = DONE;
                    shouldErase = true;
                }
                break;

            default:
                break;
        }

        if (shouldErase) {
            // 更新上一个处理的microbatch（对于非SENT事件）
            if (ep != EndpointType::SENT) {
                updateLastProcessedMb(mb);
            }
            
            processed++;
            it = events.erase(it);
            cout << "[STATE TRANSITION] Rank " << rank->id 
                 << " transition to " << simulator->stateToString(state)
                 << " after " << simulator->eventTypeToString(static_cast<EventType>(eventType)) << endl;
        } else {
            ++it;
        }
    }
    for (auto& [ep, type, mb, state, remainingTime] : pending_events) {
        addEvent(ep, type, mb, state, remainingTime);
        cout << "[PENDING] Added event: EP=" << simulator->endpointToString(static_cast<EndpointType>(ep)) 
            << ", Type=" << simulator->eventTypeToString(static_cast<EventType>(type)) 
            << ", MB=" << mb 
            << ", State=" << simulator->stateToString(state) 
            << ", RemainingTime=" << remainingTime << endl;
    }
    
    return processed;
}

void RankTask::progress(double time){
    cout << "[PROGRESS] Rank " << rank->id << " starting progress, events size=" << events.size() 
         << ", rankGlobalTime=" << rankGlobalTime << ", time step=" << time << endl;
    
    // 遍历所有事件，为每个microbatch更新状态
    for (auto& event : events) {
        int ep = get<0>(event);
        int eventType = get<1>(event);
        int mb = get<2>(event);
        RankState& eventState = get<3>(event);
        double& eventRemainingTime = get<4>(event);
        
        cout << "[PROGRESS] Processing event: EP=" << simulator->endpointToString(static_cast<EndpointType>(ep)) 
            << ", Type=" << simulator->eventTypeToString(static_cast<EventType>(eventType)) 
            << ", MB=" << mb 
            << ", State=" << simulator->stateToString(eventState) 
            << ", RemainingTime=" << eventRemainingTime << endl;
        
        // 从timelineEvents中查找当前事件的startTime
        double eventStartTime = rankGlobalTime; // 默认使用当前rankGlobalTime
        for (const auto& timelineEvent : simulator->timelineEvents) {
            if (timelineEvent.rank == rank->id && 
                timelineEvent.microbatch == mb && 
                timelineEvent.eventType == static_cast<EventType>(eventType)) {
                eventStartTime = timelineEvent.startTime;
                break;
            }
        }
        
        // 智能时间步进：根据microbatch并行性决定时间更新策略
        double newRankGlobalTime = calculateNewRankGlobalTime(time, mb, eventStartTime);
        if (newRankGlobalTime > rankGlobalTime) {
            cout << "[PROGRESS] Rank " << rank->id << " time update: " << rankGlobalTime << " -> " << newRankGlobalTime << endl;
            rankGlobalTime = newRankGlobalTime;
        }
        
        // 更新上一个处理的microbatch（对于非SENT事件）
        if (ep != EndpointType::SENT) {
            updateLastProcessedMb(mb);
        }
        
        // 根据事件状态进行处理
        switch (eventState) {
        case COMPUTE:            
                // 处理计算状态
                eventRemainingTime -= time;
                if (eventRemainingTime <= 1e-6) {
                    // 计算完成
                    cout << "[PROGRESS] Rank " << rank->id << " completed computation for mb=" << mb 
                         << " at rankGlobalTime=" << rankGlobalTime << endl;
                    
                    if (mb < 0) {  // 反向计算完成
                        completedBwdMicrobatches.insert(mb);
                        eventState = PP_WAIT;
                        eventRemainingTime = 0;
                        
                        // 对于最后一个rank，反向计算完成后需要发送PP_BWD
                        if (isLastRankInPipeline()) {
                            // 预调度PP_BWD发送事件
                            addEvent(EndpointType::SENT, PP_COMM_BWD, mb, PP_WAIT, 0);
                            cout << "[1F1B] Last rank " << rank->id << " scheduled PP_BWD SENT for mb=" << mb 
                                 << " after COMPUTE_BWD completion" << endl;
                    }
                } else {  // 正向计算完成
                        completedFwdMicrobatches.insert(mb);

                    if (isLastRankInPipeline()) {
                            // 最后一个rank，正向计算完成后开始反向计算
                            // 注意：反向计算事件已经在handleEvents中预调度，这里不需要重复处理
                            eventState = PP_WAIT; // 等待反向计算事件被处理
                            eventRemainingTime = 0;
                    } else { 
                            // 非最后一个rank，进入PP_WAIT状态
                            eventState = PP_WAIT;
                            eventRemainingTime = 0;
                        }
                    }
                }
                break;
                
            case PP_WAIT:
                // 处理PP等待状态，包括DP通信的接收
                if (eventType == PP_COMM_FWD && ep == EndpointType::RECV) {
                    // 接收到PP通信，开始计算
                    eventState = COMPUTE;
                    eventRemainingTime = workload->fwdCompTime;
                    cout << "[PROGRESS] Rank " << rank->id << " starting COMPUTE_FWD for mb=" << mb 
                         << " at rankGlobalTime=" << rankGlobalTime << endl;
                } else if (eventType == DP_COMM_EVENT && ep == EndpointType::RECV) {
                    // 接收到DP通信，进入DONE状态
                    eventState = DONE;
                    eventRemainingTime = 0;
                    cout << "[PROGRESS] Rank " << rank->id << " DP communication completed, entering DONE state" << endl;
                }
                break;
                
            case TP_COMM:
                // 处理TP通信状态
                if ((eventType == TP_COMM_FWD || eventType == TP_COMM_BWD) && ep == EndpointType::RECV) {
                    // TP通信完成，进行状态转换
                    // 优先触发PP_WAIT，只有在PP<=1时才直接触发COMPUTE
                    if (workload->PP > 1) {
                        eventState = PP_WAIT;
                    } else { 
                        eventState = COMPUTE;
                    }
                    eventRemainingTime = 0;
                    
                    cout << "[PROGRESS] Rank " << rank->id << " TP communication completed for mb=" << mb 
                         << ", state changed to " << simulator->stateToString(eventState) << endl;
                    
                    // TP通信完成后，根据新状态调度后续事件
                    if (eventState == PP_WAIT) {
                        // PP>1，TP通信完成后需要PP通信
                        if (mb > 0) {
                            addEvent(EndpointType::SENT, PP_COMM_FWD, mb, PP_WAIT, 0);
                            cout << "[TP->PP] Rank " << rank->id << " scheduled PP_COMM_FWD for mb=" << mb << " after TP communication" << endl;
                    } else {
                            addEvent(EndpointType::SENT, PP_COMM_BWD, mb, PP_WAIT, 0);
                            cout << "[TP->PP] Rank " << rank->id << " scheduled PP_COMM_BWD for mb=" << mb << " after TP communication" << endl;
                        }
                    }
                    // 注意：如果eventState == COMPUTE，说明PP<=1，TP通信完成后不需要调度新的计算事件
                    // 因为相应的计算事件应该已经存在于事件队列中，或者会由其他机制触发
                }
                break;
                
            default:
                break;
        }
    }
    
    // 处理通信发送事件，通过GroupTask管理通信
    for (auto it = events.begin(); it != events.end(); ) {
        int ep = get<0>(*it);
        int eventType = get<1>(*it);
        int mb = get<2>(*it);
        RankState eventState = get<3>(*it);
        
        if (eventType == TP_COMM_FWD && ep == EndpointType::SENT) {
            cout << "[PROGRESS] Processing TP_FWD SENT event for mb=" << mb << endl;
            
            // 通过GroupTask管理TP前向通信，而不是直接添加RECV事件
            if (tpFwdGroupTask) {
                // 向GroupTask添加事件，让它管理通信流程
                tpFwdGroupTask->addEvent(rank->id, mb);
                cout << "[PROGRESS] Added TP_FWD event to GroupTask " << tpFwdGroupTask->group->id 
                     << " for mb=" << mb << endl;
            }
            
            // 删除已处理的TP_FWD SENT事件
            it = events.erase(it);
        } else if (eventType == TP_COMM_BWD && ep == EndpointType::SENT) {
            cout << "[PROGRESS] Processing TP_BWD SENT event for mb=" << mb << endl;
            
            // 通过GroupTask管理TP后向通信，而不是直接添加RECV事件
            if (tpBwdGroupTask) {
                // 向GroupTask添加事件，让它管理通信流程
                tpBwdGroupTask->addEvent(rank->id, mb);
                cout << "[PROGRESS] Added TP_BWD event to GroupTask " << tpBwdGroupTask->group->id 
                     << " for mb=" << mb << endl;
            }
            
            // 删除已处理的TP_BWD SENT事件
            it = events.erase(it);
        } else if (eventType == PP_COMM_FWD && ep == EndpointType::SENT) {
            cout << "[PROGRESS] Processing PP_FWD SENT event for mb=" << mb << endl;
            
            // 通过GroupTask管理PP通信，而不是直接添加RECV事件
                        if (ppFwdGroupTask) {
                // 向GroupTask添加事件，让它管理通信流程
                ppFwdGroupTask->addEvent(rank->id, mb);
                cout << "[PROGRESS] Added PP_FWD event to GroupTask " << ppFwdGroupTask->group->id 
                     << " for mb=" << mb << endl;
            }
            
            // 删除已处理的PP_FWD SENT事件
            it = events.erase(it);
        } else if (eventType == PP_COMM_BWD && ep == EndpointType::SENT) {
            cout << "[PROGRESS] Processing PP_BWD SENT event for mb=" << mb << endl;
            
            // 通过GroupTask管理PP反向通信，而不是直接添加RECV事件
            if (ppBwdGroupTask) {
                // 向GroupTask添加事件，让它管理通信流程
                ppBwdGroupTask->addEvent(rank->id, mb);
                cout << "[PROGRESS] Added PP_BWD event to GroupTask " << ppBwdGroupTask->group->id 
                     << " for mb=" << mb << endl;
            }
            
            // 删除已处理的PP_BWD SENT事件
            it = events.erase(it);
        } else if (eventType == DP_COMM_EVENT && ep == EndpointType::SENT) {
            cout << "[PROGRESS] Processing DP_COMM SENT event for gradient sync" << endl;
            
            // 通过GroupTask管理DP通信，而不是直接添加RECV事件
            if (dpGroupTask) {
                // 向GroupTask添加事件，让它管理通信流程
                dpGroupTask->addEvent(rank->id, 0); // DP通信使用mb=0表示梯度同步
                cout << "[PROGRESS] Added DP_COMM event to GroupTask " << dpGroupTask->group->id 
                     << " for gradient sync" << endl;
            }
            
            // 删除已处理的DP_COMM SENT事件
            it = events.erase(it);
                    } else {
            ++it;
        }
    }
    
    // 更新全局状态（选择最优先的microbatch）
    updateGlobalState();
}

void RankTask::updateGlobalState() {
    // 找到最优先的microbatch作为当前状态
    int priorityMb = 0;
    RankState priorityState = PP_WAIT;
    double priorityRemainingTime = 0;
    
    for (const auto& event : events) {
        int mb = get<2>(event);
        RankState eventState = get<3>(event);
        double eventRemainingTime = get<4>(event);
        
        // 计算优先级：COMPUTE > TP_COMM > PP_WAIT
        int currentPriority = 0;
        if (eventState == COMPUTE) currentPriority = 3;
        else if (eventState == TP_COMM) currentPriority = 2;
        else if (eventState == PP_WAIT) currentPriority = 1;
        
        int existingPriority = 0;
        if (priorityState == COMPUTE) existingPriority = 3;
        else if (priorityState == TP_COMM) existingPriority = 2;
        else if (priorityState == PP_WAIT) existingPriority = 1;
        
        if (currentPriority > existingPriority) {
            priorityMb = mb;
            priorityState = eventState;
            priorityRemainingTime = eventRemainingTime;
        }
    }
    
    // 更新全局状态
    microbatch = priorityMb;
    state = priorityState;
    remainingTime = priorityRemainingTime;
    
    // 注意：microbatch状态现在由全局MicrobatchManager管理
    // 这里只更新rank的全局状态，不再直接操作microbatchStates
    
    cout << "[PROGRESS] Rank " << rank->id << " updated global state: mb=" << microbatch 
         << ", state=" << simulator->stateToString(state) << ", remainingTime=" << remainingTime << endl;
}

double RankTask::calculateNewRankGlobalTime(double time, int mb, double eventStartTime) {
    cout << "[TIME-CALC] Rank " << rank->id << " calculating new global time, current=" << rankGlobalTime 
         << ", time step=" << time << ", mb=" << mb << ", eventStartTime=" << eventStartTime << endl;
    
    // 检查事件是否已经完成
    if (eventStartTime + time <= rankGlobalTime) {
        cout << "[TIME-CALC] Event already completed, no time update needed" << endl;
        return rankGlobalTime;
    }
    
    if (abs(lastProcessedMb) == abs(mb)) {
        // 同一个microbatch的事件，串行执行
        cout << "[TIME-CALC] Same microbatch (abs: " << abs(mb) << "), serial execution" << endl;
        return rankGlobalTime + time;
                        } else {
        // 不同microbatch的事件，可以并行
        // 对于TP并行，同一TP组内的rank应该独立推进时间
        // 只有在PP通信时才需要考虑microbatch globalTime进行同步
        
        // 检查当前rank是否真正需要TP通信
        bool needsTPCommunication = (workload->TP > 1);
        // 检查当前rank是否真正需要DP通信
        bool needsDPCommunication = (workload->DP > 1);
        
        if (needsTPCommunication || needsDPCommunication) {
            // 需要TP或DP通信的rank，独立推进时间，不依赖microbatch globalTime
            cout << "[TIME-CALC] Rank " << rank->id << " needs " 
                 << (needsTPCommunication ? "TP" : "DP") << " communication, independent time progression" << endl;
            double mbGlobalTime = MicrobatchManager::getMicrobatchGlobalTime(mb);
            cout << "[TIME-CALC] mb: " << abs(mb) << ", mbGlobalTime: " << mbGlobalTime << endl;
            return mbGlobalTime;
        } else {
            // 不需要TP或DP通信的rank，使用microbatch globalTime进行同步
            double mbGlobalTime = MicrobatchManager::getMicrobatchGlobalTime(mb);
            cout << "[TIME-CALC] mb: " << abs(mb) << ", mbGlobalTime: " << mbGlobalTime << endl;
            double newTime = std::max(rankGlobalTime + time, mbGlobalTime);
            cout << "[TIME-CALC] Different microbatch (abs: " << abs(mb) << " vs " << abs(lastProcessedMb) 
                 << "), parallel execution with microbatch sync, new time=" << newTime << endl;
            return newTime;
        }
    }
}

// MicrobatchManager的静态方法实现
void MicrobatchManager::updateMicrobatchGlobalTime(int mb, double time) {
    cout << "[MICROBATCH-TIME-DEBUG] updateMicrobatchGlobalTime called: mb=" << mb << ", time=" << time << endl;
    
    // 确保microbatch状态存在
    if (microbatchStates.find(mb) == microbatchStates.end()) {
        cout << "[MICROBATCH-TIME-DEBUG] Creating new microbatch state for mb=" << mb << endl;
        microbatchStates[mb] = MicrobatchState(mb);
        
        // 对于反向microbatch（负数），初始化为其对应正向microbatch的全局时间
        if (mb < 0) {
            int positiveMb = -mb;
            auto it = microbatchStates.find(positiveMb);
            if (it != microbatchStates.end()) {
                microbatchStates[mb].globalTime = it->second.globalTime;
                cout << "[MICROBATCH-TIME] Global initialized backward microbatch " << mb 
                     << " with forward microbatch " << positiveMb << " time: " << it->second.globalTime << endl;
            }
                        }
                    } else {
        auto it = microbatchStates.find(mb);
        if (it != microbatchStates.end()) {
            cout << "[MICROBATCH-TIME-DEBUG] Microbatch " << mb << " already exists with time=" << it->second.globalTime << endl;
        } else {
            cout << "[MICROBATCH-TIME-DEBUG] ERROR: Microbatch " << mb << " should exist but not found!" << endl;
        }
    }
    
    // 对于TP并行，microbatch globalTime主要用于PP通信的同步
    // TP组内的rank应该独立推进时间，不应该被microbatch globalTime限制
    // 因此，我们只在PP通信完成时更新microbatch globalTime
    
    // 取较大值，确保microbatch时间只能向前推进
    auto it = microbatchStates.find(mb);
    if (it == microbatchStates.end()) {
        cout << "[MICROBATCH-TIME-DEBUG] ERROR: Microbatch " << mb << " not found after creation!" << endl;
        return;
    }
    double currentTime = it->second.globalTime;
    double newTime = std::max(currentTime, time);
    
    if (newTime > currentTime) {
        it->second.globalTime = newTime;
        cout << "[MICROBATCH-TIME] Global updated microbatch " << mb 
             << " global time from " << currentTime << " to " << newTime << endl;
        
        // 验证更新后的值
        double verifyTime = it->second.globalTime;
        if (std::isnan(verifyTime) || std::isinf(verifyTime) || verifyTime < 0) {
            cout << "[MICROBATCH-TIME-ERROR] Invalid value after update: " << verifyTime << " for mb=" << mb << endl;
            it->second.globalTime = currentTime; // 回滚到原值
        }
    } else {
        cout << "[MICROBATCH-TIME] Global microbatch " << mb 
             << " time unchanged: " << currentTime << " (requested: " << time << ")" << endl;
    }
}

double MicrobatchManager::getMicrobatchGlobalTime(int mb) {
    cout << "[MICROBATCH-TIME-DEBUG] getMicrobatchGlobalTime called for mb=" << mb << endl;
    cout << "[MICROBATCH-TIME-DEBUG] microbatchStates.size=" << microbatchStates.size() << endl;
    
    auto it = microbatchStates.find(mb);
    if (it != microbatchStates.end()) {
        double result = it->second.globalTime;
        cout << "[MICROBATCH-TIME] Global get MicrobatchGlobalTime for mb=" << mb 
             << ", using mb=" << mb << " time: " << result << endl;
        
        // 安全检查返回值
        if (std::isnan(result) || std::isinf(result) || result < 0) {
            cout << "[MICROBATCH-TIME-ERROR] Invalid globalTime detected: " << result << " for mb=" << mb << endl;
            return 0.0;
        }
        
        return result;
    }
    
    // 对于反向microbatch（负数），尝试获取其对应正向microbatch的时间
    if (mb < 0) {
        int positiveMb = -mb;
        auto positiveIt = microbatchStates.find(positiveMb);
        if (positiveIt != microbatchStates.end()) {
            cout << "[MICROBATCH-TIME] Global get MicrobatchGlobalTime for backward mb=" << mb 
                 << ", using forward mb=" << positiveMb << " time: " << positiveIt->second.globalTime << endl;
            return positiveIt->second.globalTime;
        }
    }
    
    // 如果没有找到，且abs(mb) > 1，尝试获取前一个microbatch的时间
    if (abs(mb) > 1) {
        int prevMb = (mb > 0) ? (mb - 1) : (mb + 1);
        auto prevIt = microbatchStates.find(prevMb);
        if (prevIt != microbatchStates.end()) {
            cout << "[MICROBATCH-TIME] Global get MicrobatchGlobalTime for mb=" << mb 
                 << ", using previous mb=" << prevMb << " time: " << prevIt->second.globalTime << endl;
            return prevIt->second.globalTime;
        }
    }
    
    // 如果都没有找到，返回0作为默认值
    cout << "[MICROBATCH-TIME] Global get MicrobatchGlobalTime failed, mb: " << mb << ", returning 0" << endl;
    return 0.0;
}



void MicrobatchManager::printStates() {
    cout << "[MICROBATCH-TIME] Global Microbatch States:" << endl;
    for (const auto& pair : microbatchStates) {
        cout << "  MB=" << pair.first << ", GlobalTime=" << pair.second.globalTime << endl;
    }
}


GroupTask::GroupTask(Group* group) : group(group), groupGlobalTime(0) {
    group->groupTask = this;
    this->group = group;
    activeCollective = nullptr;
    this->senders.clear();
    this->receivers.clear();
}


void GroupTask::printStates(){
    cout << "---------------------------" << endl;
    cout << "GroupTask:" ;
    cout << " Group: " << group->id ;
    cout << ", GroupType: " ;
    switch(group->type) {
        case GroupType::TP:
            cout << "TP";
            break;
        case GroupType::PP:
            cout << "PP";
            break;
        case GroupType::DP:
            cout << "DP";
            break;
    }
    cout << ", Group Senders: ";
    for(auto rankTask : senders) {
        cout << rankTask->rank->id << " ";
    }
    cout << ", Group Receivers: ";
    for(auto rankTask : receivers) {
        cout << rankTask->rank->id << " ";
    }
    cout << ", GroupGlobalTime: " << groupGlobalTime;

    cout << ", Waiting collectives: ";
    for(auto collective : waitingCollectives) {
        cout << collective->microbatch << " ";
    }
    cout << "; Accumulating collectives: ";
    for(auto it : accumulatingCollectives) {
        cout << "microbatch: " << it.first <<  ", accmSize: " << it.second->accumulatedInvocations << "; ";
    }

    cout << endl;
    cout << "Active collective: " ;
    if(activeCollective != nullptr) {
        activeCollective->printStates();
    }
    else {
        cout << "None" ;
    }
    cout << endl;
    cout << "Events: " << events.size() << ": ";
    for(auto event : events) {
        string event_str = "<From: " + to_string(get<0>(event)) + ", ";
        event_str += "microbatch: " + to_string(get<1>(event)) + ">";
        cout << event_str << " ";
    }
    cout << endl;
}


void GroupTask::addEvent(int from, int mb) {
    events.emplace_back(from, mb);
    cout << "[GroupTask Event Added] GroupId=" << group->id 
            << " | GroupType=" << simulator->groupTypeToString(group->type)
            << " | From=" << from 
            << " | MB=" << mb 
            << " | Total Events=" << events.size() << endl;
}

int GroupTask::handleEvents(){  // < From, MB >
    // get from, mb
    int countEvents = events.size();
    cout << "handleEvents for GroupTask: " << group->id << ", events size: " << countEvents << endl;
    for (auto it = events.begin(); it != events.end(); ) {
        int from = get<0>(*it);  // 事件来源
        int mb = get<1>(*it);    // 微批次
        cout << "[GROUP] Group " << group->id << " (" << simulator->groupTypeToString(group->type) 
             << ") received event from rank " << from 
             << " for microbatch " << mb << endl;

        // 确定事件类型（根据通信方向和组类型）
        EventType evtType;
        if (group->type == TP) {
            evtType = (mb > 0) ? TP_COMM_FWD : TP_COMM_BWD;
        } 
        else if (group->type == PP) {
            evtType = (mb > 0) ? PP_COMM_FWD : PP_COMM_BWD;
        }
        else {
            evtType = DP_COMM_EVENT;
        }


        // 检查是否有等待的集合操作
        if (accumulatingCollectives.find(mb) == accumulatingCollectives.end()) {
            Collective* collective = new Collective(group, mb, group->type == GroupType::PP ? 1 : group->ranks.size());
            accumulatingCollectives[mb] = collective;
        }
        else {
            accumulatingCollectives[mb]->accumulatedInvocations++;
        }
        // 移除已处理的事件
        it = events.erase(it);
    }

    for(auto it = accumulatingCollectives.begin(); it != accumulatingCollectives.end(); ) {
        int mb = it->first;
        Collective* collective = it->second;

        // 如果集合操作已完成，移动到等待队列
        if (collective->accumulatedInvocations == collective->accumulatedSize) {
            waitingCollectives.push_back(collective);
            it = accumulatingCollectives.erase(it);
        }
        else {
            ++it;
        }
    }
    // 如果没有活动的集合操作，尝试激活一个等待的集合操作
    if (activeCollective == nullptr && !waitingCollectives.empty()) {
        activeCollective = waitingCollectives.front();
        waitingCollectives.erase(waitingCollectives.begin());

        for (auto& evt : simulator->commEvents) {
            if (evt.endTime < 0 && evt.type == group->type && 
                evt.microbatch == activeCollective->microbatch) {
                evt.startTime = groupGlobalTime; // 更新为实际开始时间
            }
        }

    }

    return countEvents - events.size();
}


double GroupTask::stableTime(){
    if (activeCollective==nullptr) {
        if(waitingCollectives.empty()) {
            return numeric_limits<double>::infinity();
        }
        else{
            return 0;
        }
    }
    else{
        return activeCollective->stableTime();
    }
}

void GroupTask::progress(double time){
    cout << "[GROUP-PROGRESS-DEBUG] GroupTask " << group->id << " (" << simulator->groupTypeToString(group->type) 
         << ") progress called with time=" << time << endl;
    cout << "[GROUP-PROGRESS-DEBUG] Current state: activeCollective=" << (activeCollective ? "exists" : "null") 
         << ", waitingCollectives.size=" << waitingCollectives.size() 
         << ", groupGlobalTime=" << groupGlobalTime << endl;
    
    // 更新group的全局时间（基于发送和接收方rank的时间）
    // 注意：这里暂时传入0，因为此时可能还没有activeCollective
    // updateGroupGlobalTime(0);
    
    // move waiting to active
    if(activeCollective == nullptr) {
        cout << "[GROUP-PROGRESS-DEBUG] No active collective, checking waiting queue..." << endl;
        if(!waitingCollectives.empty()) {
            activeCollective = waitingCollectives.front();
            waitingCollectives.erase(waitingCollectives.begin());
            cout << "[GROUP-PROGRESS-DEBUG] Activated collective for mb=" << activeCollective->microbatch << endl;
        } else {
            cout << "[GROUP-PROGRESS-DEBUG] No waiting collectives, returning early" << endl;
        }
    }
    if(activeCollective == nullptr) {
        cout << "[GROUP-PROGRESS-DEBUG] Still no active collective, returning" << endl;
        return;
    }
    
    cout << "[GROUP-PROGRESS-DEBUG] Processing active collective for mb=" << activeCollective->microbatch << endl;
    
    try {
        cout << "[GROUP-PROGRESS-DEBUG] Calling activeCollective->progress(" << time << ")" << endl;
    activeCollective->progress(time);
        cout << "[GROUP-PROGRESS-DEBUG] Successfully completed activeCollective->progress()" << endl;
    } catch (const std::exception& e) {
        cout << "[GROUP-PROGRESS-ERROR] Exception in activeCollective->progress(): " << e.what() << endl;
        return;
    } catch (...) {
        cout << "[GROUP-PROGRESS-ERROR] Unknown exception in activeCollective->progress()" << endl;
        return;
    }

    // 安全检查activeCollective和flows
    if (activeCollective == nullptr) {
        cout << "[GROUP-PROGRESS-ERROR] activeCollective became nullptr after progress()" << endl;
        return;
    }
    
    if (activeCollective->flows.empty()) {
        cout << "[GROUP-PROGRESS-ERROR] activeCollective->flows is empty" << endl;
        return;
    }
    
    if (activeCollective->flows[0] == nullptr) {
        cout << "[GROUP-PROGRESS-ERROR] activeCollective->flows[0] is nullptr" << endl;
        return;
    }
    
    cout << "[GROUP-PROGRESS-DEBUG] After collective progress, flows[0]->remainingSize=" 
         << activeCollective->flows[0]->remainingSize << endl;

    if(activeCollective && activeCollective->flows[0]->remainingSize <= 1e-6 ) {   // EP TYPE MB
        cout << "[GROUP-PROGRESS-DEBUG] Communication completed for mb=" << activeCollective->microbatch << endl;
        
        EventType commType;
        int mb = activeCollective->microbatch;
        
        cout << "[GROUP-PROGRESS-DEBUG] Before updateGroupGlobalTime, groupGlobalTime=" << groupGlobalTime << endl;
        
        // 使用正确的microbatch更新group时间
        updateGroupGlobalTime(mb);
        
        // 计算通信事件的起止时间
        // 通信事件从现在开始，持续time时间
        double startTime = groupGlobalTime;  // 事件从现在开始
        double endTime = groupGlobalTime + time;  // 事件在time时间后结束

        if (group->type == GroupType::TP) {
            commType = (mb > 0) ? TP_COMM_FWD : TP_COMM_BWD;
        } 
        else if (group->type == GroupType::PP) {
            commType = (mb > 0) ? PP_COMM_FWD : PP_COMM_BWD;
        }
        else if (group->type == GroupType::DP) {
            commType = DP_COMM_EVENT;
        }

        cout << "[GROUP-PROGRESS] Group=" << group->id 
            << " | Type=" << simulator->groupTypeToString(group->type)
            << " | MB=" << mb
            << " | CommType=" << simulator->eventTypeToString(commType)
            << " | StartTime=" << startTime
            << " | EndTime=" << endTime
            << " | Duration=" << (endTime - startTime) << endl;

        // 检查是否已经处理过这个microbatch的通信
        bool alreadyProcessed = false;
        for(auto& evt : simulator->commEvents) {
            if(evt.type == group->type && evt.microbatch == mb && evt.endTime > 0) {
                alreadyProcessed = true;
                cout << "[GROUP-DUPLICATE] Skip duplicate event for MB=" << mb << endl;
                break;
            }
        }

        if(!alreadyProcessed) {
            // 记录通信完成时间
            for (auto& evt : simulator->commEvents) {
                if (evt.endTime < 0 && evt.type == group->type && 
                    evt.microbatch == mb) {
                    evt.endTime = groupGlobalTime;

                    // 调试日志：打印每个通信事件的详细信息
                    cout << "[STAT] Type=" << simulator->groupTypeToString(group->type) 
                        << " MB=" << mb
                        << " Duration=" << (evt.endTime - evt.startTime)
                        << " From=" << evt.startTime 
                        << " To=" << evt.endTime << endl;
                    
                    // 记录通信的timeline事件
                    if (group->type == GroupType::PP || group->type == GroupType::TP || group->type == GroupType::DP) {
                        // 记录发送方的事件
                        for (auto sender : senders) {
                            string sentInfo;
                            if (group->type == GroupType::PP) sentInfo = "PP_COMM sent";
                            else if (group->type == GroupType::TP) sentInfo = "TP_COMM sent";
                            else if (group->type == GroupType::DP) sentInfo = "DP_COMM sent";
                            
                            simulator->recordTimelineEvent(
                                sender->rank->id, group->type, EndpointType::SENT,
                                commType, mb, startTime, endTime, sentInfo
                            );
                        }
                        
                        // 更新接收方的事件结束时间（如果已存在）
                        for (auto receiver : receivers) {
                            bool found = false;
                            for (auto& timelineEvent : simulator->timelineEvents) {
                                if (timelineEvent.rank == receiver->rank->id && 
                                    timelineEvent.microbatch == mb && 
                                    timelineEvent.endTime < 0 &&
                                    ((group->type == GroupType::PP && 
                                      (timelineEvent.eventType == PP_COMM_FWD || timelineEvent.eventType == PP_COMM_BWD)) ||
                                     (group->type == GroupType::TP && 
                                      (timelineEvent.eventType == TP_COMM_FWD || timelineEvent.eventType == TP_COMM_BWD)) ||
                                     (group->type == GroupType::DP && 
                                      timelineEvent.eventType == DP_COMM_EVENT))) {
                                    timelineEvent.endTime = endTime;
                                    cout << "[TIMELINE] Updated RECV end time for rank " << receiver->rank->id 
                                         << " mb=" << mb << " to " << endTime << endl;
                                    found = true;
                                    break;
                                }
                            }
                            
                            // 如果没有找到已存在的RECV事件，创建新的
                            if (!found) {
                                string receivedInfo;
                                if (group->type == GroupType::PP) receivedInfo = "PP_COMM received";
                                else if (group->type == GroupType::TP) receivedInfo = "TP_COMM received";
                                else if (group->type == GroupType::DP) receivedInfo = "DP_COMM received";
                                
                                simulator->recordTimelineEvent(
                                    receiver->rank->id, group->type, EndpointType::RECV,
                                    commType, mb, startTime, endTime, receivedInfo
                                );
                            }
                        }
                    }
                    
                    // 更新统计（以rank 0为基准）
                    // if (senders[0]->rank->id == 0 || receivers[0]->rank->id == 0) {
                        double duration = evt.endTime - evt.startTime;
                        if (group->type == GroupType::TP) {
                            if (mb > 0) 
                                simulator->commStats.tpForward += duration;
                            else 
                                simulator->commStats.tpBackward += duration;
                        } 
                        else if (group->type == GroupType::PP) {
                            if (mb > 0) 
                                simulator->commStats.ppForward += duration;
                            else 
                                simulator->commStats.ppBackward += duration;
                        }
                        else if (group->type == GroupType::DP) {
                            simulator->commStats.dpTotal += duration;
                        }
                    // }
                }
            }
        }

        // 1. 更新接收方完成状态
        for (auto rankTask : receivers) {
            switch(group->type) {
                case GroupType::PP:
                    if (mb > 0) {
                        rankTask->completedFwdMicrobatches.insert(mb);
                    } else {
                        rankTask->completedBwdMicrobatches.insert(mb);
                        // 如果是最后一个 rank 的完成事件，同步全局状态
                        if (rankTask->isLastRankInPipeline()) {
                            for (auto r : group->ranks) {
                                r->rankTask->completedBwdMicrobatches.insert(mb);
                            }
                        }
                    }
                    break;
                case GroupType::DP:
                    if (mb == 0) { // 梯度同步
                        for (int i = 1; i <= workload->microbatches; ++i) {
                            rankTask->completedBwdMicrobatches.insert(-i);
                        }
                    }
                    break;
                // TP不需要特殊处理
            }
        }

        // 注意：不应该通知senders，因为senders已经完成了发送
        // 通知senders会导致死循环，因为发送方会再次收到相同的事件
        // 只有接收方需要收到RECV事件

        // notify receivers
        // 为了在timeline上展示，故只记录RECV方的PP
        
        // 调试：打印receivers信息
        cout << "[DEBUG-RECEIVERS] Group " << group->id << " (" << simulator->groupTypeToString(group->type) 
             << ") receivers count: " << receivers.size() << endl;
        for (auto rankTask : receivers) {
            cout << "  Receiver rank: " << rankTask->rank->id << endl;
        }
        
        // 对于TP通信，需要为所有参与者添加RECV事件，但要避免重复
        if (group->type == GroupType::TP) {
            cout << "[DEBUG] TP communication completed, adding RECV events to all participants" << endl;
            // 使用set来避免重复添加
            set<int> notifiedRanks;
            for(auto rankTask: receivers){
                if (notifiedRanks.find(rankTask->rank->id) == notifiedRanks.end()) {
                    notifiedRanks.insert(rankTask->rank->id);
                    
                    string receiverInfo = "RECEIVER [" + to_string(rankTask->rank->id) + "]";
                    simulator->recordTimelineEvent(
                        rankTask->rank->id,
                        group->type,          // GroupType 
                        EndpointType::RECV,   // EndpointType
                        commType,        // EventType
                        mb,
                        startTime,
                        endTime,
                        receiverInfo           // 可选附加信息
                    );
                    
                    // 更新microbatch的全局时间
                    MicrobatchManager::updateMicrobatchGlobalTime(mb, endTime);
                    
                    rankTask->addEvent(EndpointType::RECV, commType, mb, TP_COMM, 0);
                    cout << "[DEBUG] Added TP RECV event for rank " << rankTask->rank->id << endl;
                } else {
                    cout << "[DEBUG] Skipped duplicate TP RECV event for rank " << rankTask->rank->id << endl;
                }
            }
        } else {
            // 对于PP和DP通信，正常为receivers添加RECV事件
        for(auto rankTask: receivers){
            bool shouldNotify = true;
            if (group->type == GroupType::DP && mb == 0) {
                shouldNotify = !rankTask->completedBwdMicrobatches.count(workload->microbatches);
            }

            if (shouldNotify) {
                string receiverInfo = "RECEIVER [" + to_string(rankTask->rank->id) + "]";
                simulator->recordTimelineEvent(
                    rankTask->rank->id,
                    group->type,          // GroupType 
                    EndpointType::RECV,   // EndpointType
                    commType,        // EventType
                    mb,
                    startTime,
                    endTime,
                    receiverInfo           // 可选附加信息
                );
                    
                    // 更新microbatch的全局时间
                    MicrobatchManager::updateMicrobatchGlobalTime(mb, endTime);
                    
                    rankTask->addEvent(EndpointType::RECV, commType, mb, PP_WAIT, 0);
                }           
            }           
        }

        cout << "[GROUP-PROGRESS-DEBUG] Cleaning up completed collective for mb=" << mb << endl;

        delete activeCollective;  
        activeCollective = nullptr;
        
        cout << "[GROUP-PROGRESS-DEBUG] After cleanup: waitingCollectives.size=" << waitingCollectives.size() << endl;
        
        // 优先选择与当前流水线阶段匹配的 microbatch
        if (!waitingCollectives.empty()) {
            cout << "[GROUP-PROGRESS-DEBUG] Before erase, waitingCollectives.size=" << waitingCollectives.size() << endl;
            
            int currentDir = mb > 0 ? 1 : -1;
            auto it = find_if(waitingCollectives.begin(), waitingCollectives.end(),
                [currentDir](Collective* c) { 
                    return (c->microbatch > 0) == (currentDir > 0); 
                });
            
            if (it != waitingCollectives.end()) {
                activeCollective = *it;
                cout << "[GROUP-PROGRESS-DEBUG] Found matching collective for mb=" << activeCollective->microbatch << endl;
            waitingCollectives.erase(it);
                cout << "[GROUP-PROGRESS-DEBUG] Erased matching collective, remaining size=" << waitingCollectives.size() << endl;
            } else {
                activeCollective = waitingCollectives.front();
                cout << "[GROUP-PROGRESS-DEBUG] No matching collective found, using front mb=" << activeCollective->microbatch << endl;
                waitingCollectives.erase(waitingCollectives.begin());
                cout << "[GROUP-PROGRESS-DEBUG] Erased front collective, remaining size=" << waitingCollectives.size() << endl;
            }
            
            cout << "[GROUP-PROGRESS-DEBUG] Activated next collective for mb=" << activeCollective->microbatch << endl;
            
            // 添加安全检查
            if (activeCollective == nullptr) {
                cout << "[GROUP-PROGRESS-ERROR] activeCollective is nullptr after activation!" << endl;
                return;
            }
            
        } else {
            cout << "[GROUP-PROGRESS-DEBUG] No more waiting collectives" << endl;
        }
    }

}

void GroupTask::updateGroupGlobalTime(int mb) {
    cout << "[GROUP-TIME-DEBUG] updateGroupGlobalTime called for group " << group->id 
         << " with mb=" << mb << endl;
    
    // 获取microbatch的全局时间作为基础
    double mbGlobalTime = MicrobatchManager::getMicrobatchGlobalTime(mb);
    cout << "[GROUP-TIME-DEBUG] Retrieved mbGlobalTime=" << mbGlobalTime << endl;
    
    // GroupTask的通信任务只需要考虑发起该通信任务的microbatch globalTime
    double newGroupTime = mbGlobalTime;
    
    // 更新group的全局时间
    groupGlobalTime = newGroupTime;
    
    // 更新microbatch的全局时间（如果需要）
    if (mb != 0) {
        cout << "[GROUP-TIME-DEBUG] Calling MicrobatchManager::updateMicrobatchGlobalTime(" << mb << ", " << newGroupTime << ")" << endl;
        MicrobatchManager::updateMicrobatchGlobalTime(mb, newGroupTime);
        cout << "[GROUP-TIME-DEBUG] Successfully updated microbatch global time" << endl;
    }
    
    cout << "[GROUP-TIME] Group " << group->id << " (" << simulator->groupTypeToString(group->type) 
         << ") updated global time to " << groupGlobalTime 
         << " (mbGlobalTime=" << mbGlobalTime 
         << ", mb=" << mb << ")" << endl;
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
    commStats = CommStats();
    
    // 清空microbatch状态，防止全局变量污染
    cout << "[INIT] Before clearing, microbatchStates size: " << MicrobatchManager::microbatchStates.size() << endl;
    MicrobatchManager::microbatchStates.clear();
    cout << "[INIT] After clearing, microbatchStates size: " << MicrobatchManager::microbatchStates.size() << endl;
    cout << "[INIT] Cleared microbatchStates to prevent global variable pollution" << endl;

    globalTime = 0;

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
        if (rank->pp == 0) { // 第一个pipeline stage的rank
            cout << "[INIT-DEBUG] Initializing first pipeline stage rank " << rank->id << endl;
            cout << "[INIT-DEBUG] Initial rankGlobalTime=" << task->rankGlobalTime << endl;
            task->state = RankState::COMPUTE;
            task->remainingTime = workload->fwdCompTime;
            task->microbatch = 1; // 确保microbatch正确设置
            
            // 记录初始计算事件（结束时间将在progress中设置）
            recordTimelineEvent(
                rank->id,
                NONE_GROUP,       // GroupType
                NONE_ENDPOINT,    // EndpointType
                COMPUTE_FWD,      // EventType
                1,                // microbatch
                task->rankGlobalTime, // startTime - 使用rank的独立时间
                task->rankGlobalTime + workload->fwdCompTime,               // endTime - 将在progress中设置
                "Initial fwd compute"
            );
            // 同步更新 microbatch(1) 的全局时间与 rank 的时间
            cout << "[INIT-DEBUG] Before updateMicrobatchGlobalTime: task->rankGlobalTime=" << task->rankGlobalTime 
                 << ", workload->fwdCompTime=" << workload->fwdCompTime 
                 << ", total=" << (task->rankGlobalTime + workload->fwdCompTime) << endl;
            // 直接设置microbatch的全局时间，不依赖handleEvents的调用
            MicrobatchManager::updateMicrobatchGlobalTime(1, workload->fwdCompTime);
            cout << "[INIT-DEBUG] Before rankGlobalTime update: " << task->rankGlobalTime << endl;
            task->rankGlobalTime += workload->fwdCompTime;
            cout << "[INIT-DEBUG] After rankGlobalTime update: " << task->rankGlobalTime << endl;
            
            // 为所有microbatch预调度计算事件：
            // 仅当存在 TP 或 PP 通信时（需要算/通重叠），才一次性预调度后续 FWD。
            // 纯 DP 场景（TP<=1 && PP<=1）下不做预调度，按照 1F1B 顺序逐个推进。
            if (workload->TP > 1 || workload->PP > 1) {
                for (int mb = 2; mb <= workload->microbatches; ++mb) {
                    task->addEvent(EndpointType::NONE_ENDPOINT, EventType::COMPUTE_FWD, mb, COMPUTE, workload->fwdCompTime);
                }
            }
            
            // 为第一个pipeline stage预调度通信事件（TP优先级高于PP）
            // 1. 首先预调度TP_FWD事件（如果TP>1）
            if (workload->TP > 1) {
                // 预调度第一个TP发送事件（在计算完成后触发）
                task->addEvent(EndpointType::SENT, EventType::TP_COMM_FWD, 1, TP_COMM, 0);
                
                // 注意：不在这里预调度GroupTask事件，而是在RankTask::handleEvents中处理
                cout << "[INIT] Pre-scheduled TP_FWD SENT event for rank " << rank->id << " (GroupTask event will be added later)" << endl;
            } else {
                cout << "[INIT] TP=1, no tensor parallelism communication needed for rank " << rank->id << endl;
            }
            
            // 2. 然后预调度PP_FWD事件（如果PP>1）
            if (workload->PP > 1) {
                // 预调度第一个PP发送事件（在计算完成后触发）
                task->addEvent(EndpointType::SENT, EventType::PP_COMM_FWD, 1, PP_WAIT, 0);
                
                // 为下一个rank预调度对应的RECV事件，通过GroupTask管理
                if (rank->ppFwdGroup != nullptr && rank->ppFwdGroup->groupTask != nullptr) {
                    rank->ppFwdGroup->groupTask->addEvent(rank->id, 1);
                    cout << "[INIT] Pre-scheduled PP_FWD event to GroupTask " << rank->ppFwdGroup->id 
                         << " for mb=1 (from rank " << rank->id << ")" << endl;
                }
            } else {
                cout << "[INIT] PP=1, no pipeline communication needed for rank " << rank->id << endl;
            }

            // 纯DP场景（TP=1, PP=1, DP>1）：在首个FWD后预调度首个BWD，保证DP-only流程可运行
            if (workload->TP <= 1 && workload->PP <= 1 && workload->DP > 1) {
                task->addEvent(EndpointType::NONE_ENDPOINT, EventType::COMPUTE_BWD, -1, COMPUTE, workload->bwdCompTime);
                cout << "[INIT] DP-only path: pre-scheduled COMPUTE_BWD for mb=-1 on rank " << rank->id << endl;
            }
            
        } else {
            // 其他pipeline stage的rank保持等待状态，事件将由前一阶段触发
            task->state = PP_WAIT;
            task->microbatch = 0; // 初始化为0，等待事件触发
            cout << "[INIT] Rank " << rank->id << " (pp=" << rank->pp << ") is not the first pipeline stage, events will be triggered by previous stage" << endl;
        }
    }

    // Calculate pure communication times (for reference)
    pureTpCommTime = pureTpFwCommTime = pureTpBwCommTime = 0;
    purePpCommTime = purePpFwCommTime = purePpBwCommTime = 0;
    pureDpCommTime = pureTotalCommTime = 0;

    for (auto group : workload->groups) {
        for (auto conn : group->connections) {
            double commTime = 0;
            double fwCommTime = 0;
            double bwCommTime = 0;
            
            switch (group->type) {
                case GroupType::TP:
                    for (auto link : conn->pathLinks) {
                        fwCommTime += workload->fwdTPSize / link->capacity;
                        bwCommTime += workload->bwdTPSize / link->capacity;
                    }
                    commTime = fwCommTime + bwCommTime;
                    pureTpFwCommTime += fwCommTime;
                    pureTpBwCommTime += bwCommTime;
                    pureTpCommTime += commTime;
                    break;
                    
                case GroupType::PP:
                    for (auto link : conn->pathLinks) {
                        fwCommTime += workload->fwdPPSize / link->capacity;
                        bwCommTime += workload->bwdPPSize / link->capacity;
                    }
                    commTime = fwCommTime + bwCommTime;
                    purePpFwCommTime += fwCommTime;
                    purePpBwCommTime += bwCommTime;
                    purePpCommTime += commTime;
                    break;
                    
                case GroupType::DP:
                    for (auto link : conn->pathLinks) {
                        commTime += workload->dpSize / link->capacity;
                    }
                    pureDpCommTime += commTime;
                    break;
            }
            pureTotalCommTime += commTime;
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

// void Simulator::run(){
//     globalTime=0;
//     cout << "===========================" << endl;
//     int round = 0;
//     int targetRound = -1;    
//     while(true){
//         // cout << "===========================" << endl;
//         // cout << "Global Time: " << globalTime << ", Round " << round << endl;
//         // cout << "----------------------------" << endl;
//         cout << " before handle events" << endl;
//         if(round==targetRound) printStates(); // !!!!!!!!!!!!!!

//         while(1){
//             int countEvents = 0;
//             for(auto task : tasks){
//                 countEvents += task->handleEvents();
//             }
//             if(countEvents == 0) break;
//         }
//         // cout << " after handle events, before update states" << endl;
//         if(round==targetRound) printStates(); // !!!!!!!!!!!!!!
//         // update states
//         updateStates();
//         cout << "----------------------------" << endl;
//         // cout << " after update states " << endl;
//         if(round==targetRound) printStates(); // !!!!!!!!!!!!!!!
//         // stable time
//         double time = numeric_limits<double>::infinity();
//         for(auto task : tasks){
//             double t = task->stableTime();
//             if(t < time) time = t;
//         }
//         // cout << "----------------------------" << endl;
//         // cout << "Stable time: " << time << endl;
//         if(time == numeric_limits<double>::infinity()){
//             break;
//         }
//         // progress
//         for(auto task : tasks){
//             task->progress(time);
//         }
//         globalTime += time;

//         // cout << "Progressed time: " << time << endl;
//         // cout << "---------------------------" << endl;
//         if(round==targetRound )printStates(); // !!!!!!!!!!!!!!!
//         // cout << "===========================" << endl;
//         round++;
//     }
//     cout << "Simulation finished" << endl;
//     cout << "Global Time: " << globalTime << endl;
//     cout << "---------------------------" << endl;
// }

SimResult Simulator::py_run(){
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
    result.pureTpCommTime = pureTpCommTime;
    result.pureTpFwCommTime = pureTpFwCommTime;
    result.pureTpBwCommTime = pureTpBwCommTime;
    result.purePpCommTime = purePpCommTime;
    result.purePpFwCommTime = purePpFwCommTime;
    result.purePpBwCommTime = purePpBwCommTime;
    result.pureDpCommTime = pureDpCommTime;
    result.pureTotalCommTime = pureTotalCommTime;
    
    // 融合打印和收集timeline事件：在遍历过程中同时打印和收集数据
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

    cout << "Simulation finished" << endl;
    cout << "Final Global Time: " << finalGlobalTime << endl;
    cout << "TP Forward Time: " << commStats.tpForward << endl;
    cout << "TP Backward Time: " << commStats.tpBackward << endl;
    cout << "PP Forward Time: " << commStats.ppForward << endl;
    cout << "PP Backward Time: " << commStats.ppBackward << endl;
    cout << "DP Total Time: " << commStats.dpTotal << endl;
    cout << "---------------------------" << endl;

    return result;
}

double RankTask::calculateHandleEventsTime(int mb) {
    cout << "[HANDLE-TIME-CALC] Rank " << rank->id << " calculating handleEvents time for mb=" << mb 
         << ", lastProcessedMb=" << lastProcessedMb << endl;
    
    // 检查当前rank是否真正需要TP通信
    bool needsTPCommunication = (workload->TP > 1);
    // 检查当前rank是否真正需要DP通信
    bool needsDPCommunication = (workload->DP > 1);
    
    if (abs(mb) == abs(lastProcessedMb)) {
        // 同一个microbatch的事件
        if (needsTPCommunication || needsDPCommunication) {
            // 需要TP或DP通信的rank，独立推进时间，不依赖microbatch globalTime
            cout << "[HANDLE-TIME-CALC] Same microbatch (abs: " << abs(mb) << "), " 
                 << (needsTPCommunication ? "TP" : "DP") << " communication rank, using rankGlobalTime=" 
                 << rankGlobalTime << endl;
            return rankGlobalTime;
        } else {
            // 不需要TP或DP通信的rank，使用microbatch globalTime进行同步
            double mbGlobalTime = MicrobatchManager::getMicrobatchGlobalTime(mb);
            double newTime = std::max(rankGlobalTime, mbGlobalTime);
            cout << "[HANDLE-TIME-CALC] Same microbatch (abs: " << abs(mb) << "), non-TP/DP communication rank, using max(rankGlobalTime=" 
                 << rankGlobalTime << ", mbGlobalTime=" << mbGlobalTime << ") = " << newTime << endl;
            return newTime;
        }
    } else {
        // 不同microbatch的事件
        if (needsTPCommunication || needsDPCommunication) {
            // 需要TP或DP通信的rank，独立推进时间，不依赖microbatch globalTime
            cout << "[HANDLE-TIME-CALC] Different microbatch (abs: " << abs(mb) << " vs " << abs(lastProcessedMb) 
                 << "), " << (needsTPCommunication ? "TP" : "DP") << " communication rank, using rankGlobalTime=" 
                 << rankGlobalTime << endl;
            return rankGlobalTime;
        } else {
            // 不需要TP或DP通信的rank，使用microbatch globalTime进行同步
            double mbGlobalTime = MicrobatchManager::getMicrobatchGlobalTime(mb);
            cout << "[HANDLE-TIME-CALC] Different microbatch (abs: " << abs(mb) << " vs " << abs(lastProcessedMb) 
                 << "), non-TP/DP communication rank, using mbGlobalTime=" << mbGlobalTime << endl;
            return mbGlobalTime;
        }
    }
}

void RankTask::updateLastProcessedMb(int mb) {
    if (abs(mb) != abs(lastProcessedMb)) {
        cout << "[LAST-PROCESSED] Rank " << rank->id << " updated: lastProcessedMb=" << lastProcessedMb 
             << " -> " << mb << " (abs: " << abs(mb) << ")" << endl;
        lastProcessedMb = mb;
    }
}
