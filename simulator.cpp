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
                flow->remainingSize = microbatch > 0 ? workload->fwdTPSize 
                                                    : workload->bwdTPSize;
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

RankTask::RankTask(Rank* rank) : rank(rank), state(PP_WAIT), remainingTime(0), rankGlobalTime(0), microbatch(0), lastProcessedMb(0), lastProcessedWasCompute(false) {
    rank->rankTask = this;
    this->rank = rank;
    this->dpGroupTask = nullptr; // corrected assignment
    this->tpGroupTask = nullptr;  // corrected assignment
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
    cout << ", LastProcessed: mb=" << lastProcessedMb << ", compute=" << (lastProcessedWasCompute ? "true" : "false");
    cout << ", MicrobatchStates: ";
    for (const auto& pair : MicrobatchManager::microbatchStates) {
        cout << "mb" << pair.first << "(time=" << pair.second.globalTime << ", compute=" << pair.second.lastComputeEndTime << ") ";
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
    if (microbatch != 0) {
        MicrobatchManager::updateMicrobatchGlobalTime(microbatch, rankGlobalTime);
    }

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
        if (mb != 0) {
            MicrobatchManager::updateMicrobatchGlobalTime(mb, rankGlobalTime);
        }

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
                if (ep == EndpointType::RECV && microbatch > 0 && mb == microbatch) {
                    // 接收到PP前向通信，开始计算
                    state = COMPUTE;
                    remainingTime = workload->fwdCompTime;
                    shouldErase = true;
                    
                    // 注意：PP通信事件已经在GroupTask::progress中记录，这里不需要重复记录
                    // 只记录计算开始事件
                    // 使用智能时间步进计算开始时间和结束时间
                    double startTime = calculateHandleEventsTime(mb, 0); // 计算开始时间
                    double endTime = startTime + workload->fwdCompTime; // 计算结束时间
                    simulator->recordTimelineEvent(
                        rank->id, NONE_GROUP, NONE_ENDPOINT,
                        COMPUTE_FWD, mb, startTime, endTime,
                        "COMPUTE_FWD started after PP_FWD RECV"
                    );
                    
                    // 更新microbatch的全局时间和计算时间
                    MicrobatchManager::updateMicrobatchGlobalTime(mb, endTime);
                    MicrobatchManager::updateMicrobatchComputeTime(mb, endTime);
                    // 注意：不更新rankGlobalTime，保持rank的时间独立性
                    
                    // 更新上一个处理的事件信息
                    updateLastProcessedEvent(mb, true); // COMPUTE_FWD是计算事件
                    
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
                if (ep == EndpointType::RECV && microbatch < 0 && microbatch == mb) {
                    state = COMPUTE;
                    microbatch = mb;
                    remainingTime = workload->bwdCompTime;
                    shouldErase = true;
                    
                    // 注意：PP通信事件已经在GroupTask::progress中记录，这里不需要重复记录
                    // 只记录计算开始事件
                    // 使用智能时间步进计算开始时间和结束时间
                    double startTime = calculateHandleEventsTime(mb, 0); // 计算开始时间
                    double endTime = startTime + workload->bwdCompTime; // 计算结束时间
                    
                    simulator->recordTimelineEvent(
                        rank->id, NONE_GROUP, NONE_ENDPOINT,
                        COMPUTE_BWD, mb, startTime, endTime,
                        "COMPUTE_BWD started after PP_BWD RECV"
                    );
                    
                    // 更新microbatch的全局时间和计算时间
                    MicrobatchManager::updateMicrobatchGlobalTime(mb, endTime);
                    MicrobatchManager::updateMicrobatchComputeTime(mb, endTime);
                    // 注意：不更新rankGlobalTime，保持rank的时间独立性
                    
                    // 更新上一个处理的事件信息
                    updateLastProcessedEvent(mb, true); // COMPUTE_BWD是计算事件
                    
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
                if (ep == EndpointType::RECV && state == TP_COMM) {
                    state = (isLastRankInPipeline()) ? COMPUTE : PP_WAIT;
                    shouldErase = true;
                    
                    // 记录TP通信接收事件
                    simulator->recordTimelineEvent(
                        rank->id, GroupType::TP, EndpointType::RECV,
                        static_cast<EventType>(eventType), mb, rankGlobalTime,
                        rankGlobalTime, // TP通信接收是瞬间完成的
                        "TP_COMM received"
                    );
                }
                else if (ep == EndpointType::SENT) {
                    // 记录TP通信发送事件
                    simulator->recordTimelineEvent(
                        rank->id, GroupType::TP, EndpointType::SENT,
                        static_cast<EventType>(eventType), mb, rankGlobalTime,
                        rankGlobalTime, // TP通信发送是瞬间完成的
                        "TP_COMM sent"
                    );
                    
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
                    double startTime = calculateHandleEventsTime(mb, 0); // 计算开始时间
                    double endTime = startTime + workload->fwdCompTime; // 计算结束时间
                    
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
                    
                    // 更新microbatch的全局时间和计算时间
                    MicrobatchManager::updateMicrobatchGlobalTime(mb, endTime);
                    MicrobatchManager::updateMicrobatchComputeTime(mb, endTime);
                    // 注意：不更新rankGlobalTime，保持rank的时间独立性
                    
                    // 更新上一个处理的事件信息
                    updateLastProcessedEvent(mb, true); // COMPUTE_FWD是计算事件
                    
                    // 为第一个rank，预调度计算完成后的PP发送事件
                    if (isFirstRankInPipeline()) {
                        pending_events.emplace_back(EndpointType::SENT, PP_COMM_FWD, mb, PP_WAIT, 0);
                        cout << "Rank " << rank->id << " scheduled PP_COMM_FWD for mb=" << mb << endl;
                    }
                }
                break;

            case COMPUTE_BWD:
                // 反向计算事件由progress处理，这里记录完整的计算时间
                if (ep == EndpointType::NONE_ENDPOINT) {
                    cout << "Rank " << rank->id << " scheduling COMPUTE_BWD for mb=" << mb 
                         << " (previous microbatch=" << microbatch << ")" << endl;
                    
                    // 使用智能时间步进计算开始时间和结束时间
                    double startTime = calculateHandleEventsTime(mb, 0); // 计算开始时间
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
                    
                    // 更新microbatch的全局时间和计算时间
                    MicrobatchManager::updateMicrobatchGlobalTime(mb, endTime);
                    MicrobatchManager::updateMicrobatchComputeTime(mb, endTime);
                    // 注意：不更新rankGlobalTime，保持rank的时间独立性
                    
                    // 更新上一个处理的事件信息
                    updateLastProcessedEvent(mb, true); // COMPUTE_BWD是计算事件
                    
                    // 对于反向传播，除了第一个rank外，都需要触发PP_BWD事件
                    if (!isFirstRankInPipeline()) {
                        pending_events.emplace_back(EndpointType::SENT, PP_COMM_BWD, mb, PP_WAIT, 0);
                        cout << "[1F1B] Rank " << rank->id << " scheduled PP_BWD SENT for mb=" << mb 
                             << " after COMPUTE_BWD scheduling" << endl;
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
            // 更新上一个处理的事件信息
            bool wasCompute = (eventType == COMPUTE_FWD || eventType == COMPUTE_BWD);
            updateLastProcessedEvent(mb, wasCompute);
            
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
        cout << "[PENDING] Added event: EP=" << ep << ", Type=" << type << ", MB=" << mb 
             << ", State=" << simulator->stateToString(state) << ", RemainingTime=" << remainingTime << endl;
    }
    
    return processed;
}

void RankTask::progress(double time){
    cout << "[PROGRESS] Rank " << rank->id << " starting progress, events size=" << events.size() 
         << ", rankGlobalTime=" << rankGlobalTime << ", time step=" << time << endl;
    
    // 智能时间步进：根据microbatch并行性决定时间更新策略
    double newRankGlobalTime = calculateNewRankGlobalTime(time);
    cout << "[PROGRESS] Rank " << rank->id << " time update: " << rankGlobalTime << " -> " << newRankGlobalTime << endl;
    rankGlobalTime = newRankGlobalTime;
    
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
        
        // 更新上一个处理的事件信息
        bool wasCompute = (eventType == COMPUTE_FWD || eventType == COMPUTE_BWD);
        updateLastProcessedEvent(mb, wasCompute);
        
        // 根据事件状态进行处理
        switch (eventState) {
        case COMPUTE:            
                // 处理计算状态
                eventRemainingTime -= time;
                if (eventRemainingTime <= 1e-6) {
                    // 计算完成
                    cout << "[PROGRESS] Rank " << rank->id << " completed computation for mb=" << mb 
                         << " at rankGlobalTime=" << rankGlobalTime << endl;
                    
                    // 更新microbatch的计算时间
                    double computeEndTime = rankGlobalTime;
                    MicrobatchManager::updateMicrobatchComputeTime(mb, computeEndTime);
                    cout << "[PROGRESS] Updated microbatch " << mb << " compute time to " << computeEndTime << endl;
                    
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
                // 处理PP等待状态
                if (eventType == PP_COMM_FWD && ep == EndpointType::RECV) {
                    // 接收到PP通信，开始计算
                    eventState = COMPUTE;
                    eventRemainingTime = workload->fwdCompTime;
                    cout << "[PROGRESS] Rank " << rank->id << " starting COMPUTE_FWD for mb=" << mb 
                         << " at rankGlobalTime=" << rankGlobalTime << endl;
                }
                break;
                
            case TP_COMM:
                // 处理TP通信状态
                if (eventType == TP_COMM_FWD && ep == EndpointType::RECV) {
                    // TP通信完成
                    eventState = (isLastRankInPipeline()) ? COMPUTE : PP_WAIT;
                    eventRemainingTime = 0;
                }
                break;
                
            default:
                break;
        }
    }
    
    // 处理PP发送事件，通过GroupTask管理通信
    for (auto it = events.begin(); it != events.end(); ) {
        int ep = get<0>(*it);
        int eventType = get<1>(*it);
        int mb = get<2>(*it);
        RankState eventState = get<3>(*it);
        
        if (eventType == PP_COMM_FWD && ep == EndpointType::SENT) {
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

double RankTask::calculateNewRankGlobalTime(double time) {
    cout << "[TIME-CALC] Rank " << rank->id << " calculating new global time, current=" << rankGlobalTime 
         << ", time step=" << time << ", lastProcessedMb=" << lastProcessedMb 
         << ", lastProcessedWasCompute=" << (lastProcessedWasCompute ? "true" : "false") << endl;
    
    // 找到当前要处理的主要事件（通常是计算事件）
    int currentMb = 0;
    bool currentIsCompute = false;
    double currentComputeEndTime = 0;
    
    for (const auto& event : events) {
        int mb = get<2>(event);
        int eventType = get<1>(event);
        RankState eventState = get<3>(event);
        double eventRemainingTime = get<4>(event);
        
        if ((eventType == COMPUTE_FWD || eventType == COMPUTE_BWD) && eventRemainingTime > 1e-6) {
            // 找到正在进行的计算事件
            currentMb = mb;
            currentIsCompute = true;
            currentComputeEndTime = MicrobatchManager::getMicrobatchComputeTime(mb) + eventRemainingTime;
            break;
        }
    }
    
    if (currentIsCompute) {
        cout << "[TIME-CALC] Found current compute event: mb=" << currentMb 
             << ", compute end time=" << currentComputeEndTime << endl;
        
        if (abs(lastProcessedMb) == abs(currentMb) && lastProcessedWasCompute) {
            // 同一个microbatch的连续计算事件，串行执行
            cout << "[TIME-CALC] Same microbatch continuous compute (abs: " << abs(currentMb) << "), serial execution" << endl;
            return rankGlobalTime + time;
        } else if (abs(lastProcessedMb) != abs(currentMb) && !lastProcessedWasCompute) {
            // 不同microbatch，且上一个不是计算事件，可以并行
            // 取该microbatch最后一个计算事件的结束时间作为起始时间
            double mbLastComputeTime = MicrobatchManager::getMicrobatchComputeTime(currentMb);
            double newTime = std::max(rankGlobalTime, mbLastComputeTime) + time;
            cout << "[TIME-CALC] Different microbatch (abs: " << abs(currentMb) << " vs " << abs(lastProcessedMb) 
                 << "), parallel execution, new time=" << newTime << endl;
            return newTime;
                        } else {
            // 其他情况，使用默认的串行逻辑
            cout << "[TIME-CALC] Default serial execution" << endl;
            return rankGlobalTime + time;
                        }
                    } else {
        // 没有计算事件，只有通信事件，串行执行
        cout << "[TIME-CALC] No compute event, serial execution" << endl;
        return rankGlobalTime + time;
    }
}

// MicrobatchManager的静态方法实现
void MicrobatchManager::updateMicrobatchGlobalTime(int mb, double time) {
    // 确保microbatch状态存在
    if (microbatchStates.find(mb) == microbatchStates.end()) {
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
    }
    
    // 取较大值，确保microbatch时间只能向前推进
    double currentTime = microbatchStates[mb].globalTime;
    double newTime = std::max(currentTime, time);
    
    if (newTime > currentTime) {
        microbatchStates[mb].globalTime = newTime;
        cout << "[MICROBATCH-TIME] Global updated microbatch " << mb 
             << " global time from " << currentTime << " to " << newTime << endl;
    } else {
        cout << "[MICROBATCH-TIME] Global microbatch " << mb 
             << " time unchanged: " << currentTime << " (requested: " << time << ")" << endl;
    }
}

double MicrobatchManager::getMicrobatchGlobalTime(int mb) {
    auto it = microbatchStates.find(mb);
    if (it != microbatchStates.end()) {
        return it->second.globalTime;
    }
    
    // 对于反向microbatch（负数），尝试获取其对应正向microbatch的时间
    if (mb < 0) {
        int positiveMb = -mb;
        auto positiveIt = microbatchStates.find(positiveMb);
        if (positiveIt != microbatchStates.end()) {
            cout << "[MICROBATCH-TIME] Global getMicrobatchGlobalTime for backward mb=" << mb 
                 << ", using forward mb=" << positiveMb << " time: " << positiveIt->second.globalTime << endl;
            return positiveIt->second.globalTime;
        }
    }
    
    // 如果没有找到，返回0作为默认值
    cout << "[MICROBATCH-TIME] Global getMicrobatchGlobalTime failed, mb: " << mb << ", returning 0" << endl;
    return 0.0;
}

void MicrobatchManager::updateMicrobatchComputeTime(int mb, double computeEndTime) {
    if (microbatchStates.find(mb) == microbatchStates.end()) {
        microbatchStates[mb] = MicrobatchState(mb);
    }
    
    double currentComputeTime = microbatchStates[mb].lastComputeEndTime;
    double newComputeTime = std::max(currentComputeTime, computeEndTime);
    
    if (newComputeTime > currentComputeTime) {
        microbatchStates[mb].lastComputeEndTime = newComputeTime;
        microbatchStates[mb].hasComputeEvent = true;
        cout << "[MICROBATCH-TIME] Global updated microbatch " << mb 
             << " compute time from " << currentComputeTime << " to " << newComputeTime << endl;
    }
}

double MicrobatchManager::getMicrobatchComputeTime(int mb) {
    auto it = microbatchStates.find(mb);
    if (it != microbatchStates.end()) {
        return it->second.lastComputeEndTime;
    }
    
    // 对于反向microbatch（负数），尝试获取其对应正向microbatch的计算时间
    if (mb < 0) {
        int positiveMb = -mb;
        auto positiveIt = microbatchStates.find(positiveMb);
        if (positiveIt != microbatchStates.end()) {
            cout << "[MICROBATCH-TIME] Global getMicrobatchComputeTime for backward mb=" << mb 
                 << ", using forward mb=" << positiveMb << " time: " << positiveIt->second.lastComputeEndTime << endl;
            return positiveIt->second.lastComputeEndTime;
        }
    }
    
    cout << "[MICROBATCH-TIME] Global getMicrobatchComputeTime failed, mb: " << mb << ", returning 0" << endl;
    return 0.0;
}

void MicrobatchManager::printStates() {
    cout << "[MICROBATCH-TIME] Global Microbatch States:" << endl;
    for (const auto& pair : microbatchStates) {
        cout << "  MB=" << pair.first << ", GlobalTime=" << pair.second.globalTime 
             << ", ComputeTime=" << pair.second.lastComputeEndTime 
             << ", HasCompute=" << (pair.second.hasComputeEvent ? "true" : "false") << endl;
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

        // 记录到timelineEvents（发送方提交事件）
        // simulator->recordTimelineEvent(
        //     from,                // rank
        //     group->type,         // GroupType
        //     EndpointType::SENT,  // 标记为发送端事件
        //     evtType,             // EventType
        //     mb,                  // microbatch
        //     simulator->globalTime, // startTime
        //     -1                   // endTime（未完成）
        // );

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
    // 更新group的全局时间（基于发送和接收方rank的时间）
    // 注意：这里暂时传入0，因为此时可能还没有activeCollective
    updateGroupGlobalTime(0);
    
    // move waiting to active
    if(activeCollective == nullptr) {
        if(!waitingCollectives.empty()) {
            activeCollective = waitingCollectives.front();
            waitingCollectives.erase(waitingCollectives.begin());
        }
    }
    if(activeCollective == nullptr) 
        return ;

    activeCollective->progress(time);

    if(activeCollective && activeCollective->flows[0]->remainingSize <= 1e-6 ) {   // EP TYPE MB
        
        EventType commType;
        int mb = activeCollective->microbatch;
        
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
                    if (group->type == GroupType::PP || group->type == GroupType::TP) {
                        // 记录发送方的事件
                        for (auto sender : senders) {
                            simulator->recordTimelineEvent(
                                sender->rank->id, group->type, EndpointType::SENT,
                                commType, mb, startTime, endTime,
                                (group->type == GroupType::PP ? "PP_COMM sent" : "TP_COMM sent")
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
                                      (timelineEvent.eventType == TP_COMM_FWD || timelineEvent.eventType == TP_COMM_BWD)))) {
                                    timelineEvent.endTime = endTime;
                                    cout << "[TIMELINE] Updated RECV end time for rank " << receiver->rank->id 
                                         << " mb=" << mb << " to " << endTime << endl;
                                    found = true;
                                    break;
                                }
                            }
                            
                            // 如果没有找到已存在的RECV事件，创建新的
                            if (!found) {
                                simulator->recordTimelineEvent(
                                    receiver->rank->id, group->type, EndpointType::RECV,
                                    commType, mb, startTime, endTime,
                                    (group->type == GroupType::PP ? "PP_COMM received" : "TP_COMM received")
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
                rankTask->addEvent(EndpointType::RECV, commType, mb, PP_WAIT, 0);
            }           
        }

        delete activeCollective;  
        activeCollective = nullptr;
        // 优先选择与当前流水线阶段匹配的 microbatch
        if (!waitingCollectives.empty()) {
            int currentDir = mb > 0 ? 1 : -1;
            auto it = find_if(waitingCollectives.begin(), waitingCollectives.end(),
                [currentDir](Collective* c) { 
                    return (c->microbatch > 0) == (currentDir > 0); 
                });
            
            activeCollective = (it != waitingCollectives.end()) ? *it : waitingCollectives.front();
            waitingCollectives.erase(it);
        }
    }

}

void GroupTask::updateGroupGlobalTime(int mb) {
    // 获取microbatch的全局时间作为基础
    double mbGlobalTime = MicrobatchManager::getMicrobatchGlobalTime(mb);
    
    // 找到发送方和接收方中的最大时间
    double maxSenderTime = 0.0;
    double maxReceiverTime = 0.0;
    
    // 找到发送方中的最大时间
    for (auto sender : senders) {
        if (sender->rankGlobalTime > maxSenderTime) {
            maxSenderTime = sender->rankGlobalTime;
        }
    }
    
    // 找到接收方中的最大时间
    for (auto receiver : receivers) {
        if (receiver->rankGlobalTime > maxReceiverTime) {
            maxReceiverTime = receiver->rankGlobalTime;
        }
    }
    
    // 取microbatch全局时间、发送方最大时间、接收方最大时间中的最大值
    double newGroupTime = std::max({mbGlobalTime, maxSenderTime, maxReceiverTime});
    
    // 更新group的全局时间
    groupGlobalTime = newGroupTime;
    
    // 更新microbatch的全局时间
    if (mb != 0) {
        MicrobatchManager::updateMicrobatchGlobalTime(mb, newGroupTime);
    }
    
    // 更新发送方和接收方的rank时间（如果它们的时间落后于group时间）
    for (auto sender : senders) {
        if (sender->rankGlobalTime < newGroupTime) {
            sender->rankGlobalTime = newGroupTime;
        }
    }
    
    for (auto receiver : receivers) {
        if (receiver->rankGlobalTime < newGroupTime) {
            receiver->rankGlobalTime = newGroupTime;
        }
    }
    
    cout << "[GROUP-TIME] Group " << group->id << " (" << simulator->groupTypeToString(group->type) 
         << ") updated global time to " << groupGlobalTime 
         << " (mbGlobalTime=" << mbGlobalTime 
         << ", maxSender=" << maxSenderTime 
         << ", maxReceiver=" << maxReceiverTime 
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
    // Clear existing data
    tasks.clear();
    timelineEvents.clear();
    commEvents.clear();
    commStats = CommStats();

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
        GroupTask* tpGroupTask = rank->tpGroup->groupTask;
        GroupTask* dpGroupTask = rank->dpGroup->groupTask;

        // 设置rank与TP组的双向连接
        rankTask->tpGroupTask = tpGroupTask;
        tpGroupTask->senders.push_back(rankTask);    // rank可发送TP通信
        tpGroupTask->receivers.push_back(rankTask);  // rank可接收TP通信

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
    for (auto rank : workload->ranks) {
        RankTask* task = rank->rankTask;
        if (rank->pp == 0) { // 首阶段
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
            task->rankGlobalTime += workload->fwdCompTime;
            
            
            // 为所有microbatch预调度计算事件
            for (int mb = 2; mb <= workload->microbatches; ++mb) {
                task->addEvent(EndpointType::NONE_ENDPOINT, EventType::COMPUTE_FWD, mb, COMPUTE, workload->fwdCompTime);
            }
            
            // 预调度第一个PP发送事件（在计算完成后触发）
            task->addEvent(EndpointType::SENT, EventType::PP_COMM_FWD, 1, PP_WAIT, 0);
            
            // 为下一个rank预调度对应的RECV事件，通过GroupTask管理
            if (rank->ppFwdGroup != nullptr && rank->ppFwdGroup->groupTask != nullptr) {
                rank->ppFwdGroup->groupTask->addEvent(rank->id, 1);
                cout << "[INIT] Pre-scheduled PP_FWD event to GroupTask " << rank->ppFwdGroup->id 
                     << " for mb=1 (from rank " << rank->id << ")" << endl;
            }
        }
        else if (rank->pp > 1) { // 其他中间阶段
            // 保持等待状态即可，事件将由前一阶段触发
            task->state = PP_WAIT;
            task->microbatch = 0; // 初始化为0，等待事件触发
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
        // 计算当前所有rank和group的平均时间作为全局时间显示
        double avgGlobalTime = 0;
        int timeCount = 0;
        for (auto task : tasks) {
            if (dynamic_cast<RankTask*>(task) != nullptr) {
                RankTask* rankTask = dynamic_cast<RankTask*>(task);
                avgGlobalTime += rankTask->rankGlobalTime;
                timeCount++;
            } else if (dynamic_cast<GroupTask*>(task) != nullptr) {
                GroupTask* groupTask = dynamic_cast<GroupTask*>(task);
                avgGlobalTime += groupTask->groupGlobalTime;
                timeCount++;
            }
        }
        if (timeCount > 0) avgGlobalTime /= timeCount;
        
        cout << "\n=== Round " << round << " at avg time " << avgGlobalTime << " ===" << endl;
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

        cout << "[TIME-PRE] AvgGlobalTime=" << avgGlobalTime 
            << " | MinStableTime=" << minStableTime << endl;

        // 5. 推进时间并更新所有任务状态（每个任务独立更新时间）
        for (auto task : tasks) {
            task->progress(minStableTime);
        }

        cout << "[TIME-POST] AvgGlobalTime updated" << endl;

        if(round==targetRound )printStates(); // !!!!!!!!!!!!!!!
        round++;
    }

    // 6. 确保所有未完成的事件标记为完成（防止遗漏）
    // 计算最终的平均时间
    double finalAvgTime = 0;
    int finalTimeCount = 0;
    for (auto task : tasks) {
        if (dynamic_cast<RankTask*>(task) != nullptr) {
            RankTask* rankTask = dynamic_cast<RankTask*>(task);
            finalAvgTime += rankTask->rankGlobalTime;
            finalTimeCount++;
        } else if (dynamic_cast<GroupTask*>(task) != nullptr) {
            GroupTask* groupTask = dynamic_cast<GroupTask*>(task);
            finalAvgTime += groupTask->groupGlobalTime;
            finalTimeCount++;
        }
    }
    if (finalTimeCount > 0) finalAvgTime /= finalTimeCount;
    
    for (auto& evt : commEvents) {
        if (evt.endTime < 0) {
            evt.endTime = finalAvgTime;  // 标记为最终平均时间完成
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
    for (auto& e : timelineEvents) {
        cout << "    [" << e.rank << ", \"" << simulator->eventTypeToString(e.eventType) << "\", " 
                << e.microbatch << ", " << e.startTime << ", " << e.endTime << "]";
                
        // 如果不是最后一个元素，添加逗号
        if (&e != &timelineEvents.back()) {
            cout << ",";
        }
        cout << "\n";
    }

    cout << "Simulation finished" << endl;
    cout << "Final Average Time: " << finalAvgTime << endl;
    // cout << "TP Pure Communication Time: " << pureTpCommTime << endl;
    // cout << "TP Forward Pure Communication Time: " << pureTpFwCommTime << endl;
    // cout << "TP Backward Pure Communication Time: " << pureTpBwCommTime << endl;
    // cout << "PP Pure Communication Time: " << purePpCommTime << endl;
    // cout << "PP Forward Pure Communication Time: " << purePpFwCommTime << endl;
    // cout << "PP Backward Pure Communication Time: " << purePpBwCommTime << endl;
    // cout << "DP Pure Communication Time: " << pureDpCommTime << endl;
    // cout << "Total Pure Communication Time: " << pureTotalCommTime << endl;
    cout << "TP Forward Time: " << commStats.tpForward << endl;
    cout << "TP Backward Time: " << commStats.tpBackward << endl;
    cout << "PP Forward Time: " << commStats.ppForward << endl;
    cout << "PP Backward Time: " << commStats.ppBackward << endl;
    cout << "DP Total Time: " << commStats.dpTotal << endl;
    cout << "---------------------------" << endl;

    SimResult result;
    result.globalTime = finalAvgTime;
    result.pureTpCommTime = pureTpCommTime;
    result.pureTpFwCommTime = pureTpFwCommTime;
    result.pureTpBwCommTime = pureTpBwCommTime;
    result.purePpCommTime = purePpCommTime;
    result.purePpFwCommTime = purePpFwCommTime;
    result.purePpBwCommTime = purePpBwCommTime;
    result.pureDpCommTime = pureDpCommTime;
    result.pureTotalCommTime = pureTotalCommTime;
    return result;
}

double RankTask::calculateHandleEventsTime(int mb, double computeTime) {
    cout << "[HANDLE-TIME-CALC] Rank " << rank->id << " calculating handleEvents time for mb=" << mb 
         << ", computeTime=" << computeTime << ", lastProcessedMb=" << lastProcessedMb 
         << ", lastProcessedWasCompute=" << (lastProcessedWasCompute ? "true" : "false") << endl;
    
    bool currentIsCompute = true; // handleEvents中的COMPUTE事件都是计算事件
    
    if (abs(lastProcessedMb) == abs(mb) && lastProcessedWasCompute) {
        // 同一个microbatch的连续计算事件，串行执行
        cout << "[HANDLE-TIME-CALC] Same microbatch continuous compute (abs: " << abs(mb) << "), serial execution" << endl;
        return rankGlobalTime + computeTime;
    } else if (abs(lastProcessedMb) != abs(mb) && !lastProcessedWasCompute) {
        // 不同microbatch，且上一个不是计算事件，可以并行
        // 取该microbatch最后一个计算事件的结束时间作为起始时间
        double mbLastComputeTime = MicrobatchManager::getMicrobatchComputeTime(mb);
        double newTime = std::max(rankGlobalTime, mbLastComputeTime) + computeTime;
        cout << "[HANDLE-TIME-CALC] Different microbatch (abs: " << abs(mb) << " vs " << abs(lastProcessedMb) 
             << "), parallel execution, new time=" << newTime << endl;
        return newTime;
    } else {
        // 其他情况，使用默认的串行逻辑
        cout << "[HANDLE-TIME-CALC] Default serial execution" << endl;
        return rankGlobalTime + computeTime;
    }
}

void RankTask::updateLastProcessedEvent(int mb, bool wasCompute) {
    if (wasCompute != lastProcessedWasCompute || abs(mb) != abs(lastProcessedMb)) {
        cout << "[LAST-PROCESSED] Rank " << rank->id << " updated: lastProcessedMb=" << lastProcessedMb 
             << " -> " << mb << " (abs: " << abs(mb) << "), lastProcessedWasCompute=" 
             << (lastProcessedWasCompute ? "true" : "false") 
             << " -> " << (wasCompute ? "true" : "false") << endl;
        lastProcessedMb = mb;
        lastProcessedWasCompute = wasCompute;
    }
}