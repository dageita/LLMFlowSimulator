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

void RankTask::addEvent(int ep, int type, int mb) {
    events.emplace_back(ep, type, mb);

    cout << "[RankTask Event Added] Rank=" << rank->id 
            << " | EP=" << simulator->endpointToString(static_cast<EndpointType>(ep))
            << " | GroupType=" << simulator->groupTypeToString(static_cast<GroupType>(type))
            << " | MB=" << mb 
            << " | Total Events=" << events.size() << endl;
}

RankTask::RankTask(Rank* rank) : rank(rank) {
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
        }
        switch(get<1>(event)) {
            case GroupType::TP:
                event_str += "TP, ";
                break;
            case GroupType::PP:
                event_str += "PP, ";
                break;
            case GroupType::DP:
                event_str += "DP, ";
                break;
        }
        event_str += to_string(get<2>(event)) + ">";
        cout << event_str << " ";
    }
    cout << endl;
}

int RankTask::handleEvents(){   // < EP, TYPE, MB >
    int countEvents = events.size();
    cout << "handleEvents for RankTask: " << rank->id << ", events size: " << countEvents << endl;
    for(auto it = events.begin(); it != events.end(); ) {
        int ep = get<0>(*it); // 端点
        int type = get<1>(*it);  // 类型
        int mb = get<2>(*it); // 第一个microbatch

        LOG_EVENT(rank->id, "Event EP="+to_string(ep)+" Type="+to_string(type), mb);

        if( type == GroupType::DP ) {
            if(ep == EndpointType::SENT) {
                it = events.erase(it); continue;
            }
            // RECV
            // transit to complete
            if(state == RankState::DP_COMM){
                state = RankState::DONE;
                it = events.erase(it); continue;
            }
            else{
                it++; continue;
            }            
        }
        else if (type == GroupType::PP) {
            int mb = get<2>(*it); // 重新获取 microbatch（确保最新值）

            // 统一处理 PP 事件
            if (ep == EndpointType::SENT) {
                // 丢弃所有 SENT 事件（PP的SENT仅用于通知，无状态影响）
                it = events.erase(it);
                continue;
            } 
            else if (ep == EndpointType::RECV) {
                // 检查是否允许处理（正向或反向RECV）
                bool isPPReversed = (mb == -microbatch);
                if (!isPPReversed && mb != microbatch && mb != 0) {
                    ++it; // 不匹配当前 microbatch，跳过
                    continue;
                }

                // 处理正向 PP（FWD）
                if (mb > 0 && state == RankState::PP_WAIT && mb == microbatch) {
                    LOG_STATE_TRANSITION(rank->id, "PP_WAIT", "COMPUTE(FWD)", mb);
                    state = RankState::COMPUTE;
                    remainingTime = workload->fwdCompTime;

                    simulator->recordTimelineEvent(
                        rank->id,
                        GroupType::PP,
                        EndpointType::RECV,
                        EventType::COMPUTE_FWD,
                        mb,
                        simulator->globalTime,
                        simulator->globalTime + remainingTime
                    );

                    // // 预置反向事件
                    // if (ppBwdGroupTask) {
                    //     ppBwdGroupTask->addEvent(rank->id, -mb);
                    // }
                }
                // 处理反向 PP（BWD）
                else if (mb < 0) {
                    LOG_STATE_TRANSITION(rank->id, simulator->stateToString(state), "COMPUTE(BWD)", mb);
                    state = RankState::COMPUTE;
                    remainingTime = workload->bwdCompTime;
                    microbatch = mb; // 更新为负数

                    simulator->recordTimelineEvent(
                        rank->id,
                        GroupType::PP,
                        EndpointType::RECV,
                        EventType::COMPUTE_BWD,
                        mb,
                        simulator->globalTime,
                        simulator->globalTime + remainingTime
                    );
                }
                else {
                    // 不匹配的 microbatch，丢弃事件
                    it = events.erase(it);
                    continue;
                }

                // 处理完成后移除事件
                it = events.erase(it);
                continue;
            }

            // 其他情况（理论上不会走到这里）
            ++it;
            continue;
        }
    }
    return countEvents - events.size();
}


double RankTask::stableTime(){
    switch(state) {
        case COMPUTE:
            return remainingTime;
        default:
            return numeric_limits<double>::infinity();
    }
}

void RankTask::progress(double time){
    double start, end;
    EventType type;

    switch(state) {
        case COMPUTE:            
            remainingTime -= time;
            if (remainingTime <= 1e-6) {
                if (microbatch < 0) { 
                    //手动更新globalTime步进时间
                    double computeEndTime = simulator->globalTime + workload->bwdCompTime;
                    simulator->globalTime = computeEndTime;
                    if (!isFirstRankInPipeline()) {
                        if (ppBwdGroupTask) {
                            auto ev = make_tuple(rank->id, microbatch);
                            if (find(ppBwdGroupTask->events.begin(), ppBwdGroupTask->events.end(), ev)
                                == ppBwdGroupTask->events.end()) {
                                ppBwdGroupTask->addEvent(rank->id, microbatch);
                            }
                        }
                        LOG_STATE_TRANSITION(rank->id, "COMPUTE(BWD)", "PP_WAIT", microbatch);
                        state = RankState::PP_WAIT;
                    } else {
                        if (dpGroupTask && dpGroupTask->group->ranks.size() > 1) {
                            auto dp_ev = make_tuple(rank->id, 0);
                            if (find(dpGroupTask->events.begin(), dpGroupTask->events.end(), dp_ev)
                                == dpGroupTask->events.end()) {
                                dpGroupTask->addEvent(rank->id, 0);
                            }
                        }
                        // 状态转换：COMP_BWD → DP_WAIT（等待梯度同步完成）
                        LOG_STATE_TRANSITION(rank->id, "COMPUTE(BWD)", "DP_WAIT", microbatch);
                        state = RankState::DP_WAIT;
                    }
                } else {
                    if (isLastRankInPipeline()) {
                        // 最后一个 Rank：直接触发 COMP_BWD
                        LOG_STATE_TRANSITION(rank->id, "COMPUTE(FWD)", "COMPUTE(BWD)", microbatch);
                        state = RankState::COMPUTE;
                        microbatch = -microbatch;
                        
                        simulator->recordTimelineEvent(
                            rank->id,
                            GroupType::PP,
                            EndpointType::RECV,
                            EventType::COMPUTE_BWD,
                            microbatch,
                            simulator->globalTime,
                            simulator->globalTime + workload->bwdCompTime
                        );

                        // // 预置 PP_BWD 给前一个 Rank
                        // if (ppBwdGroupTask) {
                        //     ppBwdGroupTask->addEvent(rank->id, microbatch);  // mb 已经是负数
                        // }
                    } else {
                        LOG_STATE_TRANSITION(rank->id, "COMPUTE(FWD)", 
                            (tpGroupTask->group->ranks.size() > 1) ? "TP_COMM" : "PP_WAIT", 
                            microbatch);
                        
                        // TP通信处理（如果需要）
                        if (tpGroupTask->group->ranks.size() > 1) {
                            state = RankState::TP_COMM;
                            tpGroupTask->addEvent(rank->id, microbatch);
                        }
                        
                        // 无论TP度如何，只要需要PP前向就触发
                        if (ppFwdGroupTask) {
                            auto ev = make_tuple(rank->id, microbatch);
                            if (find(ppFwdGroupTask->events.begin(), ppFwdGroupTask->events.end(), ev)
                                == ppFwdGroupTask->events.end()) {
                                ppFwdGroupTask->addEvent(rank->id, microbatch);
                            }
                        }
                        
                        // 如果没有TP通信，直接进入PP_WAIT
                        if (tpGroupTask->group->ranks.size() <= 1) {
                            state = RankState::PP_WAIT;
                        }
                    }
                }
            }
            break;
        default:
            break;
    }
}


GroupTask::GroupTask(Group* group) : group(group) {
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
        simulator->recordTimelineEvent(
            from,                // rank
            group->type,         // GroupType
            EndpointType::SENT,  // 标记为发送端事件
            evtType,             // EventType
            mb,                  // microbatch
            simulator->globalTime, // startTime
            -1                   // endTime（未完成）
        );

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
                evt.startTime = simulator->globalTime; // 更新为实际开始时间
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


void GroupTask::progress(double time){
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
        double startTime = simulator->globalTime - time;  // 事件实际开始时间
        double endTime = simulator->globalTime;

        if (group->type == GroupType::TP) {
            commType = (activeCollective->microbatch > 0) ? TP_COMM_FWD : TP_COMM_BWD;
        } 
        else if (group->type == GroupType::PP) {
            commType = (activeCollective->microbatch > 0) ? PP_COMM_FWD : PP_COMM_BWD;
        }
        else if (group->type == GroupType::DP) {
            commType = DP_COMM_EVENT;
        }

        cout << "[GROUP-PROGRESS] Group=" << group->id 
            << " | Type=" << simulator->groupTypeToString(group->type)
            << " | MB=" << activeCollective->microbatch
            << " | CommType=" << simulator->eventTypeToString(commType)
            << " | StartTime=" << startTime
            << " | EndTime=" << endTime
            << " | Duration=" << (endTime - startTime) << endl;

        // 检查是否已经处理过这个microbatch的通信
        bool alreadyProcessed = false;
        for(auto& evt : simulator->commEvents) {
            if(evt.type == group->type && evt.microbatch == activeCollective->microbatch && 
               evt.endTime > 0) {
                alreadyProcessed = true;
                cout << "[GROUP-DUPLICATE] Skip duplicate event for MB=" << activeCollective->microbatch << endl;
                break;
            }
        }

        if(!alreadyProcessed) {
            //记录事件
            // EventType commType;
            // double startTime = simulator->globalTime - time;  // 事件实际开始时间
            // double endTime = simulator->globalTime;

            // if (group->type == GroupType::TP) {
            //     commType = (activeCollective->microbatch > 0) ? TP_COMM_FWD : TP_COMM_BWD;
            // } 
            // else if (group->type == GroupType::PP) {
            //     commType = (activeCollective->microbatch > 0) ? PP_COMM_FWD : PP_COMM_BWD;
            // }
            // else if (group->type == GroupType::DP) {
            //     commType = DP_COMM_EVENT;
            // }

            // // 记录发送方事件
            // for (auto rankTask : senders) {
            //     string senderInfo = "SENDER [" + to_string(rankTask->rank->id) + "]";
            //     simulator->recordTimelineEvent(
            //         rankTask->rank->id,
            //         group->type,          // GroupType
            //         EndpointType::SENT,   // EndpointType
            //         commType,        // EventType
            //         activeCollective->microbatch,
            //         startTime,
            //         endTime,
            //         senderInfo             // 可选附加信息
            //     );
            // }

            // // 记录接收方事件
            // for (auto rankTask : receivers) {
            //     string receiverInfo = "RECEIVER [" + to_string(rankTask->rank->id) + "]";
            //     simulator->recordTimelineEvent(
            //         rankTask->rank->id,
            //         group->type,          // GroupType 
            //         EndpointType::RECV,   // EndpointType
            //         commType,        // EventType
            //         activeCollective->microbatch,
            //         startTime,
            //         endTime,
            //         receiverInfo           // 可选附加信息
            //     );
            // }

            // 记录通信完成时间
            for (auto& evt : simulator->commEvents) {
                if (evt.endTime < 0 && evt.type == group->type && 
                    evt.microbatch == activeCollective->microbatch) {
                    evt.endTime = simulator->globalTime;

                    // 调试日志：打印每个通信事件的详细信息
                    cout << "[STAT] Type=" << simulator->groupTypeToString(group->type) 
                        << " MB=" << activeCollective->microbatch
                        << " Duration=" << (evt.endTime - evt.startTime)
                        << " From=" << evt.startTime 
                        << " To=" << evt.endTime << endl;
                    
                    // 更新统计（以rank 0为基准）
                    // if (senders[0]->rank->id == 0 || receivers[0]->rank->id == 0) {
                        double duration = evt.endTime - evt.startTime;
                        if (group->type == GroupType::TP) {
                            if (activeCollective->microbatch > 0) 
                                simulator->commStats.tpForward += duration;
                            else 
                                simulator->commStats.tpBackward += duration;
                        } 
                        else if (group->type == GroupType::PP) {
                            if (activeCollective->microbatch > 0) 
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

        // notify senders
        for(auto rankTask : senders){
            rankTask->addEvent(EndpointType::SENT, group->type, activeCollective->microbatch);
        }

        // notify receivers
        // 为了在timeline上展示，故只记录RECV方的PP
        for(auto rankTask: receivers){
            string receiverInfo = "RECEIVER [" + to_string(rankTask->rank->id) + "]";
            simulator->recordTimelineEvent(
                rankTask->rank->id,
                group->type,          // GroupType 
                EndpointType::RECV,   // EndpointType
                commType,        // EventType
                activeCollective->microbatch,
                startTime,
                endTime,
                receiverInfo           // 可选附加信息
            );
            
            rankTask->addEvent(EndpointType::RECV, group->type, activeCollective->microbatch);
        }

        delete activeCollective;  
        activeCollective = nullptr;
        if(!waitingCollectives.empty()) {
            activeCollective = waitingCollectives.front();
            waitingCollectives.erase(waitingCollectives.begin());
        }
    }

}


void Simulator::initialize() {
    // Clear existing data
    tasks.clear();
    timelineEvents.clear();
    commEvents.clear();
    globalTime = 0;
    commStats = CommStats();

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
            recordTimelineEvent(
                rank->id,
                NONE_GROUP,       // GroupType
                NONE_ENDPOINT,    // EndpointType
                COMPUTE_FWD,      // EventType
                1,                // microbatch
                0,                // startTime
                workload->fwdCompTime // endTime
            );
        }
        else if (rank->pp == workload->PP - 1) { // 末阶段
            task->state = RankState::PP_WAIT;
            // 预置最后一个microbatch的反向接收事件
            if (workload->microbatches > 1) {
                task->events.emplace_back(EndpointType::RECV, GroupType::PP, -workload->microbatches);
            }
        }
        else { // 中间阶段（PP>2时）
            task->state = RankState::PP_WAIT;
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

void Simulator::run(){
    globalTime=0;
    cout << "===========================" << endl;
    int round = 0;
    int targetRound = -1;    
    while(true){
        // cout << "===========================" << endl;
        // cout << "Global Time: " << globalTime << ", Round " << round << endl;
        // cout << "----------------------------" << endl;
        cout << " before handle events" << endl;
        if(round==targetRound) printStates(); // !!!!!!!!!!!!!!

        while(1){
            int countEvents = 0;
            for(auto task : tasks){
                countEvents += task->handleEvents();
            }
            if(countEvents == 0) break;
        }
        // cout << " after handle events, before update states" << endl;
        if(round==targetRound) printStates(); // !!!!!!!!!!!!!!
        // update states
        updateStates();
        cout << "----------------------------" << endl;
        // cout << " after update states " << endl;
        if(round==targetRound) printStates(); // !!!!!!!!!!!!!!!
        // stable time
        double time = numeric_limits<double>::infinity();
        for(auto task : tasks){
            double t = task->stableTime();
            if(t < time) time = t;
        }
        // cout << "----------------------------" << endl;
        // cout << "Stable time: " << time << endl;
        if(time == numeric_limits<double>::infinity()){
            break;
        }
        // progress
        for(auto task : tasks){
            task->progress(time);
        }
        globalTime += time;

        // cout << "Progressed time: " << time << endl;
        // cout << "---------------------------" << endl;
        if(round==targetRound )printStates(); // !!!!!!!!!!!!!!!
        // cout << "===========================" << endl;
        round++;
    }
    cout << "Simulation finished" << endl;
    cout << "Global Time: " << globalTime << endl;
    cout << "---------------------------" << endl;
}

SimResult Simulator::py_run(){
    cout << "===========================" << endl;

    globalTime = 0;
    commEvents.clear();  // 清空之前的事件记录
    commStats = CommStats();  // 重置统计

    int round = 0;
    int targetRound = -1;   // 用于周期性打印states 
    while(true){
        cout << "\n=== Round " << round << " at time " << globalTime << " ===" << endl;
        if(round==targetRound) printStates(); // !!!!!!!!!!!!!!
        // 1. 处理所有待处理的事件
        int eventsProcessed;
        do {
            eventsProcessed = 0;
            for (auto task : tasks) {
                eventsProcessed += task->handleEvents();
            }
        } while (eventsProcessed > 0);  // 直到没有新事件产生

        if(round==targetRound) printStates(); // !!!!!!!!!!!!!!
        // 2. 更新网络状态（计算各 Flow 的吞吐量）
        updateStates();

        // 3. 计算最短稳定时间（所有任务中最小的 stableTime）
        double minStableTime = numeric_limits<double>::infinity();
        for (auto task : tasks) {
            double taskStableTime = task->stableTime();
            if (taskStableTime < minStableTime) {
                minStableTime = taskStableTime;
            }
        }
        // 4. 如果所有任务都已完成，退出循环
        if (minStableTime == numeric_limits<double>::infinity()) {
            break;
        }

        cout << "[TIME-PRE] GlobalTime=" << globalTime 
            << " | MinStableTime=" << minStableTime << endl;

        // 5. 推进时间并更新所有任务状态
        globalTime += minStableTime;  // 更新全局时间
        for (auto task : tasks) {
            task->progress(minStableTime);
        }

        cout << "[TIME-POST] GlobalTime=" << globalTime << endl;

        if(round==targetRound )printStates(); // !!!!!!!!!!!!!!!
        round++;
    }

    // 6. 确保所有未完成的事件标记为完成（防止遗漏）
    for (auto& evt : commEvents) {
        if (evt.endTime < 0) {
            evt.endTime = globalTime;  // 标记为当前时间完成
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
    cout << "Global Time: " << globalTime << endl;
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
    result.globalTime = globalTime;
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