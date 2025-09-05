#include "common.h"
#include "simulator.h"

extern Workload* workload;
extern Simulator* simulator;

void RankTask::addEvent(int ep, int eventType, int mb, RankState state, double remainingTime, double startTime, double endTime) {
    events.emplace_back(ep, eventType, mb, state, remainingTime, startTime, endTime);
    cout << "[RankTask Event Added] Rank=" << rank->id 
            << " | EP=" << simulator->endpointToString(static_cast<EndpointType>(ep))
            << " | EventType=" << simulator->eventTypeToString(static_cast<EventType>(eventType))
            << " | MB=" << mb 
            << " | State=" << simulator->stateToString(state)
            << " | RemainingTime=" << remainingTime
            << " | StartTime=" << startTime
            << " | EndTime=" << endTime
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
        event_str += to_string(get<2>(event)) + ", " + simulator->stateToString(get<3>(event)) + ", " + to_string(get<4>(event)) + ", " + to_string(get<5>(event)) + ", " + to_string(get<6>(event)) + ">";
        cout << event_str << " ";
    }
    cout << endl;
}
