#include "common.h"
#include "simulator.h"

extern Workload* workload;
extern Simulator* simulator;

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
