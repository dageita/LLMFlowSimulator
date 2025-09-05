#include "common.h"
#include "simulator.h"
#include <algorithm>

extern Workload* workload;
extern Simulator* simulator;

// GroupTask的progress方法实现

void GroupTask::progress(double time){
    cout << "[GROUP-PROGRESS-DEBUG] GroupTask " << group->id << " (" << simulator->groupTypeToString(group->type) 
         << ") progress called with time=" << time << endl;
    cout << "[GROUP-PROGRESS-DEBUG] Current state: activeCollective=" << (activeCollective ? "exists" : "null") 
         << ", waitingCollectives.size=" << waitingCollectives.size() 
         << ", groupGlobalTime=" << groupGlobalTime << endl;
    
    // 更新group的全局时间（基于发送和接收方rank的时间）
    // 注意：这里暂时传入0，因为此时可能还没有activeCollective
    
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
        
        cout << "[GROUP-PROGRESS-DEBUG] Before update GroupGlobalTime, groupGlobalTime=" << groupGlobalTime << endl;
        
        // 使用正确的microbatch和发送方rank更新group时间
        // 从senders中获取发送方rank ID
        int fromRank = -1;
        if (!senders.empty()) {
            fromRank = senders[0]->rank->id;
        }
        updateGroupGlobalTime(mb, fromRank);
        
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
                    
                    rankTask->addEvent(EndpointType::RECV, commType, mb, TP_COMM, 0, startTime, endTime);
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
                    
                    rankTask->addEvent(EndpointType::RECV, commType, mb, PP_WAIT, 0, startTime, endTime);
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

void GroupTask::updateGroupGlobalTime(int mb, int fromRank) {
    cout << "[GROUP-TIME-DEBUG] update GroupGlobalTime called for group " << group->id 
         << " with mb=" << mb << " from rank " << fromRank << endl;
    
    // 获取发送方rank的microbatch完成时间（而不是全局microbatch时间）
    double senderMbTime = MicrobatchManager::getRankMicrobatchTime(fromRank, mb);
    cout << "[GROUP-TIME-DEBUG] Retrieved sender microbatch time: Rank=" << fromRank 
         << ", MB=" << mb << ", Time=" << senderMbTime << endl;
    
    // GroupTask的通信任务基于发送方rank的microbatch完成时间
    double newGroupTime = senderMbTime;
    
    // 更新group的全局时间
    groupGlobalTime = newGroupTime;
    
    cout << "[GROUP-TIME] Group " << group->id << " (" << simulator->groupTypeToString(group->type) 
         << ") updated global time to " << groupGlobalTime 
         << " (senderMbTime=" << senderMbTime 
         << ", fromRank=" << fromRank << ", mb=" << mb << ")" << endl;
}
