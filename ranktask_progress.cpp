#include "common.h"
#include "simulator.h"

extern Workload* workload;
extern Simulator* simulator;

// RankTask的progress方法实现

void RankTask::progress(double time){
    cout << "[PROGRESS] Rank " << rank->id << " starting progress, events size=" << events.size() 
         << ", rankGlobalTime=" << rankGlobalTime << ", time step=" << time << endl;
    
    // 统一处理所有事件：状态更新和通信管理
    for (auto it = events.begin(); it != events.end(); ) {
        auto& event = *it;
        int ep = get<0>(event);
        int eventType = get<1>(event);
        int mb = get<2>(event);
        RankState& eventState = get<3>(event);
        double& eventRemainingTime = get<4>(event);
        double& eventStartTime = get<5>(event);
        double& eventEndTime = get<6>(event);
        
        cout << "[PROGRESS] Processing event: EP=" << simulator->endpointToString(static_cast<EndpointType>(ep)) 
            << ", Type=" << simulator->eventTypeToString(static_cast<EventType>(eventType)) 
            << ", MB=" << mb 
            << ", eventState=" << simulator->stateToString(eventState) 
            << ", RemainingTime=" << eventRemainingTime << endl;
        
        // 处理SENT类型的通信事件（通过GroupTask管理）
        if (ep == EndpointType::SENT) {
            bool shouldErase = false;
            
            if (eventType == TP_COMM_FWD) {
                cout << "[PROGRESS] Processing TP_FWD SENT event for mb=" << mb << endl;
                if (tpFwdGroupTask) {
                    tpFwdGroupTask->addEvent(rank->id, mb);
                    cout << "[PROGRESS] Added TP_FWD event to GroupTask " << tpFwdGroupTask->group->id << " for mb=" << mb << endl;
                }
                shouldErase = true;
            } else if (eventType == TP_COMM_BWD) {
                cout << "[PROGRESS] Processing TP_BWD SENT event for mb=" << mb << endl;
                if (tpBwdGroupTask) {
                    tpBwdGroupTask->addEvent(rank->id, mb);
                    cout << "[PROGRESS] Added TP_BWD event to GroupTask " << tpBwdGroupTask->group->id << " for mb=" << mb << endl;
                }
                shouldErase = true;
            } else if (eventType == PP_COMM_FWD) {
                cout << "[PROGRESS] Processing PP_FWD SENT event for mb=" << mb << endl;
                if (ppFwdGroupTask) {
                    ppFwdGroupTask->addEvent(rank->id, mb);
                    cout << "[PROGRESS] Added PP_FWD event to GroupTask " << ppFwdGroupTask->group->id << " for mb=" << mb << endl;
                }
                shouldErase = true;
            } else if (eventType == PP_COMM_BWD) {
                cout << "[PROGRESS] Processing PP_BWD SENT event for mb=" << mb << endl;
                if (ppBwdGroupTask) {
                    ppBwdGroupTask->addEvent(rank->id, mb);
                    cout << "[PROGRESS] Added PP_BWD event to GroupTask " << ppBwdGroupTask->group->id << " for mb=" << mb << endl;
                }
                shouldErase = true;
            } else if (eventType == DP_COMM_EVENT) {
                cout << "[PROGRESS] Processing DP_COMM SENT event for gradient sync" << endl;
                if (dpGroupTask) {
                                dpGroupTask->addEvent(rank->id, 0);
                    cout << "[PROGRESS] Added DP_COMM event to GroupTask " << dpGroupTask->group->id << " for gradient sync" << endl;
                }
                shouldErase = true;
            }
            
            if (shouldErase) {
                it = events.erase(it);
                continue;
            }
        }
        
        // 从timelineEvents中查找当前事件的startTime
        double timelineEventStartTime = rankGlobalTime; // 默认使用当前rankGlobalTime
        for (const auto& timelineEvent : simulator->timelineEvents) {
            if (timelineEvent.rank == rank->id && 
                timelineEvent.microbatch == mb && 
                timelineEvent.eventType == static_cast<EventType>(eventType)) {
                timelineEventStartTime = timelineEvent.startTime;
                break;
            }
        }
        
        // 智能时间步进：根据microbatch并行性决定时间更新策略
        double newRankGlobalTime = calculateNewRankGlobalTime(time, mb, timelineEventStartTime);
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
                            addEvent(EndpointType::SENT, PP_COMM_BWD, mb, PP_WAIT, 0, 0, 0);
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
                    cout << "[PROGRESS] Rank " << rank->id << " starting COMPUTE_FWD for mb=" << mb << " at rankGlobalTime=" << rankGlobalTime << endl;
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
                    
                    // 注意：PP通信事件的调度已经在handleEvents中完成，这里不需要重复添加
                    // 避免重复添加事件：handleEvents负责调度，progress负责状态更新
                    cout << "[PROGRESS] Rank " << rank->id << " TP communication state updated, PP events already scheduled in handleEvents" << endl;
                }
                break;
                    
            default:
                break;
        }
        
        ++it;
    }
    
    // 注意：SENT类型的事件处理已经合并到上面的统一循环中

        
  
    
    // 更新全局状态（选择最优先的microbatch）
    updateGlobalState();
}
