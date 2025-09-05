#include "common.h"
#include "simulator.h"

extern Workload* workload;
extern Simulator* simulator;

// RankTask的handleEvents和progress方法实现

int RankTask::handleEvents(){   // < EP, TYPE, MB >
    int processed = 0;
    cout << "handleEvents for RankTask: " << rank->id << ", events size: " 
        << events.size() <<  ", microbatch: " << microbatch << endl;

    std::vector<std::tuple<int, int, int, RankState, double, double, double>> pending_events; // 临时存储新事件

    for(auto it = events.begin(); it != events.end(); ) {
        int ep = get<0>(*it); // 端点
        int eventType = get<1>(*it);  // 事件类型
        int mb = get<2>(*it); // microbatch
        RankState& eventState = get<3>(*it); // 事件状态
        double& eventRemainingTime = get<4>(*it); // 事件剩余时间
        double& eventStartTime = get<5>(*it); // 事件开始时间
        double& eventEndTime = get<6>(*it); // 事件结束时间

        bool shouldErase = false; // 标记是否应删除该事件

        cout << "[HANDLE EVENT] Rank " << rank->id 
            << ", EP=" << simulator->endpointToString(static_cast<EndpointType>(ep))
            << ", EventType=" << simulator->eventTypeToString(static_cast<EventType>(eventType))
            << ", MB=" << mb 
            << ", CurrentState=" << simulator->stateToString(state) << endl;

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
                    // 更新当前rank的microbatch时间
                    MicrobatchManager::updateRankMicrobatchTime(rank->id, mb, endTime);
                    // 更新rankGlobalTime为microbatch的最新时间
                    rankGlobalTime = endTime;
                    
                    // 更新上一个处理的microbatch
                    updateLastProcessedMb(mb);
                    
                    // 对于最后一个rank，预调度反向计算事件（1F1B流水线）
                    if (isLastRankInPipeline()) {
                        pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_BWD, -mb, COMPUTE, workload->bwdCompTime, 0, 0);
                        cout << "[1F1B] Last rank " << rank->id << " scheduled COMPUTE_BWD for mb=" << (-mb) 
                             << " after PP_FWD RECV for mb=" << mb << endl;
                    }
                }
                else if (ep == EndpointType::SENT) {
                    // 处理PP前向发送事件，通过GroupTask管理通信
                    if (ppFwdGroupTask) {
                        // 向PP前向GroupTask添加事件，让它管理通信流程
                        ppFwdGroupTask->addEvent(rank->id, mb);
                        cout << "[HANDLE-EVENT] Added PP_FWD event to GroupTask " << ppFwdGroupTask->group->id 
                             << " for mb=" << mb << endl;
                    }
                    
                    // PP前向发送完成，可以删除事件
                    shouldErase = true;
                    cout << "Rank " << rank->id << " completed PP_FWD send for mb=" << mb << endl;
                }
                break;

            case PP_COMM_BWD:
                cout << "[DEBUG] Rank " << rank->id 
                    << " microbatch=" << microbatch << endl;
                if (ep == EndpointType::RECV && mb < 0) {
                    state = COMPUTE;
                    remainingTime = workload->bwdCompTime;
                    shouldErase = true;
                    
                    // 关键修复：将COMPUTE_BWD事件添加到pending_events，确保后续处理逻辑能执行
                    // 注意：不在这里记录COMPUTE_BWD事件或更新时间，这些操作将在COMP_BWD分支中完成
                    pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_BWD, mb, COMPUTE, workload->bwdCompTime, 0, 0);
                    cout << "[PP_BWD->COMP_BWD] Rank " << rank->id << " scheduled COMPUTE_BWD for mb=" << mb 
                         << " after PP_BWD RECV" << endl;
                    
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
                    // 使用TP通信事件的结束时间更新microbatch时间
                    // 如果事件还没有结束时间，使用当前rankGlobalTime
                    double tpEventEndTime = (eventEndTime > 0) ? eventEndTime : rankGlobalTime;
                    MicrobatchManager::updateMicrobatchGlobalTime(mb, tpEventEndTime);
                    // 更新当前rank的microbatch时间
                    MicrobatchManager::updateRankMicrobatchTime(rank->id, mb, tpEventEndTime);
                    
                    // TP通信接收完成，根据事件类型和PP配置决定后续调度
                    if (eventType == TP_COMM_FWD) {
                        // TP前向通信完成，需要决定后续流程
                        if (workload->PP > 1) {
                            // 存在PP组，触发PP_WAIT
                            pending_events.emplace_back(EndpointType::SENT, PP_COMM_FWD, mb, PP_WAIT, 0, 0, 0);
                            cout << "[TP_FWD->PP] Rank " << rank->id << " scheduled PP_COMM_FWD for mb=" << mb 
                                 << " after TP_FWD RECV (PP>1)" << endl;
                        } else {
                            // 无PP组，直接触发反向计算
                            pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_BWD, -mb, COMPUTE, workload->bwdCompTime, 0, 0);
                            cout << "[TP_FWD->COMP_BWD] Rank " << rank->id << " scheduled COMPUTE_BWD for mb=" << (-mb) 
                                 << " after TP_FWD RECV (PP=1)" << endl;
                        }
                    } else if (eventType == TP_COMM_BWD) {
                        // TP后向通信完成，需要决定后续流程
                        if (workload->PP > 1) {
                            // 存在PP组，触发PP_WAIT
                            pending_events.emplace_back(EndpointType::SENT, PP_COMM_BWD, mb, PP_WAIT, 0, 0, 0);
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
                    // 更新当前rank的microbatch时间
                    MicrobatchManager::updateRankMicrobatchTime(rank->id, mb, endTime);
                    // 更新rankGlobalTime为microbatch的最新时间
                    rankGlobalTime = endTime;
                    
                    // 更新上一个处理的microbatch
                    updateLastProcessedMb(mb);
                    
                    // 为第一个rank，预调度计算完成后的通信事件（TP优先级高于PP）
                    if (isFirstRankInPipeline()) {
                        // 1. 首先预调度TP_FWD事件（如果TP>1）
                        if (workload->TP > 1) {
                            pending_events.emplace_back(EndpointType::SENT, TP_COMM_FWD, mb, TP_COMM, 0, 0, 0);
                            cout << "Rank " << rank->id << " scheduled TP_COMM_FWD for mb=" << mb << endl;
                        }
                        
                        // 2. 然后预调度PP_FWD事件（如果PP>1且TP=1）
                        // 注意：对于TP>1的情况，PP事件将由TP通信完成后触发，不需要在这里预调度
                        if (workload->PP > 1 && workload->TP <= 1) {
                            pending_events.emplace_back(EndpointType::SENT, PP_COMM_FWD, mb, PP_WAIT, 0, 0, 0);
                            cout << "Rank " << rank->id << " scheduled PP_COMM_FWD for mb=" << mb << " (TP=1 case)" << endl;
                        } else if (workload->PP > 1 && workload->TP > 1) {
                            cout << "Rank " << rank->id << " PP>1 and TP>1: PP_FWD will be triggered by TP_FWD completion for mb=" << mb << endl;
                        }
                    }

                    // 纯DP场景（TP<=1, PP<=1, DP>1）：当前FWD完成后，立即调度对应的BWD
                    if (workload->TP <= 1 && workload->PP <= 1 && workload->DP > 1 && mb <= workload->microbatches) {
                        pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_BWD, -mb, COMPUTE, workload->bwdCompTime, 0, 0);
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
                    // 更新当前rank的microbatch时间
                    MicrobatchManager::updateRankMicrobatchTime(rank->id, mb, endTime);
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
                        pending_events.emplace_back(EndpointType::SENT, TP_COMM_BWD, mb, TP_COMM, 0, 0, 0);
                        cout << "[1F1B] Rank " << rank->id << " scheduled TP_BWD SENT for mb=" << mb 
                             << " after COMPUTE_BWD scheduling" << endl;
                    }
                    
                    // 2. 然后预调度PP_BWD事件（如果PP>1且TP=1且不是第一个rank）
                    // 注意：对于TP>1的情况，PP_BWD事件将由TP_BWD完成后触发，不需要在这里预调度
                    if (workload->PP > 1 && workload->TP <= 1 && !isFirstRankInPipeline()) {
                        pending_events.emplace_back(EndpointType::SENT, PP_COMM_BWD, mb, PP_WAIT, 0, 0, 0);
                        cout << "[1F1B] Rank " << rank->id << " scheduled PP_BWD SENT for mb=" << mb 
                             << " after COMPUTE_BWD scheduling (TP=1 case)" << endl;
                    } else if (workload->PP > 1 && workload->TP > 1 && !isFirstRankInPipeline()) {
                        cout << "[1F1B] PP>1 and TP>1: PP_BWD will be triggered by TP_BWD completion for rank " << rank->id << endl;
                    }
                    
                    // 3a. 纯DP场景（TP<=1, PP<=1, DP>1）：在非最后一个microbatch时，继续调度下一个FWD
                    if (workload->TP <= 1 && workload->PP <= 1 && workload->DP > 1) {
                        int currentAbsMb = std::abs(mb);
                        if (currentAbsMb < workload->microbatches) {
                            int nextMb = currentAbsMb + 1;
                            pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_FWD, nextMb, COMPUTE, workload->fwdCompTime, 0, 0);
                            cout << "[DP-ONLY-1F1B] Rank " << rank->id << " scheduled COMPUTE_FWD for mb=" << nextMb
                                 << " after completing COMPUTE_BWD for mb=" << mb << endl;
                        }
                    }

                    // 3b. 检查是否需要触发DP通信
                    // 在1F1B流水线中，每个rank完成最后一个microbatch的COMP_BWD后，应该触发DP通信
                    // DP通信是在DP组内进行的，每个rank独立完成自己的计算后参与DP同步
                    if (workload->DP > 1 && std::abs(mb) == workload->microbatches) {
                        // 当前rank完成了最后一个microbatch的COMP_BWD，向DP GroupTask添加事件
                        if (dpGroupTask) {
                            dpGroupTask->addEvent(rank->id, mb);
                            cout << "[DP-SYNC] Rank " << rank->id << " completed last microbatch COMP_BWD for mb=" << mb 
                                 << ", added DP communication event to GroupTask " << dpGroupTask->group->id 
                                 << " (gradient sync within DP group)" << endl;
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
    for (auto& [ep, type, mb, state, remainingTime, startTime, endTime] : pending_events) {
        addEvent(ep, type, mb, state, remainingTime, startTime, endTime);
        cout << "[PENDING] Added event: EP=" << simulator->endpointToString(static_cast<EndpointType>(ep)) 
            << ", Type=" << simulator->eventTypeToString(static_cast<EventType>(type)) 
            << ", MB=" << mb 
            << ", State=" << simulator->stateToString(state) 
            << ", RemainingTime=" << remainingTime 
            << ", StartTime=" << startTime
            << ", EndTime=" << endTime << endl;
    }
    
    return processed;
}
