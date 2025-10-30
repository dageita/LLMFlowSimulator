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
                    // 接收到PP前向通信，直接调度COMPUTE_FWD，并使token ready，保证后续stage也能正确推进
                    shouldErase = true;
                    pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_FWD, mb, COMPUTE, workload->fwdCompTime, 0, 0);
                    onTokenReady(mb); // 新增，确保token ready
                    cout << "[PP_FWD->DIRECT] Rank " << rank->id << " received PP_FWD for mb=" << mb << ", directly scheduled COMPUTE_FWD 并 token ready" << endl;
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
                    
                    // PP_FWD发送完成后，不需要推进图状态，因为图状态已经在COMPUTE_FWD完成时推进了
                    // 这里只需要标记状态转换
                    state = COMPUTE;
                }
                break;

            case PP_COMM_BWD:
                cout << "[DEBUG] Rank " << rank->id 
                    << " microbatch=" << microbatch << endl;
                if (ep == EndpointType::RECV && mb < 0) {
                    // 接收到PP后向通信，直接调度BWD事件，不通过图驱动机制
                    // 但是最后一个PP stage不应该由PP通信触发BWD，应该只由同mb的COMPUTE_FWD触发
                    if (isLastRankInPipeline()) {
                        cout << "[PP_BWD->SKIP] Rank " << rank->id << " received PP_BWD for mb=" << mb 
                             << ", but this is the last PP stage, BWD should be triggered by COMPUTE_FWD, not PP communication" << endl;
                        shouldErase = true;
                    } else {
                        shouldErase = true;
                        pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_BWD, mb, COMPUTE, workload->bwdCompTime, 0, 0);
                        cout << "[PP_BWD->DIRECT] Rank " << rank->id << " received PP_BWD for mb=" << mb << ", directly scheduled COMPUTE_BWD" << endl;
                        
                        // Megatron-1F1B交替模式：rank0接收到PP_BWD后，先调度对应的BWD，BWD完成后再调度下一个FWD
                        if (isFirstRankInPipeline()) {
                            cout << "[PP-MEGATRON-1F1B] Rank " << rank->id << " received PP_BWD for mb=" << mb 
                                 << " (Megatron 1F1B: BWD will be scheduled first, then next FWD after BWD completion)" << endl;
                        }
                    }
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
                    
                    // TP通信接收完成，使用图驱动机制推进
                    if (eventType == TP_COMM_FWD) {
                        // TP前向通信完成，标记就绪，由图推进调度
                        onTokenReady(mb);
                        cout << "[TP_FWD->GRAPH] Rank " << rank->id << " token=" << mb << " ready after TP_FWD RECV" << endl;
                    } else if (eventType == TP_COMM_BWD) {
                        // TP后向通信完成，标记就绪，由图推进调度
                        onTokenReady(mb);
                        cout << "[TP_BWD->GRAPH] Rank " << rank->id << " token=" << mb << " ready after TP_BWD RECV" << endl;
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
                    
                    // 根据并行模式决定通信和计算调度策略（并在图推进下切换 expectedToken）
                    if (workload->PP > 1) {
                        if (!isLastRankInPipeline()) {
                            // 只要不是pipeline最后一组，FWD完成后要发送PP_FWD给下一个stage
                            pending_events.emplace_back(EndpointType::SENT, PP_COMM_FWD, mb, PP_WAIT, 0, 0, 0);
                            cout << "[PP-PIPELINE] Rank " << rank->id << " scheduled PP_COMM_FWD for mb=" << mb << " (to next pipeline stage)" << endl;
                        }
                        if (isLastRankInPipeline()) {
                            // 最后一组，直接进入BWD
                            pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_BWD, -mb, COMPUTE, workload->bwdCompTime, 0, 0);
                            cout << "[PP-PIPELINE] Rank " << rank->id << " scheduled COMPUTE_BWD for mb=" << -mb << " after COMPUTE_FWD (last pipeline stage)" << endl;
                        }
                        // TP+PP模式：在COMPUTE_FWD完成后调度TP通信（所有rank都需要）
                        if (workload->TP > 1) {
                            pending_events.emplace_back(EndpointType::SENT, EventType::TP_COMM_FWD, mb, TP_COMM, 0, 0, 0);
                            cout << "[TP+PP] Rank " << rank->id << " scheduled TP_COMM_FWD for mb=" << mb
                                 << " after COMPUTE_FWD (TP+PP mode: all ranks need TP communication)" << endl;
                        }
                        
                        // PP模式：FWD完成后推进图状态
                        cout << "[PP-GRAPH] Rank " << rank->id << " completed COMPUTE_FWD for mb=" << mb 
                             << " (PP mode: Megatron-LM 1F1B pattern)" << endl;
                        // 图驱动机制：对于PP模式，只有mb<=2时才调用onTokenReady，避免与PP-MEGATRON-1F1B机制冲突
                        if (mb <= 2) {
                            onTokenReady(mb);
                        } else {
                            cout << "[PP-GRAPH] Rank " << rank->id << " mb=" << mb << " > 2, skipping onTokenReady to avoid conflict with PP-MEGATRON-1F1B" << endl;
                        }
                    } else if (workload->TP > 1) {
                        // TP模式：使用图驱动机制（纯TP模式）
                        cout << "[TP-GRAPH] Rank " << rank->id << " completed COMPUTE_FWD for mb=" << mb 
                             << " (TP mode: using graph-driven mechanism)" << endl;
                    } else {
                        // 其他模式（DP或单机）：使用1F1B流水线
                        if (workload->DP > 1 && mb <= workload->microbatches) {
                            // 纯DP场景：当前FWD完成后，立即调度对应的BWD
                            pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_BWD, -mb, COMPUTE, workload->bwdCompTime, 0, 0);
                            cout << "[DP-ONLY-1F1B] Rank " << rank->id << " scheduled COMPUTE_BWD for mb=" << -mb
                                 << " after COMPUTE_FWD for mb=" << mb << endl;
                        } else if (workload->DP <= 1 && mb >= 1 && mb <= workload->microbatches) {
                            pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_BWD, -mb, COMPUTE, workload->bwdCompTime, 0, 0);
                            cout << "[SINGLE-DEVICE] Rank " << rank->id << " scheduled COMPUTE_BWD for mb=" << -mb
                                 << " after COMPUTE_FWD for mb=" << mb << endl;
                        }
                    }

                    // 图驱动机制：FWD 计算完成后不推进token，由tryScheduleExpectedToken统一处理
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
                    
                    // 3b. 检查是否需要触发DP通信（在COMPUTE_BWD事件调度时检查）
                    // 在1F1B流水线中，所有rank完成最后一个microbatch的COMP_BWD后都应该触发DP通信
                    // DP通信是梯度同步，需要在所有DP组内进行AllReduce操作，所有rank都应该参与
                    if (workload->DP > 1 && std::abs(mb) == workload->microbatches) {
                        // 检查当前rank是否已经触发过DP通信，避免重复触发
                        // 注意：不能依赖completedBwdMicrobatches，因为最后一个PP stage完成BWD时会同步更新所有rank的状态
                        // 使用rank级别的跟踪，避免static变量被所有rank共享
                        if (completedFwdMicrobatches.find(mb) == completedFwdMicrobatches.end()) {
                            // 当前rank首次完成最后一个microbatch的COMP_BWD，增加计数器
                            simulator->completedLastBwdCount++;
                            cout << "[DP-SYNC] Rank " << rank->id << " (pp=" << rank->pp << " stage) completed last microbatch COMP_BWD for mb=" << mb 
                                 << ", completedLastBwdCount=" << simulator->completedLastBwdCount 
                                 << "/" << simulator->totalRanks << " (checking if all ranks completed)" << endl;
                            
                            // 所有rank都添加DP通信事件到各自的GroupTask，确保DP通信的起始时间都是PP=0 stage的COMP_BWD结束时间
                            if (dpGroupTask) {
                                dpGroupTask->addEvent(rank->id, mb); // 使用最后一个microbatch的数值进行梯度同步
                                cout << "[DP-SYNC] Rank " << rank->id << " added DP communication event to GroupTask " 
                                     << dpGroupTask->group->id << " for mb=" << mb << " (gradient sync within DP group)" << endl;
                            }
                            
                            // 检查是否所有rank都完成了最后一个microbatch的BWD
                            if (simulator->completedLastBwdCount >= simulator->totalRanks) {
                                cout << "[DP-SYNC] All ranks completed last microbatch BWD, unified DP communication triggered" << endl;
                            } else {
                                cout << "[DP-SYNC] Rank " << rank->id << " completed last microbatch BWD, waiting for other ranks (" 
                                     << simulator->completedLastBwdCount << "/" << simulator->totalRanks << ")" << endl;
                            }
                            
                            // 标记当前rank已经触发过DP通信，避免重复触发
                            completedFwdMicrobatches.insert(mb);
                        } else {
                            cout << "[DP-SYNC] Rank " << rank->id << " already triggered DP communication for mb=" << mb 
                                 << ", skipping duplicate trigger" << endl;
                        }
                    }
                    
                    // 对于反向传播，需要触发通信事件（TP优先级高于PP）
                    // 注意：只有从COMPUTE_FWD转换来的COMPUTE_BWD才需要触发TP_BWD
                    // 从TP_BWD转换来的COMPUTE_BWD不应该再次触发TP_BWD
                    
                    // 1. 首先预调度TP_BWD事件（如果TP>1且不是从TP_BWD转换来的）
                    if (workload->TP > 1 && mb < 0) {
                        // TP+PP模式：在COMPUTE_BWD完成后调度TP通信（所有rank都需要）
                        pending_events.emplace_back(EndpointType::SENT, EventType::TP_COMM_BWD, mb, TP_COMM, 0, 0, 0);
                        cout << "[TP+PP] Rank " << rank->id << " scheduled TP_COMM_BWD for mb=" << mb
                             << " after COMPUTE_BWD (TP+PP mode: all ranks need TP communication)" << endl;
                    }
                    
                    // 2. 然后预调度PP_BWD事件（如果PP>1且不是第一个rank）
                    // 对于最后一个PP stage，在COMPUTE_BWD完成后应该立即发送PP_BWD
                    if (workload->PP > 1 && !isFirstRankInPipeline()) {
                        pending_events.emplace_back(EndpointType::SENT, PP_COMM_BWD, mb, PP_WAIT, 0, 0, 0);
                        cout << "[1F1B] Rank " << rank->id << " scheduled PP_BWD SENT for mb=" << mb 
                             << " after COMPUTE_BWD completion (PP mode)" << endl;
                    }
                    
                    // 3a. 根据并行模式决定流水线策略
                    int currentAbsMb = std::abs(mb);
                    if (currentAbsMb < workload->microbatches) {
                        int nextMb = currentAbsMb + 1;
                        
                        if (workload->PP > 1) {
                            // PP模式：根据Megatron-LM的1F1B实现，BWD完成后调度下一个FWD
                            if (isFirstRankInPipeline()) {
                                // rank0：BWD完成后调度下一个FWD（实现真正的Megatron-1F1B交替模式）
                                // 根据Megatron-1F1B逻辑：mb=-1 BWD完成后调度mb=3 FWD，mb=-2 BWD完成后调度mb=4 FWD
                                int nextMb = -mb + 2; // 从BWD的mb计算下一个FWD的mb：-(-1)+2=3, -(-2)+2=4
                                if (nextMb <= workload->microbatches) {
                                    // 检查是否已经调度过下一个FWD，避免重复调度
                                    // 使用scheduledFwdMicrobatches集合检查，记录所有已调度的FWD事件
                                    bool alreadyScheduled = (scheduledFwdMicrobatches.find(nextMb) != scheduledFwdMicrobatches.end());
                                    
                                    if (!alreadyScheduled) {
                                        // 只添加到events中，不添加到pending_events，避免重复调度
                                        addEvent(EndpointType::NONE_ENDPOINT, EventType::COMPUTE_FWD, nextMb, COMPUTE, workload->fwdCompTime, 0, 0);
                                        cout << "[PP-MEGATRON-1F1B] Rank " << rank->id << " scheduled next COMPUTE_FWD for mb=" << nextMb 
                                             << " after COMPUTE_BWD for mb=" << mb << " (Megatron 1F1B: alternating pattern)" << endl;
                                    } else {
                                        cout << "[PP-MEGATRON-1F1B] Rank " << rank->id << " COMPUTE_FWD for mb=" << nextMb 
                                             << " already scheduled, skipping to prevent duplicate" << endl;
                                    }
                                } else {
                                    cout << "[PP-MEGATRON-1F1B] Rank " << rank->id << " completed COMPUTE_BWD for mb=" << mb 
                                         << " (Megatron 1F1B: no more FWD to schedule, nextMb=" << nextMb << " > microbatches=" << workload->microbatches << ")" << endl;
                                }
                            } else {
                                // 后续rank：BWD完成后推进图状态
                                if (expectedToken != 0) {
                                    cout << "[PP-GRAPH] Rank " << rank->id << " completed COMPUTE_BWD for mb=" << mb 
                                         << " (PP mode: advancing to next FWD after BWD completion)" << endl;
                                    // PP模式：BWD完成后推进图到下一个FWD
                                    // 但是最后一个PP stage不应该推进图状态，因为BWD应该只由同mb的COMPUTE_FWD触发一次
                                    if (!isLastRankInPipeline()) {
                                        onTokenReady(mb);
                                    } else {
                                        cout << "[PP-GRAPH] Rank " << rank->id << " is last PP stage, skipping onTokenReady to avoid duplicate BWD scheduling" << endl;
                                    }
                                } else {
                                    cout << "[PP-GRAPH] Rank " << rank->id << " completed COMPUTE_BWD for mb=" << mb 
                                         << " (PP mode: graph already terminated, skipping graph advancement)" << endl;
                                }
                            }
                        } else if (workload->TP > 1) {
                            // 纯TP模式：使用图驱动机制，不直接调度下一个microbatch
                            cout << "[TP-GRAPH] Rank " << rank->id << " completed COMPUTE_BWD for mb=" << mb 
                                 << " (pure TP mode: next microbatch will be scheduled by graph-driven mechanism)" << endl;
                        } else {
                            // 其他模式（DP或单机）：使用1F1B流水线
                            pending_events.emplace_back(EndpointType::NONE_ENDPOINT, COMPUTE_FWD, nextMb, COMPUTE, workload->fwdCompTime, 0, 0);
                            cout << "[DEFAULT-1F1B] Rank " << rank->id << " scheduled COMPUTE_FWD for mb=" << nextMb
                                 << " after completing COMPUTE_BWD for mb=" << mb << " (default 1F1B)" << endl;
                        }
                    }

                    // DP通信检查已移至COMPUTE_BWD事件调度时进行，避免重复检查

                    // 图驱动机制：BWD 计算完成后不推进token，由tryScheduleExpectedToken统一处理
                }
                break;

            case DP_COMM_EVENT:
                if (ep == EndpointType::RECV) {
                    // DP通信事件完成，更新所有microbatch的BWD完成状态
                    for (int i = 1; i <= workload->microbatches; ++i) {
                        completedBwdMicrobatches.insert(-i);
                    }
                    state = DONE;
                    shouldErase = true;
                    cout << "[DP-COMM] Rank " << rank->id << " received DP communication for mb=" << mb 
                         << ", updated completedBwdMicrobatches, entering DONE state" << endl;
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
    // 图驱动调度逻辑已移至tryScheduleExpectedToken方法中，避免重复调度

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
    
    // 图驱动调度：处理就绪的token
    tryScheduleExpectedToken();
    
    return processed;
}
