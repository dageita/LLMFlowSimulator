#include "common.h"
#include "simulator.h"

extern Workload* workload;
extern Simulator* simulator;

void RankTask::addEvent(int ep, int eventType, int mb, RankState state, double remainingTime, double startTime, double endTime) {
    events.emplace_back(ep, eventType, mb, state, remainingTime, startTime, endTime);
    
    // 记录已调度的FWD事件，用于重复检查
    if (eventType == COMPUTE_FWD && mb > 0) {
        scheduledFwdMicrobatches.insert(mb);
        cout << "[SCHEDULED-FWD] Rank " << rank->id << " scheduled COMPUTE_FWD for mb=" << mb << " (added to scheduledFwdMicrobatches)" << endl;
    }
    
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

    // 1F1B 图驱动推进初始化
    expectedToken = 0;
    readyTokens.clear();
}

void RankTask::onTokenReady(int token) {
    readyTokens.insert(token);
    cout << "[TOKEN-READY] Rank " << rank->id << " token=" << token << " ready. expectedToken=" << expectedToken << endl;
}

void RankTask::tryScheduleExpectedToken() {
    cout << "[GRAPH-DEBUG] Rank " << rank->id << " tryScheduleExpectedToken: expectedToken=" << expectedToken 
         << ", readyTokens.size=" << readyTokens.size() << endl;
    if (expectedToken == 0) {
        cout << "[GRAPH-DEBUG] Rank " << rank->id << " expectedToken=0, returning" << endl;
        return;
    }
    if (readyTokens.find(expectedToken) == readyTokens.end()) {
        cout << "[GRAPH-DEBUG] Rank " << rank->id << " expectedToken=" << expectedToken << " not ready, returning" << endl;
        return;
    }
    
        // 检查是否已经调度过这个token，避免重复调度
        // 检查当前events中是否已经有相同的事件
        bool alreadyScheduled = false;
        
        // 检查events列表
        for (const auto& event : events) {
            if (expectedToken > 0) {
                if (std::get<1>(event) == EventType::COMPUTE_FWD && std::get<2>(event) == expectedToken) {
                    alreadyScheduled = true;
                    break;
                }
            } else {
                if (std::get<1>(event) == EventType::COMPUTE_BWD && std::get<2>(event) == expectedToken) {
                    alreadyScheduled = true;
                    break;
                }
            }
        }
    
    if (alreadyScheduled) {
        cout << "[GRAPH-DEBUG] Rank " << rank->id << " COMPUTE_" << (expectedToken > 0 ? "FWD" : "BWD") 
             << " for mb=" << expectedToken << " already scheduled, skipping" << endl;
        // 不要删除readyTokens中的token，因为图驱动机制可能还会被调用
        // readyTokens.erase(expectedToken);
        return;
    }

    // 按 expectedToken 调度 compute 事件
    if (expectedToken > 0) {
        if (workload->PP > 1) {
            // PP模式：根据Megatron-LM的1F1B实现
            if (isFirstRankInPipeline()) {
                // rank0：实现Megatron 1F1B模式
                // 初始阶段：只调度mb=1和mb=2的FWD
                // 之后等待PP通信和BWD，不再主动调度FWD
                if (expectedToken <= 2) {
                    // 检查是否已经被调度过
                    bool alreadyScheduled = false;
                    for (const auto& event : events) {
                        if (std::get<1>(event) == EventType::COMPUTE_FWD && std::get<2>(event) == expectedToken) {
                            alreadyScheduled = true;
                            break;
                        }
                    }
                    
                    if (!alreadyScheduled) {
                        addEvent(EndpointType::NONE_ENDPOINT, EventType::COMPUTE_FWD, expectedToken, COMPUTE, workload->fwdCompTime, 0, 0);
                        cout << "[PP-MEGATRON-1F1B] Rank " << rank->id << " scheduled COMPUTE_FWD for mb=" << expectedToken << " (Megatron 1F1B: initial phase, mb<=2)" << endl;
                        
                        // TP模式：在FWD完成后调度TP通信（纯TP模式）
                        // 对于TP+PP模式，TP通信应该在COMPUTE_FWD完成后调度，避免重复调度
                        if (workload->TP > 1 && workload->PP <= 1) {
                            addEvent(EndpointType::SENT, EventType::TP_COMM_FWD, expectedToken, TP_COMM, 0, 0, 0);
                            cout << "[GRAPH-ADVANCE] Rank " << rank->id << " scheduled TP_COMM_FWD for mb=" << expectedToken << " by graph order (pure TP mode)" << endl;
                        } else if (workload->TP > 1 && workload->PP > 1) {
                            cout << "[GRAPH-ADVANCE] Rank " << rank->id << " TP+PP mode: TP communication will be scheduled after COMPUTE_FWD completion" << endl;
                        }
                        
                        // PP模式：PP通信事件应该在COMPUTE_FWD完成后发送，而不是在调度时发送
                        // 这里只调度COMPUTE_FWD，PP通信事件由COMPUTE_FWD完成后的处理逻辑负责
                        if (workload->PP > 1) {
                            cout << "[GRAPH-ADVANCE] Rank " << rank->id << " PP communication will be scheduled after COMPUTE_FWD completion (PP mode)" << endl;
                        }
                    } else {
                        cout << "[PP-MEGATRON-1F1B] Rank " << rank->id << " COMPUTE_FWD for mb=" << expectedToken << " already scheduled, skipping" << endl;
                    }
                } else {
                    // mb>2：完全跳过图驱动机制的调度，由PP-MEGATRON-1F1B机制处理
                    cout << "[PP-MEGATRON-1F1B] Rank " << rank->id << " expectedToken=" << expectedToken << " > 2, skipping graph-driven scheduling (Megatron 1F1B: alternating phase handled by PP-MEGATRON-1F1B)" << endl;
                    // 消耗就绪标记，避免重复调度
                    readyTokens.erase(expectedToken);
                    return;
                }
            } else {
                // 后续rank：等待PP通信，不直接调度COMPUTE_FWD
                cout << "[PP-GRAPH] Rank " << rank->id << " expectedToken=" << expectedToken << " is FWD, waiting for PP communication in PP mode" << endl;
            }
        } else {
            // 非PP模式：直接调度COMPUTE_FWD
            addEvent(EndpointType::NONE_ENDPOINT, EventType::COMPUTE_FWD, expectedToken, COMPUTE, workload->fwdCompTime, 0, 0);
            cout << "[GRAPH-ADVANCE] Rank " << rank->id << " scheduled COMPUTE_FWD for mb=" << expectedToken << " by graph order" << endl;
            
            // TP模式：在FWD完成后调度TP通信（纯TP模式）
            // 对于TP+PP模式，TP通信应该在COMPUTE_FWD完成后调度，避免重复调度
            if (workload->TP > 1 && workload->PP <= 1) {
                addEvent(EndpointType::SENT, EventType::TP_COMM_FWD, expectedToken, TP_COMM, 0, 0, 0);
                cout << "[GRAPH-ADVANCE] Rank " << rank->id << " scheduled TP_COMM_FWD for mb=" << expectedToken << " by graph order (pure TP mode)" << endl;
            } else if (workload->TP > 1 && workload->PP > 1) {
                cout << "[GRAPH-ADVANCE] Rank " << rank->id << " TP+PP mode: TP communication will be scheduled after COMPUTE_FWD completion" << endl;
            }
            
            // PP模式：在FWD完成后调度PP通信（如果同时有TP和PP）
            if (workload->PP > 1) {
                addEvent(EndpointType::SENT, EventType::PP_COMM_FWD, expectedToken, PP_WAIT, 0, 0, 0);
                cout << "[GRAPH-ADVANCE] Rank " << rank->id << " scheduled PP_COMM_FWD for mb=" << expectedToken << " by graph order (PP mode)" << endl;
            }
        }
    } else {
        // BWD阶段：根据并行模式决定调度策略
        if (workload->PP > 1) {
            // PP模式：只有最后一个PP stage才由图驱动机制直接调度BWD
            if (isLastRankInPipeline()) {
                // 最后一个PP stage：BWD应该只由同mb的COMPUTE_FWD触发，不由图驱动机制调度
                cout << "[PP-GRAPH] Rank " << rank->id << " expectedToken=" << expectedToken << " is BWD, but last PP stage BWD should be triggered by COMPUTE_FWD, not graph scheduling" << endl;
            } else {
                // 非最后一个PP stage：BWD由PP通信传播触发，不由图驱动机制调度
                cout << "[PP-GRAPH] Rank " << rank->id << " expectedToken=" << expectedToken << " is BWD, but PP mode uses communication propagation, not graph scheduling (non-last stage)" << endl;
            }
        } else if (workload->TP > 1) {
            // 纯TP模式：由图驱动机制调度BWD
            addEvent(EndpointType::NONE_ENDPOINT, EventType::COMPUTE_BWD, expectedToken, COMPUTE, workload->bwdCompTime, 0, 0);
            cout << "[GRAPH-ADVANCE] Rank " << rank->id << " scheduled COMPUTE_BWD for mb=" << expectedToken << " by graph order (pure TP mode)" << endl;
            
            // TP模式：在BWD完成后调度TP通信（纯TP模式）
            // 对于TP+PP模式，TP通信应该在COMPUTE_BWD完成后调度，避免重复调度
            if (workload->PP <= 1) {
                addEvent(EndpointType::SENT, EventType::TP_COMM_BWD, expectedToken, TP_COMM, 0, 0, 0);
                cout << "[GRAPH-ADVANCE] Rank " << rank->id << " scheduled TP_COMM_BWD for mb=" << expectedToken << " by graph order (pure TP mode)" << endl;
            } else {
                cout << "[GRAPH-ADVANCE] Rank " << rank->id << " TP+PP mode: TP communication will be scheduled after COMPUTE_BWD completion" << endl;
            }
        } else {
            // 其他模式（DP或单机）：使用图驱动机制调度BWD
            addEvent(EndpointType::NONE_ENDPOINT, EventType::COMPUTE_BWD, expectedToken, COMPUTE, workload->bwdCompTime, 0, 0);
            cout << "[GRAPH-ADVANCE] Rank " << rank->id << " scheduled COMPUTE_BWD for mb=" << expectedToken << " by graph order (other mode)" << endl;
        }
    }
    
    // 消耗就绪标记，避免重复调度
    readyTokens.erase(expectedToken);
    cout << "[GRAPH-ADVANCE] Rank " << rank->id << " consumed ready token " << expectedToken << ", remaining readyTokens.size=" << readyTokens.size() << endl;
    
    // 图驱动机制：调度完成后推进到下一个token
    // 对于TP>1的情况，使用图驱动机制
    // 对于PP>1的情况，只在FWD完成后推进，BWD由PP通信传播
    if (workload->TP > 1) {
        int stage = rank->pp;
        int currentToken = expectedToken; // 保存当前token
        auto itNext = workload->nextMicrobatch.find(make_tuple(stage, currentToken));
        if (itNext != workload->nextMicrobatch.end()) {
            int nextToken = itNext->second;
            expectedToken = nextToken;
            cout << "[GRAPH-NEXT] Rank " << rank->id << " stage=" << stage << " from token=" << currentToken << " -> next token=" << nextToken << endl;
            // 标记下一个token为就绪
            onTokenReady(nextToken);
        } else {
            cout << "[GRAPH-NEXT] Rank " << rank->id << " stage=" << stage << " from token=" << currentToken << " -> no next token found, graph completed" << endl;
            // 图已完成，设置expectedToken为0表示结束
            expectedToken = 0;
        }
    } else if (workload->PP > 1) {
        // PP模式：根据Megatron 1F1B实现图推进
        if (isFirstRankInPipeline()) {
            // rank0：只在初始阶段（mb<=2且mb>0）推进图
            if (expectedToken > 0 && expectedToken <= 2) {
                int stage = rank->pp;
                int currentToken = expectedToken; // 保存当前token
                auto itNext = workload->nextMicrobatch.find(make_tuple(stage, currentToken));
                if (itNext != workload->nextMicrobatch.end()) {
                    int nextToken = itNext->second;
                    expectedToken = nextToken;
                    cout << "[PP-MEGATRON-1F1B] Rank " << rank->id << " stage=" << stage << " from token=" << currentToken << " -> next token=" << nextToken << " (Megatron 1F1B: initial phase)" << endl;
                    // 标记下一个token为就绪
                    onTokenReady(nextToken);
                } else {
                    cout << "[PP-MEGATRON-1F1B] Rank " << rank->id << " stage=" << stage << " from token=" << currentToken << " -> no next token found, initial phase completed" << endl;
                    // 初始阶段完成，设置expectedToken为0，等待PP通信和BWD
                    expectedToken = 0;
                }
            } else {
                // mb>2或mb<0：不推进图，等待PP通信和BWD
                cout << "[PP-MEGATRON-1F1B] Rank " << rank->id << " expectedToken=" << expectedToken << " not in initial phase (not 1 or 2), not advancing graph (Megatron 1F1B: alternating phase)" << endl;
            }
        } else {
            // 后续rank：FWD和BWD完成后都推进图到下一个FWD
            int stage = rank->pp;
            int currentToken = expectedToken; // 保存当前token
            auto itNext = workload->nextMicrobatch.find(make_tuple(stage, currentToken));
            if (itNext != workload->nextMicrobatch.end()) {
                int nextToken = itNext->second;
                expectedToken = nextToken;
                cout << "[PP-GRAPH-NEXT] Rank " << rank->id << " stage=" << stage << " from token=" << currentToken << " -> next token=" << nextToken << endl;
                // 标记下一个token为就绪
                onTokenReady(nextToken);
            } else {
                cout << "[PP-GRAPH-NEXT] Rank " << rank->id << " stage=" << stage << " from token=" << currentToken << " -> no next token found, graph completed" << endl;
                // 图已完成，设置expectedToken为0表示结束
                expectedToken = 0;
            }
        }
    }
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
