#include "common.h"
#include "simulator.h"

extern Workload* workload;
extern Simulator* simulator;

// 这些方法属于RankTask类，但由于文件大小限制，放在单独的文件中

double RankTask::stableTime(){
    // 检查所有事件中是否有需要计算的事件
    for (const auto& event : events) {
        int ep = get<0>(event);
        int eventType = get<1>(event);
        RankState eventState = get<3>(event);
        double eventRemainingTime = get<4>(event);
        double eventStartTime = get<5>(event);
        double eventEndTime = get<6>(event);
        
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
            double eventStartTime = get<5>(event);
            double eventEndTime = get<6>(event);
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

void RankTask::updateGlobalState() {
    // 找到最优先的microbatch作为当前状态
    int priorityMb = 0;
    RankState priorityState = PP_WAIT;
    double priorityRemainingTime = 0;
    
    for (const auto& event : events) {
        int mb = get<2>(event);
        RankState eventState = get<3>(event);
        double eventRemainingTime = get<4>(event);
        double eventStartTime = get<5>(event);
        double eventEndTime = get<6>(event);
        
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
    
    double mbGlobalTime = MicrobatchManager::getMicrobatchGlobalTime(mb);

    // 检查事件是否已经完成
    if (eventStartTime + time <= rankGlobalTime) {
        cout << "[TIME-CALC] Event already completed, no time update needed" << endl;
        return rankGlobalTime;
    }
    
    if (abs(lastProcessedMb) == abs(mb)) {
        // 同一个microbatch的事件，串行执行
        // 需要考虑microbatch globalTime和rankGlobalTime的相对大小
        double startTime = std::max(rankGlobalTime, mbGlobalTime);
        cout << "[TIME-CALC] Same microbatch (abs: " << abs(mb) << "), serial execution" << endl;
        cout << "[TIME-CALC] rankGlobalTime=" << rankGlobalTime << ", mbGlobalTime=" << mbGlobalTime 
             << ", startTime=" << startTime << endl;
        return startTime;
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
            cout << "[TIME-CALC] mb: " << abs(mb) << ", mbGlobalTime: " << mbGlobalTime << endl;
            return mbGlobalTime;
        } else {
            // 不需要TP或DP通信的rank，使用microbatch globalTime进行同步
            cout << "[TIME-CALC] mb: " << abs(mb) << ", mbGlobalTime: " << mbGlobalTime << endl;
            double newTime = std::max(rankGlobalTime + time, mbGlobalTime);
            cout << "[TIME-CALC] Different microbatch (abs: " << abs(mb) << " vs " << abs(lastProcessedMb) 
                 << "), parallel execution with microbatch sync, new time=" << newTime << endl;
            return newTime;
        }
    }
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
