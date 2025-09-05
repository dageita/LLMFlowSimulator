#include "common.h"
#include "simulator.h"
#include <cmath>

// 全局microbatch状态管理器的静态成员定义
map<int, MicrobatchManager::MicrobatchState> MicrobatchManager::microbatchStates;
map<pair<int, int>, double> MicrobatchManager::rankMicrobatchTimes;

// MicrobatchManager的静态方法实现
void MicrobatchManager::updateMicrobatchGlobalTime(int mb, double time) {
    cout << "[MICROBATCH-TIME-DEBUG] update MicrobatchGlobalTime called: mb=" << mb << ", time=" << time << endl;
    
    // 确保microbatch状态存在
    if (microbatchStates.find(mb) == microbatchStates.end()) {
        cout << "[MICROBATCH-TIME-DEBUG] Creating new microbatch state for mb=" << mb << endl;
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
    } else {
        auto it = microbatchStates.find(mb);
        if (it != microbatchStates.end()) {
            cout << "[MICROBATCH-TIME-DEBUG] Microbatch " << mb << " already exists with time=" << it->second.globalTime << endl;
        } else {
            cout << "[MICROBATCH-TIME-DEBUG] ERROR: Microbatch " << mb << " should exist but not found!" << endl;
        }
    }
    
    // 对于TP并行，microbatch globalTime主要用于PP通信的同步
    // TP组内的rank应该独立推进时间，不应该被microbatch globalTime限制
    // 因此，我们只在PP通信完成时更新microbatch globalTime
    
    // 取较大值，确保microbatch时间只能向前推进
    auto it = microbatchStates.find(mb);
    if (it == microbatchStates.end()) {
        cout << "[MICROBATCH-TIME-DEBUG] ERROR: Microbatch " << mb << " not found after creation!" << endl;
        return;
    }
    double currentTime = it->second.globalTime;
    double newTime = std::max(currentTime, time);
    
    if (newTime > currentTime) {
        it->second.globalTime = newTime;
        cout << "[MICROBATCH-TIME] Global updated microbatch " << mb 
             << " global time from " << currentTime << " to " << newTime << endl;
        
        // 验证更新后的值
        double verifyTime = it->second.globalTime;
        if (std::isnan(verifyTime) || std::isinf(verifyTime) || verifyTime < 0) {
            cout << "[MICROBATCH-TIME-ERROR] Invalid value after update: " << verifyTime << " for mb=" << mb << endl;
            it->second.globalTime = currentTime; // 回滚到原值
        }
    } else {
        cout << "[MICROBATCH-TIME] Global microbatch " << mb 
             << " time unchanged: " << currentTime << " (requested: " << time << ")" << endl;
    }
}

double MicrobatchManager::getMicrobatchGlobalTime(int mb) {
    cout << "[MICROBATCH-TIME-DEBUG] getMicrobatchGlobalTime called for mb=" << mb << endl;
    cout << "[MICROBATCH-TIME-DEBUG] microbatchStates.size=" << microbatchStates.size() << endl;
    
    auto it = microbatchStates.find(mb);
    if (it != microbatchStates.end()) {
        double result = it->second.globalTime;
        cout << "[MICROBATCH-TIME] Global get MicrobatchGlobalTime for mb=" << mb 
             << ", using mb=" << mb << " time: " << result << endl;
        
        // 安全检查返回值
        if (std::isnan(result) || std::isinf(result) || result < 0) {
            cout << "[MICROBATCH-TIME-ERROR] Invalid globalTime detected: " << result << " for mb=" << mb << endl;
            return 0.0;
        }
        
        return result;
    }
    
    // 对于反向microbatch（负数），尝试获取其对应正向microbatch的时间
    if (mb < 0) {
        int positiveMb = -mb;
        auto positiveIt = microbatchStates.find(positiveMb);
        if (positiveIt != microbatchStates.end()) {
            cout << "[MICROBATCH-TIME] Global get MicrobatchGlobalTime for backward mb=" << mb 
                 << ", using forward mb=" << positiveMb << " time: " << positiveIt->second.globalTime << endl;
            return positiveIt->second.globalTime;
        }
    }
    
    // 如果没有找到，且abs(mb) > 1，尝试获取前一个microbatch的时间
    if (abs(mb) > 1) {
        int prevMb = (mb > 0) ? (mb - 1) : (mb + 1);
        auto prevIt = microbatchStates.find(prevMb);
        if (prevIt != microbatchStates.end()) {
            cout << "[MICROBATCH-TIME] Global get MicrobatchGlobalTime for mb=" << mb 
                 << ", using previous mb=" << prevMb << " time: " << prevIt->second.globalTime << endl;
            return prevIt->second.globalTime;
        }
    }
    
    // 如果都没有找到，返回0作为默认值
    cout << "[MICROBATCH-TIME] Global get MicrobatchGlobalTime failed, mb: " << mb << ", returning 0" << endl;
    return 0.0;
}

void MicrobatchManager::printStates() {
    cout << "[MICROBATCH-TIME] Global Microbatch States:" << endl;
    for (const auto& pair : microbatchStates) {
        cout << "  MB=" << pair.first << ", GlobalTime=" << pair.second.globalTime << endl;
    }
    
    cout << "[MICROBATCH-TIME] Rank Microbatch Times:" << endl;
    for (const auto& pair : rankMicrobatchTimes) {
        cout << "  Rank=" << pair.first.first << ", MB=" << pair.first.second 
             << ", Time=" << pair.second << endl;
    }
}

// 新增：更新特定rank的microbatch时间
void MicrobatchManager::updateRankMicrobatchTime(int rankId, int mb, double time) {
    auto key = make_pair(rankId, mb);
    auto it = rankMicrobatchTimes.find(key);
    
    if (it == rankMicrobatchTimes.end()) {
        rankMicrobatchTimes[key] = time;
        cout << "[MICROBATCH-TIME] Created rank microbatch time: Rank=" << rankId 
             << ", MB=" << mb << ", Time=" << time << endl;
    } else {
        double oldTime = it->second;
        double newTime = std::max(oldTime, time);
        it->second = newTime;
        cout << "[MICROBATCH-TIME] Updated rank microbatch time: Rank=" << rankId 
             << ", MB=" << mb << ", " << oldTime << " -> " << newTime << endl;
    }
}

// 新增：获取特定rank的microbatch时间
double MicrobatchManager::getRankMicrobatchTime(int rankId, int mb) {
    auto key = make_pair(rankId, mb);
    auto it = rankMicrobatchTimes.find(key);
    
    if (it != rankMicrobatchTimes.end()) {
        double result = it->second;
        cout << "[MICROBATCH-TIME] Retrieved rank microbatch time: Rank=" << rankId 
             << ", MB=" << mb << ", Time=" << result << endl;
        return result;
    }
    
    // 如果没有找到，返回0作为默认值
    cout << "[MICROBATCH-TIME] Rank microbatch time not found: Rank=" << rankId 
         << ", MB=" << mb << ", returning 0" << endl;
    return 0.0;
}
