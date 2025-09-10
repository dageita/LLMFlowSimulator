#include "topology.h"
#include "workload.h"
#include "simulator.h"
#include <chrono>
#include <iostream>
#include <cstdint>
#include <cstring>

using namespace std;

Topology* topology = nullptr;
Workload* workload = nullptr;
Simulator* simulator = nullptr;

// 设置基本结果参数的辅助函数
void setBasicResults(const SimResult& resultTime, 
                    double* globalTime, 
                    double* batchTpFwComm, double* batchTpBwComm, double* batchPpFwComm, double* batchPpBwComm, double* batchDpComm,
                    double* batchTpComm, double* batchPpComm,
                    double* microbatchTpFwComm, double* microbatchTpBwComm, double* microbatchPpFwComm, double* microbatchPpBwComm,
                    double* totalCommTime) {
    
    // 设置全局时间
    *globalTime = resultTime.globalTime;
    
    // 设置新的通信时间统计字段
    *batchTpFwComm = resultTime.batchTpFwCommTime;
    *batchTpBwComm = resultTime.batchTpBwCommTime;
    *batchPpFwComm = resultTime.batchPpFwCommTime;
    *batchPpBwComm = resultTime.batchPpBwCommTime;
    *batchDpComm = resultTime.batchDpCommTime;
    *batchTpComm = resultTime.batchTpCommTime;
    *batchPpComm = resultTime.batchPpCommTime;
    
    *microbatchTpFwComm = resultTime.microbatchTpFwCommTime;
    *microbatchTpBwComm = resultTime.microbatchTpBwCommTime;
    *microbatchPpFwComm = resultTime.microbatchPpFwCommTime;
    *microbatchPpBwComm = resultTime.microbatchPpBwCommTime;
    
    // 设置总通信时间
    *totalCommTime = resultTime.totalCommTime;
}

// 处理时间线事件的辅助函数
void processTimelineEvents(const SimResult& resultTime,
                          int* timelineEventCount, int* timelineRanks, char* timelineEventTypes[],
                          int* timelineMicrobatches, double* timelineStartTimes, double* timelineEndTimes) {
    if (timelineEventCount && timelineRanks && timelineEventTypes && timelineMicrobatches && timelineStartTimes && timelineEndTimes) {
        // 先检查指针是否指向有效内存区域
        if (timelineEventCount == nullptr) {
            cout << "[TIMELINE-ERROR] timelineEventCount is NULL, cannot proceed" << endl;
            return;
        }
        
        // 尝试安全的内存访问
        try {
            // 使用 memcpy 安全地写入新值
            int newValue = resultTime.timelineEvents.size();
            memcpy(timelineEventCount, &newValue, sizeof(int));
            
        } catch (const std::exception& e) {
            cout << "[TIMELINE-ERROR] Exception while setting timelineEventCount: " << e.what() << endl;
            cout << "[TIMELINE-ERROR] Skipping timeline event processing due to memory access error" << endl;
            return;
        } catch (...) {
            cout << "[TIMELINE-ERROR] Unknown exception while setting timelineEventCount" << endl;
            cout << "[TIMELINE-ERROR] Skipping timeline event processing due to memory access error" << endl;
            return;
        }
        
        for (size_t i = 0; i < resultTime.timelineEvents.size(); ++i) {
            const auto& event = resultTime.timelineEvents[i];
            
            // 安全地复制数据
            try {
                timelineRanks[i] = event.rank;
                timelineMicrobatches[i] = event.microbatch;
                timelineStartTimes[i] = event.startTime;
                timelineEndTimes[i] = event.endTime;
                
                // 复制事件类型字符串
                if (timelineEventTypes[i]) {
                    try {
                        size_t srcLen = event.eventType.length();
                        size_t maxLen = 63;  // 保留一个字节给 null 终止符
                        
                        if (srcLen > maxLen) {
                            strncpy(timelineEventTypes[i], event.eventType.c_str(), maxLen);
                            timelineEventTypes[i][maxLen] = '\0';
                        } else {
                            strcpy(timelineEventTypes[i], event.eventType.c_str());
                        }
                    } catch (const std::exception& e) {
                        cout << "[TIMELINE-ERROR] Exception while copying string: " << e.what() << endl;
                    } catch (...) {
                        cout << "[TIMELINE-ERROR] Unknown exception while copying string" << endl;
                    }
                } else {
                    cout << "[TIMELINE-WARNING] timelineEventTypes[" << i << "] is NULL, skipping string copy" << endl;
                }
                
            } catch (const std::exception& e) {
                cout << "[TIMELINE-ERROR] Exception while processing event " << i << ": " << e.what() << endl;
            } catch (...) {
                cout << "[TIMELINE-ERROR] Unknown exception while processing event " << i << endl;
            }
        }
        
        cout << "[TIMELINE] Successfully returned " << *timelineEventCount << " timeline events to Python" << endl;
    } else {
        cout << "[TIMELINE-WARNING] Some timeline parameters are NULL, skipping timeline event processing" << endl;
    }
}

void printSingleTopologyLinks(const Topology* topology) {
    cout << "Single machine topology links (unidirectional):" << endl;
    for (const auto& link : topology->links) {
        cout << "NVLink " << link->id << ": "
             << link->src->id << " -> " << link->dst->id
             << ", capacity: " << link->capacity << endl;
    }
}

/*
inter, intra: Bps
fwdTPSize...: Bytes
*/

// microbatches: GAS
extern "C" void pycall_main(int pp, int dp, int tp, double inter, double intra, double fwdCompTime, double bwdCompTime, int microbatches, const char* topology_type, uint64_t fwdTPSize, uint64_t bwdTPSize, uint64_t fwdPPSize, uint64_t bwdPPSize, uint64_t dpSize, int* timelineEventCount, int* timelineRanks, char* timelineEventTypes[], int* timelineMicrobatches, double* timelineStartTimes, double* timelineEndTimes, double* globalTime, double* batchTpFwComm, double* batchTpBwComm, double* batchPpFwComm, double* batchPpBwComm, double* batchDpComm, double* batchTpComm, double* batchPpComm, double* microbatchTpFwComm, double* microbatchTpBwComm, double* microbatchPpFwComm, double* microbatchPpBwComm, double* totalCommTime) {
    // inter, intra 单位 Bps

    cout << "wxftest " << pp << " " << dp << " " << tp << " " << inter << " " << intra << " " << fwdCompTime << " " << bwdCompTime << " " << topology_type << " " << microbatches << " " << fwdTPSize << " " << bwdTPSize << " " << fwdPPSize << " " << bwdPPSize << " " << dpSize << endl;
    // srand(time(nullptr));
    srand(0);

    // get current time
    auto start = chrono::high_resolution_clock::now();

    cout << "--------------------------" << endl;
    topology = new Topology();

    int totalRanks = pp * dp * tp;

    std::string topo_str = topology_type ? std::string(topology_type) : "";

    // 单机拓扑
    if (topo_str == "Single machine") {
        topology->isSingleMachine = true;
        topology->generateSingleMachine(totalRanks, intra); // intra-host capacity
        printSingleTopologyLinks(topology);

    } else if (topo_str == "One big switch") { // 单大交换机拓扑
        topology->isSingleMachine = false;
        topology->generateOneBigSwitch(totalRanks, inter, intra);
    } else if (topo_str == "Spine-leaf") { // Spine leaf 拓扑
        topology->isSingleMachine = false;
        topology->generateSpineleaf(totalRanks, inter, intra);
    } else {
        cerr << "Unknown topology type: " << topo_str << ", fallback to Single machine." << endl;
        topology->isSingleMachine = true;
        topology->generateSingleMachine(totalRanks, intra);
    }

    auto current = chrono::high_resolution_clock::now();
    cout << "Topology generation Execution Time: " << chrono::duration_cast<chrono::milliseconds>(current - start).count() << " ms" << endl;
    start = current;
    cout << "--------------------------" << endl;
    
    workload = new Workload(pp,      // PP
                            dp,      // DP      
                            tp,      // TP 
                            microbatches,      // microbatches   
                            fwdCompTime,    // fwdCompTime * factor
                            bwdCompTime,    // bwdCompTime * factor
                            fwdTPSize,    // fwdTPSize
                            bwdTPSize,    // bwdTPSize
                            fwdPPSize,    // fwdPPSize
                            bwdPPSize,    // bwdPPSize
                            dpSize     // dpSize
                        );
    workload->topology = topology;
    workload->configureParallelism();   // 1F1B now
    cout << "wxftest " << 1 << endl;
    workload->placement();
    cout << "wxftest " << 2 << endl;
    workload->routing(inter, intra);
    cout << "wxftest " << 3 << endl;
    // workload->print();
    // return 0;
    current = chrono::high_resolution_clock::now();
    cout << "Workload generation Execution Time: " << chrono::duration_cast<chrono::milliseconds>(current - start).count() << " ms" << endl;
    start = current;
    cout << "--------------------------" << endl;
    simulator = new Simulator();
    simulator->workload = workload;
    simulator->topology = topology;
    simulator->initialize();
    // simulator->print();    
    current = chrono::high_resolution_clock::now();
    cout << "Simulator initialization Execution Time: " << chrono::duration_cast<chrono::milliseconds>(current - start).count() << " ms" << endl;
    start = current;
    cout << "--------------------------" << endl;
    SimResult resultTime = simulator->py_run();

    current = chrono::high_resolution_clock::now();
    cout << "Simulator run Execution Time: " << chrono::duration_cast<chrono::milliseconds>(current - start).count() << " ms" << endl;
    cout << "--------------------------" << endl;

    // 设置基本结果参数
    setBasicResults(resultTime, globalTime,
                   batchTpFwComm, batchTpBwComm, batchPpFwComm, batchPpBwComm, batchDpComm,
                   batchTpComm, batchPpComm,
                   microbatchTpFwComm, microbatchTpBwComm, microbatchPpFwComm, microbatchPpBwComm,
                   totalCommTime);
    
    // 处理时间线事件数据
    processTimelineEvents(resultTime, timelineEventCount, timelineRanks, timelineEventTypes,
                         timelineMicrobatches, timelineStartTimes, timelineEndTimes);
    
    cout << "------------Success--------------" << endl;
}

int main(int argc, char** argv){
    
    // srand(time(nullptr));
    srand(0);

    // get current time
    auto start = chrono::high_resolution_clock::now();

    cout << "--------------------------" << endl;
    topology = new Topology();
    // topology->generateFattree(8, 1, 1);
    // topology->generateOneBigSwitch(8, 1); // capacity * factor
    // topology->generateOneBigSwitch(1 * 1 * 8, 400.0 * 1000000000 / 8, 400.0 * 1000000000 / 8); // Add NVLink capacity
    // 这里 400.0单位为Gbps，故 400.0 * 1000000000 / 8 的单位为GBps
    topology->generateSpineleaf(1 * 1 * 8, 400.0 * 1000000000 / 8, 400.0 * 1000000000 / 8);
    // topology->print();
    auto current = chrono::high_resolution_clock::now();
    cout << "Topology generation Execution Time: " << chrono::duration_cast<chrono::milliseconds>(current - start).count() << " ms" << endl;
    start = current;
    cout << "--------------------------" << endl;
    
    workload = new Workload(1,      // PP
                            1,      // DP      
                            8,      // TP 
                            32,      // microbatches   
                            0.00000001,    // fwdCompTime * factor
                            0.00000001,    // bwdCompTime * factor
                            2818572288,    // fwdTPSize
                            2818572288,    // bwdTPSize
                            0,    // fwdPPSize
                            0,    // bwdPPSize
                            0     // dpSize
                        );
    workload->topology = topology;
    workload->configureParallelism();   // 1F1B now
    workload->placement();
    workload->routing(400.0 * 1000000000 / 8, 400.0 * 1000000000 / 8);
    // workload->print();
    // return 0;
    current = chrono::high_resolution_clock::now();
    cout << "Workload generation Execution Time: " << chrono::duration_cast<chrono::milliseconds>(current - start).count() << " ms" << endl;
    start = current;
    cout << "--------------------------" << endl;
    simulator = new Simulator();
    simulator->workload = workload;
    simulator->topology = topology;
    simulator->initialize();
    // simulator->print();    
    current = chrono::high_resolution_clock::now();
    cout << "Simulator initialization Execution Time: " << chrono::duration_cast<chrono::milliseconds>(current - start).count() << " ms" << endl;
    start = current;
    cout << "--------------------------" << endl;
    simulator->py_run();
    current = chrono::high_resolution_clock::now();
    cout << "Simulator run Execution Time: " << chrono::duration_cast<chrono::milliseconds>(current - start).count() << " ms" << endl;
    cout << "--------------------------" << endl;

    return 0;
}