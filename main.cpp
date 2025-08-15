#include "topology.h"
#include "workload.h"
#include "simulator.h"
#include <chrono>
#include <iostream>
#include <cstdint>

using namespace std;

Topology* topology = nullptr;
Workload* workload = nullptr;
Simulator* simulator = nullptr;

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


extern "C" void pycall_main(int pp, int dp, int tp, double inter, double intra, double fwdCompTime, double bwdCompTime, int microbatches, const char* topology_type, uint64_t fwdTPSize, uint64_t bwdTPSize, uint64_t fwdPPSize, uint64_t bwdPPSize, uint64_t dpSize, double* globalTime, double* tpComm, double* tpFwComm, double* tpBwComm, double* ppComm, double* ppFwComm, double* ppBwComm, double* dpComm, double* totalComm) {
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

    *globalTime = resultTime.globalTime;
    *tpComm = resultTime.pureTpCommTime;
    *tpFwComm = resultTime.pureTpFwCommTime;
    *tpBwComm = resultTime.pureTpBwCommTime;
    *ppComm = resultTime.purePpCommTime;
    *ppFwComm = resultTime.purePpFwCommTime;
    *ppBwComm = resultTime.purePpBwCommTime;
    *dpComm = resultTime.pureDpCommTime;
    *totalComm = resultTime.pureTotalCommTime;
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
    // workload = new Workload(2,      // PP
    //                         2,      // DP      
    //                         2,      // TP 
    //                         5,      // microbatches   
    //                         0.1,    // fwdCompTime * factor
    //                         0.1,    // bwdCompTime * factor
    //                         1.0,    // fwdTPSize
    //                         1.0,    // bwdTPSize
    //                         1.0,    // fwdPPSize
    //                         1.0,    // bwdPPSize
    //                         1.0     // dpSize
    //                     );
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