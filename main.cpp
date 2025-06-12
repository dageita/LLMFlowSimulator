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

/*
inter, intra: Bps
fwdTPSize...: Bytes
*/
extern "C" {
    float pycall_main(int pp, int dp, int tp, double inter, double intra, int microbatches, uint64_t fwdTPSize, uint64_t bwdTPSize, uint64_t fwdPPSize, uint64_t bwdPPSize, uint64_t dpSize) {
        cout << "wxftest " << pp << " " << dp << " " << tp << " " << inter << " " << intra << " " << microbatches << " " << fwdTPSize << " " << bwdTPSize << " " << fwdPPSize << " " << bwdPPSize << " " << dpSize << endl;
        // srand(time(nullptr));
        srand(0);

        // get current time
        auto start = chrono::high_resolution_clock::now();

        cout << "--------------------------" << endl;
        topology = new Topology();

        int totalRanks = pp * dp * tp;
        if (totalRanks <= 8) {
            // 单机内拓扑
            topology->isSingleMachine = true;
            topology->generateSingleMachine(totalRanks, intra); // NVLink capacity
        } else {
            // 跨机拓扑
            topology->isSingleMachine = false;
            topology->generateOneBigSwitch(totalRanks, inter, intra); // Inter/Intra capacity
        }

        // topology->generateFattree(8, 1, 1);
        // topology->generateOneBigSwitch(8, 1); // capacity * factor
        // topology->generateOneBigSwitch(pp * dp * tp, inter * 1000000000 / 8, intra * 1000000000 / 8); // Add NVLink capacity
        // topology->generateSpineleaf(pp * dp * tp, inter, intra);
        // topology->print();
        auto current = chrono::high_resolution_clock::now();
        cout << "Topology generation Execution Time: " << chrono::duration_cast<chrono::milliseconds>(current - start).count() << " ms" << endl;
        start = current;
        cout << "--------------------------" << endl;
        
        workload = new Workload(pp,      // PP
                                dp,      // DP      
                                tp,      // TP 
                                microbatches,      // microbatches   
                                0.00000001,    // fwdCompTime * factor
                                0.00000001,    // bwdCompTime * factor
                                fwdTPSize,    // fwdTPSize
                                bwdTPSize,    // bwdTPSize
                                fwdPPSize,    // fwdPPSize
                                bwdPPSize,    // bwdPPSize
                                dpSize     // dpSize
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
        float globalTime = simulator->py_run();
        current = chrono::high_resolution_clock::now();
        cout << "Simulator run Execution Time: " << chrono::duration_cast<chrono::milliseconds>(current - start).count() << " ms" << endl;
        cout << "--------------------------" << endl;

        return globalTime;  
    }
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
    simulator->run();
    current = chrono::high_resolution_clock::now();
    cout << "Simulator run Execution Time: " << chrono::duration_cast<chrono::milliseconds>(current - start).count() << " ms" << endl;
    cout << "--------------------------" << endl;

    return 0;
}