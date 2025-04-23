#include "topology.h"
#include "workload.h"
#include "simulator.h"
#include <chrono>
#include <iostream>

using namespace std;

Topology* topology = nullptr;
Workload* workload = nullptr;
Simulator* simulator = nullptr;

int main(int argc, char** argv){
    
    // srand(time(nullptr));
    srand(0);

    // get current time
    auto start = chrono::high_resolution_clock::now();

    cout << "--------------------------" << endl;
    topology = new Topology();
    // topology->generateFattree(8, 1, 1);
    // topology->generateOneBigSwitch(8, 1); // capacity * factor
    topology->generateOneBigSwitch(1 * 1 * 4, 400.0 * 1000000000 / 8, 800.0 * 1000000000 / 8); // Add NVLink capacity
    // topology->print();
    auto current = chrono::high_resolution_clock::now();
    cout << "Topology generation Execution Time: " << chrono::duration_cast<chrono::milliseconds>(current - start).count() << " ms" << endl;
    start = current;
    cout << "--------------------------" << endl;
    
    workload = new Workload(1,      // PP
                            1,      // DP      
                            4,      // TP 
                            64,      // microbatches   
                            0.06385143254309411,    // fwdCompTime * factor
                            0.10335142783278387,    // bwdCompTime * factor
                            6442450944,    // fwdTPSize
                            6442450944,    // bwdTPSize
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
    workload->routing();
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