#include "topology.h"
#include "workload.h"
#include "common.h"

#include <vector>
#include <iostream>
#include <queue>
#include <unordered_set>
#include <cstdlib>

using namespace std;

void Node::print() {
    switch (type) {
        case HOST: cout << "Host"; break;
        case TOR: cout << "TOR"; break;
        case AGG: cout << "AGG"; break;
        case CORE: cout << "CORE"; break;
    }
    cout << " " << id << endl;
    cout << "Links: ";
    for (auto link : links) {
        cout << link->id << " ";
    }
    cout << endl;
    if (rank) {
        cout << "Rank ID: " << rank->id << endl;
    } else {
        cout << "No rank assigned" << endl;
    }
}


void Link::print() {
    cout << "Link " << id << ": " << src->id << " -> " << dst->id << ", capacity: " << capacity << endl;
}


void Topology::generateFattree(int switch_radix, int pods, double capacity){
    int numHosts = pods * ( switch_radix / 2 ) * ( switch_radix / 2 );
    int numTOR = pods * ( switch_radix / 2 );
    int numAGG = pods * ( switch_radix / 2 );
    int numCore = ( switch_radix * switch_radix ) / 4; 

    int nodeId = 0;
    // build hosts
    for(int i = 0; i < numHosts; ++i) {
        nodes.push_back(new Node(nodeId++, NodeType::HOST));
    }

    // build TOR
    for(int i = 0; i < numTOR; ++i) {
        nodes.push_back(new Node(nodeId++, NodeType::TOR));
    }

    // build AGG
    for(int i = 0; i < numAGG; ++i) {
        nodes.push_back(new Node(nodeId++, NodeType::AGG));
    }

    // build Core
    for(int i = 0; i < numCore; ++i) {
        nodes.push_back(new Node(nodeId++, NodeType::CORE));
    }

    int linkId = 0;
    // connect host-TOR
    for(int i = 0; i < numHosts; ++i) {
        int torIndex = i / (switch_radix / 2);
        Link* link1 = new Link(linkId++, nodes[i], nodes[torIndex + numHosts], capacity);
        Link* link2 = new Link(linkId++, nodes[torIndex + numHosts], nodes[i], capacity);
        links.push_back(link1);
        links.push_back(link2);
        nodes[i]->links.push_back(link1);
        nodes[torIndex + numHosts]->links.push_back(link2);
    }

    // connect TOR-AGG
    for(int i = 0; i < numTOR; ++i) {
        int pod = i / (switch_radix / 2);
        for(int j = 0; j < (switch_radix / 2); ++j) {
            int aggIndex = pod * (switch_radix / 2) + j;
            Link* link1 = new Link(linkId++, nodes[i + numHosts], nodes[aggIndex + numHosts + numTOR], capacity);
            Link* link2 = new Link(linkId++, nodes[aggIndex + numHosts + numTOR], nodes[i + numHosts], capacity);
            links.push_back(link1);
            links.push_back(link2);
            nodes[i + numHosts]->links.push_back(link1);
            nodes[aggIndex + numHosts + numTOR]->links.push_back(link2);
        }
    }

    // connect AGG-Core
    for(int i = 0; i < numAGG; ++i) {
        for(int j = 0; j < (switch_radix / 2); ++j) {
            int coreIndex = j;
            Link* link1 = new Link(linkId++, nodes[i + numHosts + numTOR], nodes[coreIndex + numHosts + numTOR + numAGG], capacity);
            Link* link2 = new Link(linkId++, nodes[coreIndex + numHosts + numTOR + numAGG], nodes[i + numHosts + numTOR], capacity);
            links.push_back(link1);
            links.push_back(link2);
            nodes[i + numHosts + numTOR]->links.push_back(link1);
            nodes[coreIndex + numHosts + numTOR + numAGG]->links.push_back(link2);
        }
    }

}

void Topology::generateOneBigSwitch(int totalGPUs, double interHostCapacity, double intraHostCapacity) {
    int numGPUsPerHost = 8;
    int numHosts = (totalGPUs + numGPUsPerHost - 1) / numGPUsPerHost; // 向上取整，确保有足够的主机
    int nodeId = 0;

    cout << "wxftest generateOneBigSwitch: totalGPUs=" << totalGPUs << ", numHosts=" << numHosts << ", numGPUsPerHost=" << numGPUsPerHost << endl;

    // 创建HOST节点
    vector<Node*> hosts;
    for (int i = 0; i < numHosts; ++i) {
        Node* host = new Node(nodeId++, NodeType::HOST);
        nodes.push_back(host);
        hosts.push_back(host);
    }

    // 创建GPU节点
    vector<Node*> gpus;
    for (int i = 0; i < totalGPUs; ++i) {
        Node* gpu = new Node(nodeId++, NodeType::GPU);
        nodes.push_back(gpu);
        gpus.push_back(gpu);
    }

    // 创建交换机节点
    Node* switchNode = new Node(nodeId++, NodeType::TOR);
    nodes.push_back(switchNode);

    int linkId = 0;
    
    // 连接HOST到交换机（节点间连接）
    for (int i = 0; i < numHosts; ++i) {
        Link* link1 = new Link(linkId++, hosts[i], switchNode, interHostCapacity); // HOST到交换机
        Link* link2 = new Link(linkId++, switchNode, hosts[i], interHostCapacity); // 交换机到HOST
        links.push_back(link1);
        links.push_back(link2);
        hosts[i]->links.push_back(link1);
        switchNode->links.push_back(link2);
    }

    // 连接GPU到对应的HOST（节点内连接）
    for (int i = 0; i < numHosts; ++i) {
        for (int j = 0; j < numGPUsPerHost; ++j) {
            int gpuIndex = i * numGPUsPerHost + j;
            if (gpuIndex < totalGPUs) { // 确保不超出实际GPU数量
                Link* link1 = new Link(linkId++, gpus[gpuIndex], hosts[i], intraHostCapacity, true); // GPU到HOST
                Link* link2 = new Link(linkId++, hosts[i], gpus[gpuIndex], intraHostCapacity, true); // HOST到GPU
                links.push_back(link1);
                links.push_back(link2);
                gpus[gpuIndex]->links.push_back(link1);
                hosts[i]->links.push_back(link2);
            }
        }
    }

    // 添加GPU节点间的NVLink连接（每8个GPU在一个HOST内）
    for (int i = 0; i < numHosts; ++i) {
        int startGPU = i * numGPUsPerHost;
        int endGPU = std::min(startGPU + numGPUsPerHost, totalGPUs); // 确保不超出实际GPU数量
        
        // 在同一个HOST内的GPU之间创建NVLink连接
        for (int j = startGPU; j < endGPU; ++j) {
            for (int k = j + 1; k < endGPU; ++k) {
                // 创建双向 NVLink
                Link* nvlink1 = new Link(linkId++, gpus[j], gpus[k], intraHostCapacity, true);
                Link* nvlink2 = new Link(linkId++, gpus[k], gpus[j], intraHostCapacity, true);
                links.push_back(nvlink1);
                links.push_back(nvlink2);
                gpus[j]->nvlinks.push_back(nvlink1);
                gpus[k]->nvlinks.push_back(nvlink2);
            }
        }
    }

    // 添加HOST间的连接（通过交换机实现）
    // 这里通过交换机已经实现了HOST间的连接，不需要额外的HOST到HOST直接连接

    // 打印节点和连接
    cout << "Nodes:" << endl;
    for (auto node : nodes) {
        cout << "Node ID: " << node->id << ", Type: " << node->type << endl;
    }
    cout << "Links:" << endl;
    for (auto link : links) {
        cout << "Link ID: " << link->id << ", Src: " << link->src->id << ", Dst: " << link->dst->id << ", Capacity: " << link->capacity << ", isNVLink: " << link->isNVLink << endl;
    }
    cout << "wxftest Generated one big switch topology with " << numHosts << " hosts, " << totalGPUs << " GPUs, inter-host capacity: " << interHostCapacity << ", intra-host capacity: " << intraHostCapacity << endl;
}


void Topology::generateSpineleaf(int totalGPUs, double interHostCapacity, double intraHostCapacity) {
    // 为了保持合理的网络规模，我们基于totalGPUs计算网络参数
    // 每个HOST包含8个GPU，每个Leaf交换机连接多个HOST
    int numGPUsPerHost = 8;
    int numHosts = (totalGPUs + numGPUsPerHost - 1) / numGPUsPerHost; // 向上取整
    int numLeaf = (numHosts + 3) / 4; // 每个Leaf交换机连接4个HOST
    int numSpine = numLeaf; // Spine和Leaf数量相等，形成全连接

    int nodeId = 0;
    
    // 创建HOST节点
    vector<Node*> hosts;
    for (int i = 0; i < numHosts; ++i) {
        Node* host = new Node(nodeId++, NodeType::HOST);
        nodes.push_back(host);
        hosts.push_back(host);
    }

    // 创建GPU节点
    vector<Node*> gpus;
    for (int i = 0; i < totalGPUs; ++i) {
        Node* gpu = new Node(nodeId++, NodeType::GPU);
        nodes.push_back(gpu);
        gpus.push_back(gpu);
    }

    // Build leaf switches
    for (int i = 0; i < numLeaf; ++i) {
        nodes.push_back(new Node(nodeId++, NodeType::TOR));
    }

    // Build spine switches
    for (int i = 0; i < numSpine; ++i) {
        nodes.push_back(new Node(nodeId++, NodeType::CORE));
    }

    int linkId = 0;
    
    // 连接GPU到对应的HOST（节点内连接）
    for (int i = 0; i < numHosts; ++i) {
        for (int j = 0; j < numGPUsPerHost; ++j) {
            int gpuIndex = i * numGPUsPerHost + j;
            if (gpuIndex < totalGPUs) { // 确保不超出实际GPU数量
                Link* link1 = new Link(linkId++, gpus[gpuIndex], hosts[i], intraHostCapacity, true); // GPU到HOST
                Link* link2 = new Link(linkId++, hosts[i], gpus[gpuIndex], intraHostCapacity, true); // HOST到GPU
                links.push_back(link1);
                links.push_back(link2);
                gpus[gpuIndex]->links.push_back(link1);
                hosts[i]->links.push_back(link2);
            }
        }
    }
    
    // Connect hosts to leaf switches
    for (int i = 0; i < numHosts; ++i) {
        int leafIndex = i / 4; // 每个Leaf交换机连接4个HOST
        if (leafIndex >= numLeaf) leafIndex = numLeaf - 1; // 防止越界
        Link* link1 = new Link(linkId++, hosts[i], nodes[numHosts + totalGPUs + leafIndex], interHostCapacity);
        Link* link2 = new Link(linkId++, nodes[numHosts + totalGPUs + leafIndex], hosts[i], interHostCapacity);
        links.push_back(link1);
        links.push_back(link2);
        hosts[i]->links.push_back(link1);
        nodes[numHosts + totalGPUs + leafIndex]->links.push_back(link2);
    }

    // Connect leaf switches to spine switches
    for (int i = 0; i < numLeaf; ++i) {
        for (int j = 0; j < numSpine; ++j) {
            Link* link1 = new Link(linkId++, nodes[numHosts + totalGPUs + i], nodes[numHosts + totalGPUs + numLeaf + j], interHostCapacity);
            Link* link2 = new Link(linkId++, nodes[numHosts + totalGPUs + numLeaf + j], nodes[numHosts + totalGPUs + i], interHostCapacity);
            links.push_back(link1);
            links.push_back(link2);
            nodes[numHosts + totalGPUs + i]->links.push_back(link1);
            nodes[numHosts + totalGPUs + numLeaf + j]->links.push_back(link2);
        }
    }

    // 添加GPU节点间的NVLink连接（每8个GPU在一个HOST内）
    for (int i = 0; i < numHosts; ++i) {
        int startGPU = i * numGPUsPerHost;
        int endGPU = std::min(startGPU + numGPUsPerHost, totalGPUs); // 确保不超出实际GPU数量
        
        // 在同一个HOST内的GPU之间创建NVLink连接
        for (int j = startGPU; j < endGPU; ++j) {
            for (int k = j + 1; k < endGPU; ++k) {
                // 创建双向 NVLink
                Link* nvlink1 = new Link(linkId++, gpus[j], gpus[k], intraHostCapacity, true);
                Link* nvlink2 = new Link(linkId++, gpus[k], gpus[j], intraHostCapacity, true);
                links.push_back(nvlink1);
                links.push_back(nvlink2);
                gpus[j]->nvlinks.push_back(nvlink1);
                gpus[k]->nvlinks.push_back(nvlink2);
            }
        }
    }
    
    cout << "wxftest Generated spine-leaf topology with " << numHosts << " hosts, " << totalGPUs << " GPUs, " << numLeaf << " leaf switches, " << numSpine << " spine switches, inter-host capacity: " << interHostCapacity << ", intra-host capacity: " << intraHostCapacity << endl;
}

void Topology::generateSingleMachine(int numGPUs, double intra_capacity) {
    cout << "Generating single machine topology with " << numGPUs << " GPUs and intra-host capacity: " << intra_capacity << endl;

    int nodeId = 0;
    // 创建 NVLink Switch
    Node* nvlinkSwitch = new Node(nodeId++, NodeType::NVLINK_SWITCH);
    nodes.push_back(nvlinkSwitch);

    // 创建 GPU 节点
    std::vector<Node*> gpus;
    for (int i = 0; i < numGPUs; ++i) {
        Node* gpu = new Node(nodeId++, NodeType::GPU);
        nodes.push_back(gpu);
        gpus.push_back(gpu);
    }

    int linkId = 0;
    // GPU <-> NVLink Switch
    for (auto gpu : gpus) {
        Link* link = new Link(linkId++, gpu, nvlinkSwitch, intra_capacity, true);
        links.push_back(link);
        gpu->nvlinks.push_back(link);
        nvlinkSwitch->nvlinks.push_back(link);
        // 可加反向链路
        Link* linkBack = new Link(linkId++, nvlinkSwitch, gpu, intra_capacity, true);
        links.push_back(linkBack);
        nvlinkSwitch->nvlinks.push_back(linkBack);
        gpu->nvlinks.push_back(linkBack);
    }
}

vector<Node*> Topology::ECMP(Node* src, Node* dst, double capacity, double nvlink_capacity) {
    cout << "wxftest ECMP src " << src->id << " dst " << dst->id << " capacity: " << capacity << endl;
    vector<vector<Node*>> allPaths;
    queue<vector<Node*>> q;
    unordered_set<Node*> visited;

    // 优先检查GPU到GPU的直接NVLink连接
    if (src->type == NodeType::GPU && dst->type == NodeType::GPU) {
        cout << "wxftest Checking GPU " << src->id << " to GPU " << dst->id << " connections" << endl;
        cout << "wxftest Source GPU " << src->id << " has " << src->nvlinks.size() << " nvlinks" << endl;
        
        // 检查是否有直接的NVLink连接
        for (auto link : src->nvlinks) {
            cout << "wxftest Source GPU " << src->id << " nvlink to node " << link->dst->id << " (type " << link->dst->type << "), isNVLink=" << link->isNVLink << ", capacity=" << link->capacity << endl;
            if (link->dst == dst && link->isNVLink && link->capacity >= nvlink_capacity) {
                cout << "wxftest Found direct NVLink connection between GPU " << src->id << " and GPU " << dst->id << endl;
                return {src, dst};
            }
        }
        
        // 检查是否通过同一个NVLINK_SWITCH连接
        for (auto link : src->nvlinks) {
            if (link->dst->type == NodeType::NVLINK_SWITCH) {
                for (auto link2 : dst->nvlinks) {
                    if (link2->dst == link->dst) {
                        // 只考虑scale-up链路
                        cout << "wxftest Found NVLink switch connection between GPU " << src->id << " and GPU " << dst->id << endl;
                        return {src, link->dst, dst};
                    }
                }
            }
        }
        
        cout << "wxftest No direct NVLink connection found, will use BFS search" << endl;
    }

    // Start BFS from the source node
    q.push({src});

    while (!q.empty()) {
        vector<Node*> path = q.front();
        q.pop();
        Node* current = path.back();

        // If we reach the destination, store the path
        if (current == dst) {
            allPaths.push_back(path);
            continue;
        }

        // Mark the current node as visited for this path
        visited.insert(current);

        // Explore NVLink neighbors first (优先使用NVLink)
        for (auto link : current->nvlinks) {
            if (link->isNVLink && link->capacity >= nvlink_capacity) {
                Node* neighbor = link->dst;
                if (visited.find(neighbor) == visited.end()) {
                    vector<Node*> newPath = path;
                    newPath.push_back(neighbor);
                    q.push(newPath);
                }
            }
        }

        // Explore regular links (包括非NVLink的连接)
        for (auto link : current->links) {
            if (link->capacity >= capacity) {
                Node* neighbor = link->dst;
                if (visited.find(neighbor) == visited.end()) {
                    vector<Node*> newPath = path;
                    newPath.push_back(neighbor);
                    q.push(newPath);
                }
            }
        }
    }

    // If no paths are found, return an empty vector
    if (allPaths.empty()) {
        cerr << "Error: No valid path found between nodes " << src->id << " and " << dst->id << " with capacity " << capacity << endl;
        return {};
    }

    // 优先选择最短路径（GPU直接连接优先）
    vector<Node*> bestPath = allPaths[0];
    for (const auto& path : allPaths) {
        if (path.size() < bestPath.size()) {
            bestPath = path;
        }
    }
    
    cout << "wxftest Selected path with " << bestPath.size() << " hops: ";
    for (auto node : bestPath) {
        cout << node->id << " ";
    }
    cout << endl;
    
    return bestPath;
}

void Topology::print() {
    cout << "Topology:" << endl;
    cout << "Nodes:" << endl;
    for (auto node : nodes) {
        node->print();
    }
    cout << "Links:" << endl;
    for (auto link : links) {
        link->print();
    }
}
