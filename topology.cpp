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

void Topology::generateOneBigSwitch(int switch_radix, double capacity, double nvlink_capacity) {
    int numHosts = switch_radix;
    int nodeId = 0;

    // 创建主机节点
    for (int i = 0; i < numHosts; ++i) {
        nodes.push_back(new Node(nodeId++, NodeType::HOST));
    }

    // 创建交换机节点
    nodes.push_back(new Node(nodeId++, NodeType::TOR));

    int linkId = 0;
    // 连接主机到交换机
    for (int i = 0; i < numHosts; ++i) {
        Link* link1 = new Link(linkId++, nodes[i], nodes[numHosts], capacity); // 主机到交换机
        Link* link2 = new Link(linkId++, nodes[numHosts], nodes[i], capacity); // 交换机到主机
        links.push_back(link1);
        links.push_back(link2);
        nodes[i]->links.push_back(link1);
        nodes[numHosts]->links.push_back(link2);
    }

    // 添加 NVLink 连接（每 8 个主机共享一个 NVLink 组）
    for (int i = 0; i < numHosts; i += 8) {
        for (int j = i; j < i + 8 && j < numHosts; ++j) {
            for (int k = j + 1; k < i + 8 && k < numHosts; ++k) {
                // 创建双向 NVLink
                Link* nvlink1 = new Link(linkId++, nodes[j], nodes[k], nvlink_capacity, true); // 标记为 NVLink
                Link* nvlink2 = new Link(linkId++, nodes[k], nodes[j], nvlink_capacity, true); // 反向 NVLink
                links.push_back(nvlink1); // 全局 links 中存储 NVLink
                links.push_back(nvlink2); // 全局 links 中存储反向 NVLink
                nodes[j]->nvlinks.push_back(nvlink1); // 存储到 nvlinks
                nodes[k]->nvlinks.push_back(nvlink2); // 存储到 nvlinks
            }
        }
    }

    // 添加组间 NVLink 连接
    // 计算总组数
    int numGroups = (numHosts + 7) / 8; // 向上取整
    
    // 遍历所有组对
    for (int g1 = 0; g1 < numGroups; ++g1) {
        for (int g2 = g1 + 1; g2 < numGroups; ++g2) {
            // 获取组1的节点范围
            int start1 = g1 * 8;
            int end1 = std::min(start1 + 8, numHosts);
            
            // 获取组2的节点范围
            int start2 = g2 * 8;
            int end2 = std::min(start2 + 8, numHosts);
            
            // 组间两两连接
            for (int i = start1; i < end1; ++i) {
                for (int j = start2; j < end2; ++j) {
                    // 创建双向 NVLink
                    Link* interLink1 = new Link(linkId++, nodes[i], nodes[j], capacity);
                    Link* interLink2 = new Link(linkId++, nodes[j], nodes[i], capacity);
                    links.push_back(interLink1);
                    links.push_back(interLink2);
                    nodes[i]->nvlinks.push_back(interLink1);
                    nodes[j]->nvlinks.push_back(interLink2);
                }
            }
        }
    }

    // 打印节点和连接
    cout << "Nodes:" << endl;
    for (auto node : nodes) {
        cout << "Node ID: " << node->id << ", Type: " << node->type << endl;
    }
    cout << "Links:" << endl;
    for (auto link : links) {
        cout << "Link ID: " << link->id << ", Src: " << link->src->id << ", Dst: " << link->dst->id << ", Capacity: " << link->capacity << ", isNVLink: " << link->isNVLink << endl;
    }
    cout << "wxftest Generated one big switch topology with " << numHosts << " hosts and NVLink capacity: " << nvlink_capacity << endl;
}


void Topology::generateSpineleaf(int switch_radix, double capacity, double nvlink_capacity) {
    int numHosts = switch_radix * switch_radix / 2; // Hosts per leaf switch
    int numLeaf = switch_radix / 2; // Number of leaf switches
    int numSpine = switch_radix / 2; // Number of spine switches

    int nodeId = 0;
    // Build hosts
    for (int i = 0; i < numHosts; ++i) {
        nodes.push_back(new Node(nodeId++, NodeType::HOST));
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
    // Connect hosts to leaf switches
    for (int i = 0; i < numHosts; ++i) {
        int leafIndex = i / (numHosts / numLeaf);
        Link* link1 = new Link(linkId++, nodes[i], nodes[numHosts + leafIndex], capacity);
        Link* link2 = new Link(linkId++, nodes[numHosts + leafIndex], nodes[i], capacity);
        links.push_back(link1);
        links.push_back(link2);
        nodes[i]->links.push_back(link1);
        nodes[numHosts + leafIndex]->links.push_back(link2);
    }

    // Connect leaf switches to spine switches
    for (int i = 0; i < numLeaf; ++i) {
        for (int j = 0; j < numSpine; ++j) {
            Link* link1 = new Link(linkId++, nodes[numHosts + i], nodes[numHosts + numLeaf + j], capacity);
            Link* link2 = new Link(linkId++, nodes[numHosts + numLeaf + j], nodes[numHosts + i], capacity);
            links.push_back(link1);
            links.push_back(link2);
            nodes[numHosts + i]->links.push_back(link1);
            nodes[numHosts + numLeaf + j]->links.push_back(link2);
        }
    }

    // Add NVLink connections (every 8 hosts share one NVLink group)
    for (int i = 0; i < numHosts; i += 8) {
        for (int j = i; j < i + 8 && j < numHosts; ++j) {
            for (int k = j + 1; k < i + 8 && k < numHosts; ++k) {
                Link* nvlink = new Link(linkId++, nodes[j], nodes[k], nvlink_capacity, true); // 标记为 NVLink
                links.push_back(nvlink);
                nodes[j]->nvlinks.push_back(nvlink);
                nodes[k]->nvlinks.push_back(nvlink);
            }
        }
    }
}

void Topology::generateSingleMachine(int numGPUs, double intra_capacity) {
    cout << "Generating single machine topology with " << numGPUs << " GPUs and intra-host capacity: " << intra_capacity << endl;
    int nodeId = 0;

    // 创建 GPU 节点
    for (int i = 0; i < numGPUs; ++i) {
        nodes.push_back(new Node(nodeId++, NodeType::HOST));
    }

    int linkId = 0;
    // 生成双向 NVLink（A→B 和 B→A 各存一个 Link 对象）
    for (int i = 0; i < numGPUs; ++i) {
        for (int j = i + 1; j < numGPUs; ++j) {
            Node* src = nodes[i];
            Node* dst = nodes[j];

            // 创建 A→B
            Link* link_ab = new Link(linkId++, src, dst, intra_capacity, true);
            links.push_back(link_ab);
            src->nvlinks.push_back(link_ab);  // 仅 src 存储 A→B

            // 创建 B→A
            Link* link_ba = new Link(linkId++, dst, src, intra_capacity, true);
            links.push_back(link_ba);
            dst->nvlinks.push_back(link_ba);  // 仅 dst 存储 B→A
        }
    }
}

vector<Node*> Topology::ECMP(Node* src, Node* dst, double capacity, double nvlink_capacity) {
    cout << "wxftest ECMP src " << src->id << " dst " << dst->id << " capacity: " << capacity << endl;
    vector<vector<Node*>> allPaths;
    queue<vector<Node*>> q;
    unordered_set<Node*> visited;

    if (isSingleMachine) {
        cerr << "Error: ECMP is not applicable for single-machine topology." << endl;
        return {};
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

        // Explore NVLink neighbors first
        for (auto link : current->nvlinks) {
            if (link->capacity >= capacity || (link->isNVLink && link->capacity >= nvlink_capacity)) {
                Node* neighbor = link->dst;
                if (visited.find(neighbor) == visited.end()) {
                    vector<Node*> newPath = path;
                    newPath.push_back(neighbor);
                    q.push(newPath);
                }
            }
        }

        // Explore remaining neighbors (network links)
        for (auto link : current->links) {
            if (link->capacity >= capacity || (link->isNVLink && link->capacity >= nvlink_capacity)) {
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

    // Randomly select one path from allPaths
    int randomIndex = rand() % allPaths.size();
    return allPaths[randomIndex];
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
