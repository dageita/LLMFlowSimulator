#include "workload.h"
#include "common.h"

#include <iostream>
#include <vector>
#include <map>
#include <tuple>
#include <algorithm>


using namespace std;


void Rank::print(){
    cout << "Rank ID: " << id << ", TP: " << tp << ", DP: " << dp << ", PP: " << pp;
    cout << " TP Fwd Group ID: " << (tpFwdGroup ? to_string(tpFwdGroup->id) : "None") ;
    cout << ", TP Bwd Group ID: " << (tpBwdGroup ? to_string(tpBwdGroup->id) : "None") ;
    cout << ", DP Group ID: " << (dpGroup ? to_string(dpGroup->id) : "None") ;
    cout << ", PP Fwd Group ID: " << (ppFwdGroup ? to_string(ppFwdGroup->id) : "None") ;
    cout << ", PP Bwd Group ID: " << (ppBwdGroup ? to_string(ppBwdGroup->id) : "None") ;    
    cout << ", Host ID: " << (host ? to_string(host->id) : "None") << endl;
}

void Connection::print(){
    cout << "Connection from Rank " << src->id << " to Rank " << dst->id;
    cout << ", Path: ";
    for(auto node : path) {
        cout << node->id << " ";
    }
    cout << ", Path Links: ";
    for(auto link : pathLinks) {
        cout << link->id << " ";
    }
    cout << endl;
}

void Group::print(){
    cout << "Group ID: " << id << ", Type: ";
    switch(type) {
        case TP: cout << "TP"; break;
        case PP: cout << "PP"; break;
        case DP: cout << "DP"; break;
    }
    cout << endl;
    cout << "Ranks in group: " << endl;
    for(auto rank : ranks) {
        rank->print();
    }
    cout << "Connections: ";
    for(auto conn : connections) {
        conn->print();
    }
}

void Workload::print(){
    cout << "Workload Configuration:" << endl;
    cout << "PP: " << PP << ", DP: " << DP << ", TP: " << TP << endl;
    cout << "Microbatches: " << microbatches << endl;
    cout << "Forward Computation Time: " << fwdCompTime << endl;
    cout << "Backward Computation Time: " << bwdCompTime << endl;
    cout << "Forward TP Size: " << fwdTPSize << endl;
    cout << "Backward TP Size: " << bwdTPSize << endl;
    cout << "Forward PP Size: " << fwdPPSize << endl;
    cout << "Backward PP Size: " << bwdPPSize << endl;
    cout << "DP Size: " << dpSize << endl;

    cout << "--------------------------------" << endl;
    cout << "Ranks:" << endl;
    for(auto rank : ranks) {
        cout << "--------------------------------" << endl;
        rank->print();
    }
    cout << "--------------------------------" << endl;
    
    cout << "Groups:" << endl;
    for(auto group : groups) {
        cout << "--------------------------------" << endl;
        group->print();
    }
}

Rank* Workload::getRankByPP(int pp) const {
    for (Rank* rank : ranks) {
        if (rank->pp == pp) {
            return rank;
        }
    }
    throw std::runtime_error("Rank with pp=" + std::to_string(pp) + " not found");
}

Workload::Workload(int PP, int DP, int TP, int microbatches, 
    double fwdCompTime, double bwdCompTime, double fwdTPSize, double bwdTPSize, 
    double fwdPPSize, double bwdPPSize, double dpSize) :   
    PP(PP), DP(DP), TP(TP), microbatches(microbatches), fwdCompTime(fwdCompTime), bwdCompTime(bwdCompTime),
    fwdTPSize(fwdTPSize), bwdTPSize(bwdTPSize), fwdPPSize(fwdPPSize), bwdPPSize(bwdPPSize), dpSize(dpSize) {
    
    // create ranks
    map<tuple<int, int, int>, Rank*> rankMap; // PP, DP, TP

    int rankId = 0;
    for(int i = 0; i < PP; ++i) {
        for(int j = 0; j < DP; ++j) {
            for(int k = 0; k < TP; ++k) {
                Rank* rank = new Rank(rankId++, i, j, k);
                ranks.push_back(rank);
                rankMap[make_tuple(i, j, k)] = rank; // PP, DP, TP
            }
        }
    }
    
    // create groups

    map<tuple<int, int, int>, Group*> groupMap; // PP, DP, TP

    int groupId = 0;
    for(int i = 0; i < PP; ++i) { 
        for(int j = 0; j < DP; ++j) {   // TP
            Group* group = new Group(groupId++, GroupType::TP, i, j, -1);
            groups.push_back(group);
            groupMap[make_tuple(i, j, -1)] = group; // TP group
        }
    }

    for(int i = 0; i < PP; ++i) { 
        for(int j = 0; j < TP; ++j) {   // DP
            Group* group = new Group(groupId++, GroupType::DP, i, -1, j);
            groups.push_back(group);
            groupMap[make_tuple(i, -1, j)] = group; // DP group
        }
    }

    // associate ranks and groups
    for(auto rank : ranks) {
        int pp = rank->pp;
        int dp = rank->dp;
        int tp = rank->tp;

        // TP group (both forward and backward use the same group for now)
        Group* tpGroup = groupMap[make_tuple(pp, dp, -1)];
        rank->tpFwdGroup = tpGroup;  // TP前向组
        rank->tpBwdGroup = tpGroup;  // TP后向组（暂时使用同一个组）
        tpGroup->ranks.push_back(rank);

        // DP group
        Group* dpGroup = groupMap[make_tuple(pp, -1, tp)];
        rank->dpGroup = dpGroup;
        dpGroup->ranks.push_back(rank);
    }
    
    // enumerate nodes 

    for(int j = 0; j < DP; ++j) {               // DP
        for(int i = 0; i < TP; ++i) {           // TP
            Rank* first = rankMap[make_tuple(0, j, i)];
            Rank* last = rankMap[make_tuple(PP-1, j, i)];
            first->ppBwdGroup = nullptr;  // Changed from == to =
            last->ppFwdGroup = nullptr;
            for(int k = 0; k < PP - 1; ++k) {  // PP
                // cout << "PP, TP, DP; k: " << k << ", i: " << i << ", j: " << j << endl;
                Group *fwdGroup = new Group(groupId++, GroupType::PP, k, i, j);
                Group *bwdGroup = new Group(groupId++, GroupType::PP, k+1, i, j);
                Rank* r1 = rankMap[make_tuple(k, j, i)];
                Rank* r2 = rankMap[make_tuple(k+1, j, i)];
                fwdGroup->ranks.push_back(r1);
                fwdGroup->ranks.push_back(r2);
                bwdGroup->ranks.push_back(r2);
                bwdGroup->ranks.push_back(r1);
                r1->ppFwdGroup = fwdGroup;
                r2->ppBwdGroup = bwdGroup;
                groups.push_back(fwdGroup);
                groups.push_back(bwdGroup);
            }
        }
    }
    
    // create connections

    for(auto group : groups) {
        group->createConnections();
    }

}


void Group::createConnections() {
    // TP or DP
    if(type == TP || type == DP) {
        sort(ranks.begin(), ranks.end(), [](Rank* a, Rank* b) {
            return a->id < b->id;
        });
        for(int i = 0; i < ranks.size(); ++i) {
            Rank* src = ranks[i];
            Rank* dst = ranks[(i + 1) % ranks.size()]; // circular connection
            Connection* conn = new Connection(src, dst);
            connections.push_back(conn);
        }
    } else if(type == PP) { // PP
        Rank* r1 = ranks[0];
        Rank* r2 = ranks[1];
        Connection* conn = new Connection(r1, r2);
        connections.push_back(conn);
    }
}


void Workload::configureParallelism(){
    // generate a graph
    int stages = PP;
    int microbatches = this->microbatches;
    int graph[stages][2 * (microbatches + stages - 1)];
    for(int i = 0; i < stages; ++i) {
        for(int j = 0; j < 2 * (microbatches + stages - 1); ++j) {
            graph[i][j] = 0;
        }
    }

    // configure backward [1, mb]
    for(int mb = 1; mb <= microbatches; ++mb) {
        int row = stages - 1;;
        int col = stages + 2 * (mb-1);
        for(int i=0; i < stages; ++i) {
            graph[row - i][col + i] = -mb;
        }
    }

    // configure forward [s+1, mb]
    for(int mb = stages + 1; mb <= microbatches; ++mb) {
        int row = 0;
        int col = stages * 2 + 2 * (mb - stages - 1);
        for(int i=0; i < stages; ++i) {
            graph[row + i][col + i] = mb;
        }
    }

    // configure forward [1, min(s, mb)]
    for(int row = 0; row < stages; ++row) {
        int col = row; 
        int mb = 1;
        while(mb <= min(stages, microbatches)) {
            if(graph[row][col] == 0) {
                graph[row][col] = mb;
                col++;
                mb++;
            }
            else{
                col++;
            }
        }
    }

    // configure rank
    int s, i, j;
    for(s= 0; s<stages; ++s) {
        for(i = s; i< 2*(stages+microbatches-1); i++){
            for(j=i+1; j<2*(microbatches+stages-1); j++){
                if(graph[s][j]!=0) break;
            }
            if(j<2*(microbatches+stages-1)){
                int from = graph[s][i];
                int to = graph[s][j];   
                nextMicrobatch[make_tuple(s, from)] = to;
                cout << "[GRAPH-CONFIG] Stage " << s << ": " << from << " -> " << to << endl;
                i=j-1;                
            }
            else{
                break;
            }
        }
    }
    
    // 打印完整的图配置用于调试
    cout << "[GRAPH-CONFIG] Complete nextMicrobatch configuration:" << endl;
    for (const auto& pair : nextMicrobatch) {
        int stage = std::get<0>(pair.first);
        int from = std::get<1>(pair.first);
        int to = pair.second;
        cout << "[GRAPH-CONFIG]   Stage " << stage << ": " << from << " -> " << to << endl;
    }
}


void Workload::placement(){
    // sort ranks
    sort(ranks.begin(), ranks.end(), [](Rank* a, Rank* b) {
        return a->id < b->id;
    });

    // 找到所有 GPU 节点（优先使用GPU）
    vector<Node*> gpus;
    for(auto node : topology->nodes) {
        if(node->type == NodeType::GPU) {
            gpus.push_back(node);
        }
    }
    sort(gpus.begin(), gpus.end(), [](Node* a, Node* b) {
        return a->id < b->id;
    });

    // 如果GPU节点不够，则使用HOST节点作为备选
    vector<Node*> hosts;
    if (gpus.size() < ranks.size()) {
        for(auto node : topology->nodes) {
            if(node->type == NodeType::HOST) {
                hosts.push_back(node);
            }
        }
        sort(hosts.begin(), hosts.end(), [](Node* a, Node* b) {
            return a->id < b->id;
        });
    }

    // 映射 Rank 到计算节点（优先GPU，不足时使用HOST）
    for(int i = 0; i < ranks.size(); ++i) {
        Rank* rank = ranks[i];
        Node* computeNode;
        
        if (i < gpus.size()) {
            // 优先使用GPU节点
            computeNode = gpus[i];
        } else {
            // GPU节点不足时，使用HOST节点
            int hostIndex = (i - gpus.size()) % hosts.size();
            computeNode = hosts[hostIndex];
        }
        
        rank->host = computeNode;
        computeNode->rank = rank;
        computeNode->ranks.push_back(rank); // Track all ranks on the same node
        
        cout << "Placement: Rank " << rank->id << " -> Node " << computeNode->id 
             << " (Type: " << (computeNode->type == NodeType::GPU ? "GPU" : "HOST") << ")" << endl;
    }
}

void Workload::routing(double inter, double intra) {
    // iterate on connections
    for (auto group : groups) {
        for (auto conn : group->connections) {
            Node* src = conn->src->host;
            Node* dst = conn->dst->host;

            // 判断是否在同一个 scale-up domain（如同一个 NVLINK_SWITCH 下）
            if (src->type == NodeType::GPU && dst->type == NodeType::GPU) {
                bool sameNvSwitch = false;
                Node* sharedSwitch = nullptr;
                
                // 查找共享的NVLink交换机
                for (auto link : src->nvlinks) {
                    if (link->dst->type == NodeType::NVLINK_SWITCH) {
                        for (auto link2 : dst->nvlinks) {
                            if (link2->dst == link->dst) {
                                sameNvSwitch = true;
                                sharedSwitch = link->dst;
                                break;
                            }
                        }
                        if (sameNvSwitch) break;
                    }
                }

                if (sameNvSwitch) {
                    // 精确选择连接src和dst的链接
                    conn->path = {src, sharedSwitch, dst};
                    for (auto link : src->nvlinks) {
                        if (link->dst == sharedSwitch) {
                            conn->pathLinks.push_back(link);
                            break;
                        }
                    }
                    for (auto link : dst->nvlinks) {
                        if (link->dst == sharedSwitch) {
                            conn->pathLinks.push_back(link);
                            break;
                        }
                    }
                } else {
                    vector<Node*> path = topology->ECMP(src, dst, inter, intra);
                    conn->path = path;
                    for (int i = 0; i < path.size() - 1; ++i) {
                        Node* current = path[i];
                        Node* next = path[i + 1];
                        
                        // 首先检查NVLink连接
                        bool foundLink = false;
                        for (auto link : current->nvlinks) {
                            if (link->dst == next) {
                                conn->pathLinks.push_back(link);
                                foundLink = true;
                                break;
                            }
                        }
                        
                        // 如果NVLink中没有找到，再检查普通links
                        if (!foundLink) {
                            for (auto link : current->links) {
                                if (link->dst == next) {
                                    conn->pathLinks.push_back(link);
                                    break;
                                }
                            }
                        }
                    }
                }
            } else {
                // 处理非GPU到GPU或其他情况
                vector<Node*> path = topology->ECMP(src, dst, inter, intra);
                conn->path = path;
                for (int i = 0; i < path.size() - 1; ++i) {
                    Node* current = path[i];
                    Node* next = path[i + 1];
                    
                    // 首先检查NVLink连接
                    bool foundLink = false;
                    for (auto link : current->nvlinks) {
                        if (link->dst == next) {
                            conn->pathLinks.push_back(link);
                            foundLink = true;
                            break;
                        }
                    }
                    
                    // 如果NVLink中没有找到，再检查普通links
                    if (!foundLink) {
                        for (auto link : current->links) {
                            if (link->dst == next) {
                                conn->pathLinks.push_back(link);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}


