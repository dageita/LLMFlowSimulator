#ifndef TOPOLOGY_H
#define TOPOLOGY_H
#include "common.h"
#include "simulator.h"
#include "workload.h"

#include <vector>
#include <iostream>
#include <set>

using namespace std;

class Node;
class Link;
class Topology;
class Rank;
class Flow;

class Node {
public:
    int id;
    NodeType type;
    vector<Link*> links;  // directed links from Node
    vector<Link*> nvlinks;
    vector<Link*> pcielinks;
    Node(int id, NodeType type) : id(id), type(type) { rank = nullptr; }

    // workload
    Rank* rank;
    vector<Rank*> ranks; // Add this line to track ranks on the node

    // simulator related
    double throughput;

    void print();
};

class Link {
public:
    int id;
    Node* src;
    Node* dst;
    double capacity;
    bool isNVLink; // Add this flag to indicate if the link is an NVLink
    Link(int id, Node* src, Node* dst, double capacity = 0.0, bool isNVLink = false)
        : id(id), src(src), dst(dst), capacity(capacity), isNVLink(isNVLink) {}

    // simulator related 
    double throughput;
    set<Flow*> flows; // flows using this link

    void print() ;
};

class Topology {
public:
    vector<Node*> nodes;
    vector<Link*> links;
    bool isSingleMachine = false; // 标志是否为单机网络
    Topology() {}
    ~Topology() {
        for (auto node : nodes) {
            delete node;
        }
        for (auto link : links) {
            delete link;
        }
    }

    void generateFattree(int switch_radix, int pods, double capacity);
    void generateSpineleaf(int switch_radix, double capacity, double nvlink_capacity);
    void generateOneBigSwitch(int switch_radix, double capacity, double nvlink_capacity);
    void generateSingleMachine(int numGPUs, double nvlink_capacity);
    // void routing();

    vector<Node*> ECMP(Node* src, Node* dst, double capacity, double nvlink_capacity); // Updated to include capacity

    void print();
};



#endif // TOPOLOGY_H