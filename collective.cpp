#include "common.h"
#include "simulator.h"

extern Workload* workload;

Collective::Collective(Group* group, int microbatch, int accumulatedSize) : 
        group(group), microbatch(microbatch), accumulatedSize(accumulatedSize) { 

    if (group->type == GroupType::TP && group->ranks.size() <= 1) {
    // 不生成任何 TP 流
        return;
    }

    accumulatedInvocations = 1;
    this->flows.clear();
    if (group->type == GroupType::TP || group->type == GroupType::DP) {
        for (auto connection : group->connections) {
            Flow* flow = new Flow(connection);
            if (group->type == GroupType::TP) {
                flow->remainingSize = microbatch > 0 ? workload->fwdTPSize : workload->bwdTPSize;
            } else {
                flow->remainingSize = workload->dpSize;
            }
            // TP 需要 all-to-all 放大，DP 只有一次，无放大
            double factor = (group->type == GroupType::TP) 
                                ? 2.0 * (group->ranks.size()-1) / group->ranks.size() 
                                : 1.0;
            flow->remainingSize *= factor;
            flow->collective = this;
            flows.push_back(flow);
        }
    }
    else { // PP, generate one connection
        Flow* flow = new Flow(group->connections[0]);
        flow->remainingSize = microbatch > 0 ? workload->fwdPPSize : workload->bwdPPSize;
        this->flows.push_back(flow);
        flow->collective = this;
    }
}

void Collective::printStates(){
    cout << "Collective:" ;
    cout << " Group: " << group->id <<", Group Size: " << group->ranks.size() ;
    cout << " Microbatch: " << microbatch ;
    cout << " Accumulated invocations: " << accumulatedInvocations ;
    cout << endl;
    for(auto flow : flows) {
        cout << "Flow: " ;
        cout << flow->src->id << "->" << flow->dst->id ;
        cout << ", Remaining size: " << flow->remainingSize ;
        cout << ", Throughput: " << flow->throughput ;
        cout << endl;
    }
}

double Collective::stableTime(){
    double time = numeric_limits<double>::infinity();
    for(auto flow : flows){
        double t = flow->stableTime();
        if(t < time) time = t;
    }
    return time;
}

void Collective::progress(double time){
    for(auto flow : flows){
        flow->progress(time);
    }
}
