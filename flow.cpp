#include "common.h"
#include "simulator.h"

Flow::Flow(Connection* connection){
    src = connection->src->host;
    dst = connection->dst->host;
    path = connection->path;
    pathLinks = connection->pathLinks;
}

void Flow::progress(double time){
    if(remainingSize<1e-6) {
        remainingSize = 0;
    }
    else{
        remainingSize -= throughput * time;
    }
}

double Flow::stableTime(){
    return remainingSize/throughput;
}
