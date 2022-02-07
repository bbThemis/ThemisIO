#ifndef _OST_H_
#define _OST_H_

#include <vector>
#include <unordered_map>
#include <mutex>
#include "lnet.h"
// #include "dnet_ost.h"

class LnetOst: public LnetServer
{
  private:
    const OstInfo *_info;
    MdsInfo *_mdsInfo;
    LnetClient *_toMds;
    // std::vector<OscInfo*> _oscs;
    // DatanetOst *_dnet;
    std::unordered_map<ActiveRequest, int, hash_activeReq>& activeReqs;
    std::mutex& reqLock;
    std::unordered_map<int, double>& appAlloc; 
    std::mutex& allocLock;
    // void addOsc(const LSocket &, const OscInfo *);
    void respondToMdsTimer(const LnetEntity *);
    void handleFsRequest(const LnetEntity *, const LnetMsg *);
    void getActiveRequests(std::vector<ActiveRequest>& reqs);
    void setAllocations(const LnetEntity *remote, const LnetMsg *msg);
  protected:
    virtual void onConnect();
    virtual void onDisconnect(const LnetEntity *);
    virtual void onClientRequest(const LnetEntity *);
    virtual void onRemoteServerRequest(const LnetEntity *);

  public:
    LnetOst(const LSockAddr &, int , const OstInfo */*, DatanetOst * */,
    std::unordered_map<ActiveRequest, int, hash_activeReq>& activeReqs, std::mutex& reqLock,
    std::unordered_map<int, double>& appAlloc, std::mutex& allocLock);
    ~LnetOst();
    bool pubOstInfoToMds();
    ssize_t sendMsgToMds(const LnetMsg *);
    ssize_t recvMsgFromMds(LnetMsg *);

  // friend class OstOps;
};

class OST
{
  private:
    // fields
    OstInfo *_info;
    LnetOst *_ostNet;
    // DatanetOst *_dataNet;
    // std::vector<OscInfo*> _oscs;
    bool _mdsConnected;
    std::unordered_map<ActiveRequest, int, hash_activeReq>& activeReqs;
    std::mutex& reqLock;
    std::unordered_map<int, double>& appAlloc; 
    std::mutex& allocLock;
  public:
    // constructor(s)
    // OST(const LSockAddr &addr, int port, int id, const char *name, int lnetport/*, int dataport*/);
    OST(const LSockAddr &addr, int port, int id, const char *name, int lnetport/*, int dataport*/,
        std::unordered_map<ActiveRequest, int, hash_activeReq>& activeReqs, std::mutex& reqLock,
        std::unordered_map<int, double>& appAlloc, std::mutex& allocLock);
    ~OST();

    // methods
    void eventLoop();
    ssize_t sendDataToMds(const void *buf, size_t len);
    ssize_t recvDataFromMds(void *buf, size_t len);
};

#endif // ifndef _OST_H_