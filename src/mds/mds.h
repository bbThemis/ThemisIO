#ifndef _MDS_H_
#define _MDS_H_
#include <vector>
#include <mutex>
#include <tuple>
#include <condition_variable>

#include "lnet.h"

enum Policy_t {
  POFS,
  BSIP,
  TSA,
  ESA,
  TMF,
  RND,
  MBW,
  GIFT
};

using Coupon_t = std::tuple<double, int>;
struct AppCoup_t {
  int app;
  std::vector<Coupon_t> coupons;
};

struct AppData_t {
  int    app_id;
  double bw_issued;
  double bw_redeemed;
};

using AppAlloc_t          = std::tuple<int , double>;
using AppAllocs_t         = std::vector<AppAlloc_t>;
using MapOstToAppAllocs_t = std::vector<AppAllocs_t>;

class LnetMds: public LnetServer
{
  private:
    std::vector<OscInfo*> _oscs;
    std::vector<OstInfo*> _osts;
    std::map<std::string, OstInfo*> _dirToOst;
    std::map<const LnetEntity* , std::vector<ActiveRequest>> _ostReqs;
    std::mutex *_m;
    std::condition_variable *_waitForAllOsts;
    
    bool *_dataIsReady;
    std::mutex *_ost_lock;
    void addOsc(const LSocket &remote, const OscInfo *info);
    void addOst(const LSocket &remote, const OstInfo *info);
    void sendOstsInfo(const LnetEntity *remote);
    void handleFsRequest(const LnetEntity *remote, const LnetMsg *msg);
    const OstInfo *getOstFromPath(const std::string *path) const;
    void addToTimerResponse(const LnetEntity *remote, const LnetMsg *msg);
    void computeBwAllocations(Policy_t , std::vector<std::vector<int>> &,
                              MapOstToAppAllocs_t &);
    void bcastAllocsToOsts(const MapOstToAppAllocs_t &);

    void computeBwAllocationsGIFT(const size_t ,
                                  std::vector<std::vector<int>> &,
                                  MapOstToAppAllocs_t &);
    double getEffectiveSysBw();

  protected:
    virtual void onConnect();
    virtual void onDisconnect(const LnetEntity *);
    virtual void onClientRequest(const LnetEntity *);
    virtual void onRemoteServerRequest(const LnetEntity *);

  public:
    LnetMds(int port, std::mutex *m, std::condition_variable *cv, bool *b, std::mutex *ost_lock);
    ~LnetMds();
    ssize_t sendMsgToOst(const LnetMsg *msg, int id) const;
    ssize_t recvMsgFromOst(LnetMsg *msg, int id) const;
    bool bcastMsgToOsts(const LnetMsg *msg);
    void printStats();

};

class MDS
{
  private:
    // fields
    LnetMds *_mdsNet;
    std::mutex *m;
    std::condition_variable *waitForAllOsts;
    bool dataIsReady;
    std::mutex *_ost_lock;
  public:
    // constructor(s)
    MDS(int port);
    ~MDS();

    // methods
    void eventLoop();
    void startTimer();
    ssize_t sendDataToOst(int idx, const void *buf, size_t len);
    ssize_t recvDataFromOst(int idx, void *buf, size_t len);
    void printMDSInfo();
};


#endif // ifndef _MDS_H_