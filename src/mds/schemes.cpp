#include <algorithm>
#include <cassert>
#include <iostream>
#include <vector>
#include <set>

#include "lnet.h"
#include "mds.h"
#include "ClpSimplex.hpp"

// Required for all policies
#define ALLOC_XTRA_BW   false
#define ALLOC_X_BW_EQ   true


// Required for GIFT policy
#define GIFT_SYS_RDMP_T 0.8
#define GIFT_RDMP_RT_TH 0.8
#define GIFT_B_THRESOLD 0.1
#define GIFT_COUP_BW_SZ 1e-6
#define GIFT_WINDOW_LEN 1000000
#define GIFT_RESET_TIME 86400


using namespace std;

// Required for GIFT policy
double sys_coup_issu = 0.0;
double sys_coup_rdmp = 0.0;

vector<AppData_t> appDatabase;

vector<int>    numCouponsIssued;
vector<double> valCouponsIssued;

// Track the effective storage system utilization for all policies
vector<double> runEffStorageSysUtil;


void LnetMds::computeBwAllocationsGIFT(const size_t NUM_OSTS,
                                       const std::vector<std::vector<int>> &reqs,
                                       MapOstToAppAllocs_t &allocs) {
    // Create a flat vector
    vector<int> flat;
    for (size_t i = 0; i < NUM_OSTS; i++) {
        for (auto req : reqs[i]) {
        flat.push_back(req);
        }
    }

    // Determine the number of applications doing I/O
    set<int> s = set<int>(flat.begin(), flat.end());
    vector<int> apps(s.begin(), s.end());
    const int numApps = (int)apps.size();

    // Row Order matrix
    const bool cOrd = false;

    // Row Indices
    int* rInd = NULL;
    rInd = new int[NUM_OSTS * numApps];
    for (size_t i = 0; i < NUM_OSTS; i++) {
        for (int j = 0; j < numApps; j++) {
        rInd[(i * numApps) + j] = i;
        }
    }

    // Col Indices
    int* cInd = NULL;
    cInd = new int[NUM_OSTS * numApps];
    for (size_t i = 0; i < NUM_OSTS; i++) {
        for (int j = 0; j < numApps; j++) {
        cInd[(i * numApps) + j] = j;
        }
    }

    // Constraints matrix
    double* ele = NULL;
    ele = new double[NUM_OSTS * numApps];
    memset(ele, 0, (NUM_OSTS * numApps) * sizeof(double));
    for (size_t i = 0; i < NUM_OSTS; i++) {
        for (auto req : reqs[i]) {
        int app = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
        ele[(i * numApps) + app] += 1;
        }
    }
    // Number of elements in the constraints matrix
    CoinBigIndex numels = NUM_OSTS * numApps;

    // Make a matrix of constraints
    const CoinPackedMatrix matrix(cOrd, rInd, cInd, ele, numels);

    // Lower allocation bound for each app
    double* alcs = NULL;
    alcs = new double[numApps];	
    fill_n(alcs, numApps, numeric_limits<double>::infinity());
    int* osts = NULL;
    osts = new int[numApps];
    memset(osts, 0, numApps * sizeof(int));

    vector<double> apTh; // = {true, false, true, false, true};
    bool found = false;
    for (auto app : apps) {
        found = false;
        for (auto base : appDatabase) {
        if (base.app_id == app) {
            if ((base.bw_redeemed / base.bw_issued) >= GIFT_RDMP_RT_TH) {
            apTh.push_back(base.bw_redeemed - (base.bw_issued * GIFT_RDMP_RT_TH));
            } else {
            apTh.push_back(0.0);
            }
            found = true;
            break;
        }
        }

        if (!found) {
        appDatabase.push_back(AppData_t());
        appDatabase.back().app_id = app;
        appDatabase.back().bw_issued = GIFT_COUP_BW_SZ * GIFT_WINDOW_LEN;
        appDatabase.back().bw_redeemed = GIFT_COUP_BW_SZ * GIFT_WINDOW_LEN;
        apTh.push_back(appDatabase.back().bw_redeemed - (appDatabase.back().bw_issued * GIFT_RDMP_RT_TH));
        }
    }
    for (size_t i = 0; i < NUM_OSTS; i++) {
        double alc = 1.0 / (double)reqs[i].size();
        for (auto req : reqs[i]) {
        int app = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
        alcs[app] = min(alcs[app], alc);
        osts[app] += 1;
        }
    }
    double efbw[NUM_OSTS];
    memset(efbw, 0.0, NUM_OSTS * sizeof(double));
    for (size_t i = 0; i < NUM_OSTS; i++) {
        for (auto req : reqs[i]) {
        int app = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
        efbw[i] += alcs[app];
        }
        //cout << efbw[i] << endl;
    }

    int    numCoups = 0;
    double valCoups = 0.0;

    vector<double> bwRd;
    vector<int> apps_cpy = apps;
    random_shuffle(apps_cpy.begin(), apps_cpy.end());
    for (auto app : apps_cpy) {
        double bwRq = 0.0;
        for (auto base : appDatabase) {
        if (base.app_id == app) {
            bwRq = base.bw_issued - base.bw_redeemed;
            break;
        }
        }

        if (bwRq == 0.0) {
        bwRd.push_back(0.0);
        continue;
        }

        double bwAv = numeric_limits<double>::infinity();
        for (size_t i = 0; i < NUM_OSTS; i++) {
        int numReqs = 0;
        for (auto req : reqs[i]) {
            if (req == app) {
            numReqs += 1;
            }
        }
        if (numReqs == 0) {
            continue;
        }
        if (efbw[i] == 1.0) {
            bwAv = 0.0;
            break;
        }
        bwAv = min(bwAv, ((1.0 - efbw[i])/(double)numReqs));
        }

        if (bwAv == 0.0) {
        bwRd.push_back(0.0);
        continue;
        }

        int index = 0;
        for (auto a : apps) {
        if (a == app) {
            break;
        }
        index++;
        }

        double bwGv = min(bwRq, bwAv * osts[index]);
        for (auto& base : appDatabase) {
        if (base.app_id == app) {
            for (size_t i = 0; i < NUM_OSTS; i++) {
            int numReqs = 0;
            for (auto req : reqs[i]) {
                if (req == app) {
                numReqs += 1;
                }
            }
            if (numReqs == 0) {
                continue;
            }
            efbw[i] -= ((bwGv / (double)osts[index]) * (double)numReqs);
            }
            base.bw_redeemed += (double)((int)(bwGv / GIFT_COUP_BW_SZ)) * GIFT_COUP_BW_SZ;
            break;
        }
        }
        valCoups -= (double)((int)(bwGv / GIFT_COUP_BW_SZ)) * GIFT_COUP_BW_SZ;
        sys_coup_rdmp += (double)((int)(bwGv / GIFT_COUP_BW_SZ)) * GIFT_COUP_BW_SZ;
        bwRd.push_back(bwGv/(double)osts[index]);
    }

    double* collb = NULL;
    collb = new double[numApps];
    double* fsbw = NULL;
    fsbw = new double[numApps];
    fill_n(collb, numApps, numeric_limits<double>::infinity());
    fill_n(fsbw, numApps, numeric_limits<double>::infinity());

    for (int app = 0; app < numApps; app++) {
        if (((sys_coup_issu == 0.0) or ((sys_coup_issu != 0.0) && ((sys_coup_rdmp/sys_coup_issu) > GIFT_SYS_RDMP_T))) && (apTh[app] > 0.0)) {
        if (apTh[app] > ((alcs[app] * GIFT_B_THRESOLD) * osts[app])) {
            collb[app] = (alcs[app] * (1.0 - GIFT_B_THRESOLD)) + bwRd[app];
        } else {
            collb[app] = alcs[app] - (apTh[app] / (double)osts[app]) + bwRd[app];
        }
        } else {
        collb[app] = alcs[app] + bwRd[app];
        }
        fsbw[app] = alcs[app] + bwRd[app];
    }

    /*
        for (size_t i = 0; i < NUM_OSTS; i++) {
        double alloc = 1.0 / (double)reqs[i].size();
        for (auto req : reqs[i]) {
        int app = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
        if (apTh[app]) {
        collb[app] = min(collb[app], (alloc * (1.0 - GIFT_B_THRESOLD)) + bwRd[app]);
        } else {
        collb[app] = min(collb[app], alloc + bwRd[app]);
        }
        fsbw[app] = min(fsbw[app], alloc + bwRd[app]);
        }
        }*/

    // Upper allocation bound for each app
    double* colub = NULL;
    colub = new double[numApps];
    fill_n(colub, numApps, 1);

    // The cooefficients of the minimum objective function
    double* obj = NULL;
    obj = new double[numApps];
    memset(obj, 0, numApps * sizeof(double));
    for (auto f : flat) {
        int app = (int)(find(apps.begin(), apps.end(), f) - apps.begin());
        obj[app] -= 1;
    }

    // The upper and lower bounds of each OST
    double rowlb[NUM_OSTS], rowub[NUM_OSTS];
    memset(rowlb, 0, NUM_OSTS * sizeof(double));
    fill_n(rowub, NUM_OSTS, 1);

    ClpSimplex model;
    model.loadProblem(matrix, collb, colub, obj, rowlb, rowub, NULL);
    model.primal();
    double* cVal = model.primalColumnSolution();

    bool* done = NULL;
    done = new bool[numApps];
    fill_n(done, numApps, false);

    // Determine allocation of the bandwidth
    for (size_t i = 0; i < NUM_OSTS; i++) {
        double usedBw = 0.0;
        for (auto req : reqs[i]) {
        int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
        usedBw += cVal[ind];
        }

        double xtraBw = 0.0;
        if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == true)) {
        xtraBw = (1.0 - usedBw) / (double)reqs[i].size();
        }

        AppAllocs_t alloc;

        for (auto req : reqs[i]) {
        int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
        if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == false)) {
            xtraBw = (1.0 - usedBw) * (cVal[ind] / usedBw);
        }
        alloc.push_back(make_tuple(req, cVal[ind] + xtraBw));

        if ((!done[ind]) && (cVal[ind] < fsbw[ind])) {
            done[ind] = true;
            for (auto& base : appDatabase) {
            if (base.app_id == req) {
                base.bw_redeemed -= (double)((int)(((fsbw[ind] - cVal[ind]) * osts[ind]) / GIFT_COUP_BW_SZ)) * GIFT_COUP_BW_SZ;
                numCoups += (int)(((fsbw[ind] - cVal[ind]) * osts[ind]) / GIFT_COUP_BW_SZ);
                valCoups += (double)((int)(((fsbw[ind] - cVal[ind]) * osts[ind]) / GIFT_COUP_BW_SZ)) * GIFT_COUP_BW_SZ;
                sys_coup_issu += (double)((int)(((fsbw[ind] - cVal[ind]) * osts[ind]) / GIFT_COUP_BW_SZ)) * GIFT_COUP_BW_SZ;
                //cout << "APP: " << base.app_id << endl;
                //cout << "ZOM: " << fsbw[ind] << endl;
                //cout << "RED: " << cVal[ind] << endl;
                //cout << "GOD: " << osts[ind] << endl;
                break;
            }
            }
        }
        }
        allocs.push_back(alloc);
    }

    numCouponsIssued.push_back(numCouponsIssued.back() + numCoups);
    valCouponsIssued.push_back(valCouponsIssued.back() + valCoups);

    // Determine effective storage system utilization	
    if (model.getObjValue() < 0) {
        runEffStorageSysUtil.push_back(-1 * model.getObjValue());
    }

    delete [] rInd;
    delete [] cInd;
    delete [] ele;
    delete [] alcs;
    delete [] osts;
    delete [] collb;
    delete [] fsbw;
    delete [] colub;
    delete [] obj;
    delete [] done;


}

void LnetMds::printStats()
{
#if 0
  for (auto entry : freqDatabase) {
     cout << "App " << entry.app << " appears " << entry.num << " times." << endl;
  }
#endif

  for (auto base : appDatabase) {
    cout << base.app_id << endl;
    cout << base.bw_issued << endl;
    cout << base.bw_redeemed << endl;
  }

  for (auto num : numCouponsIssued) {
    cout << num << endl;
  }

  for (auto val : valCouponsIssued) {
    cout << val << endl;
  }

  double avgEffStorageSysUtil = 0.0;
  for (auto bw : runEffStorageSysUtil) {
    avgEffStorageSysUtil += bw;
  }
  avgEffStorageSysUtil = 100.0 * avgEffStorageSysUtil / (double) runEffStorageSysUtil.size();
  avgEffStorageSysUtil /= (double) this->_osts.size();

  cout << "Effective System Util: " << avgEffStorageSysUtil << endl;
  cout << "Effective System B/w: " << this->getEffectiveSysBw() << endl;
}