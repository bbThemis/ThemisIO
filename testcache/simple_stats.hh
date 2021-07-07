#ifndef __SIMPLE_STATS_H__
#define __SIMPLE_STATS_H__

#include <cmath>
#include <vector>
#include <algorithm>
#include <mpi.h>

/* Consume a sequence of values and be able to produce simple statistics on it.
   min, max, avg, standard deviation
*/
class Stats {
public:
  Stats(bool supports_median = false)
    : store_all(supports_median), is_sorted(false),
      count(0), sum(0), sum2(0),
      min(NAN), max(NAN) {}

  void add(double value) {
    is_sorted = false;
    if (store_all) {
      values.push_back(value);
    }
    count++;
    if (count == 1) {
      min = max = value;
    } else {
      if (value < min) min = value;
      if (value > max) max = value;
    }
    sum += value;
    sum2 += value * value;
  }

  int getCount() {return count;}
  double getMinimum() {return min;}
  double getMaximum() {return max;}
  double getSum() {return sum;}
  double getSumOfSquares() {return sum2;}
  double getAverage() {return count==0 ? NAN : sum/count;}
  
  double getStdDev() {
    if (count < 2) return 0;
    double variance = (sum2 - (sum*sum)/count) / (count - 1);
    return sqrt(variance);
  }

  double getMedian() {
    if (!store_all || count == 0) return NAN;
    
    if (!is_sorted) {
      std::sort(values.begin(), values.end());
      is_sorted = true;
    }

    if (count & 1) {
      return values[count/2];
    } else {
      return (values[count/2-1] + values[count/2]) / 2;
    }
  }

  void set(int c, double s, double s2, double mn, double mx) {
    is_sorted = false;
    store_all = false;
    values.clear();
    count = c;
    sum = s;
    sum2 = s2;
    min = mn;
    max = mx;
  }

protected:
  bool store_all, is_sorted;
  int count;
  double sum, sum2, min, max;
  std::vector<double> values;
};
  

#endif // __SIMPLE_STATS_H__
