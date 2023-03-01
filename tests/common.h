#include <ctime>
#include <cmath>
#include <algorithm>
#include <cinttypes>
#include <cstdint>
#include <string>
#include <vector>
#include <cfloat>


// return a timer in seconds
double getSeconds();

// return a timer in nanoseconds
uint64_t getNanos();

// Sleeps the given number of seconds
void sleepSeconds(double seconds);


/* Consume a sequence of values and be able to produce simple statistics on it.
   min, max, avg, standard deviation
*/
class Stats {
public:
  Stats(bool supports_median = false)
    : store_all(supports_median), is_sorted(false) {
    reset();
  }

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

  int getCount() const {return count;}
  double getMinimum() const {return min;}
  double getMaximum() const {return max;}
  double getSum() const {return sum;}
  double getSumOfSquares() const {return sum2;}
  double getAverage() const {return count==0 ? NAN : sum/count;}
  
  double getStdDev() const {
    if (count < 2) return 0;
    double variance = (sum2 - (sum*sum)/count) / (count - 1);
    return sqrt(variance);
  }

  void sortValues() const {
    if (!is_sorted) {
      std::sort(values.begin(), values.end());
      is_sorted = true;
    }
  }

  double getMedian() const {
    if (!store_all || count == 0) return NAN;

    sortValues();

    if (count & 1) {
      return values[count/2];
    } else {
      return (values[count/2-1] + values[count/2]) / 2;
    }
  }

  /* Find a percentile threshold. pct will be clamped to [0..100].
     getPercentile(0) returns the smallest value, and getPercentile(100)
     returns the highest value).
     Interpolate when the threshold is between two values. */
  double getPercentile(double percentile) const {
    sortValues();
    
    if (percentile <= 0) {
      return values.front();
    } else if (percentile >= 100) {
      return values.back();
    }
    
    double index_real = percentile / 100 * (values.size() - 1);
    size_t above = ceil(index_real);
    size_t below = floor(index_real);
  
    // interpolate between "above" and "below"
    double p = index_real - below;
    return (1-p) * values[below] + p * values[above];
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

  double operator [] (size_t i) const {return values[i];}

  void reset() {
    count = 0;
    sum = sum2 = 0;
    min = DBL_MAX;
    max = -DBL_MAX;
    is_sorted = false;
    values.clear();
  }

protected:
  bool store_all;
  mutable bool is_sorted;
  int count;
  double sum, sum2, min, max;
  mutable std::vector<double> values;
};


class Timer : public Stats {
public:
  Timer(bool supports_median = false) : Stats(supports_median) {
    start();
  }

  void start() {
    // start_time = getSeconds();
    start_nanos = getNanos();
  }

  double stop() {
    // double stop_time = getSeconds();
    int64_t stop_nanos = getNanos();
    double elapsed = (stop_nanos - start_nanos) * 1e-9;
    // printf("  %ld .. %ld  elapsed=%.9f\n", (long)start_nanos, (long)stop_nanos, elapsed);
    add(elapsed);
    return elapsed;
  }

  static double getSeconds() {
    return getNanos() * 1e-9;
  }

  static int64_t getNanos() {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000000000 + t.tv_nsec;
  }

private:
  int64_t start_nanos;
};


/* Returns a timestamp in the form YYYY-mm-dd:HH:MM:SS
   If use_utc_time is nonzero, use the current UTC/GMT time.
   Otherwise use local time. */
std::string timestamp(bool use_utc_time);


/* Parse an unsigned number with a case-insensitive magnitude suffix:
     k : multiply by 2^10
     m : multiply by 2^20
     g : multiply by 2^30
     t : multiply by 2^40
     p : multiply by 2^50
     x : multiply by 2^60

   For example, "32m" would parse as 33554432.
   Floating point numbers are allowed in the input, but the result is always
   a 64-bit integer:  ".5g" yields uint64_t(.5 * 2^30)
   Return nonzero on error.
*/
int parseSize(const char *str, uint64_t *result);


/* Returns nonzero if (prefix) is a prefix of (s). */
int startsWith(const char *s, const char *prefix);


/* Return nonzero if the given filename exists and is a directory. */
int isDirectory(const char *filename);
