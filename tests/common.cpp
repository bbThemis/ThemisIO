#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common.h"


// return a timer in seconds
double getSeconds() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec + 1e-9 * t.tv_nsec;
}
  

// return a timer in nanoseconds
uint64_t getNanos() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return (uint64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}



// Sleeps the given number of seconds
void sleepSeconds(double seconds) {
  double start_time = getSeconds(), remaining;
  double int_part, fract_part;
  struct timespec ts;

  while (1) {
    remaining = seconds - (getSeconds() - start_time);
    if (remaining <= 0) break;

    fract_part = modf(remaining, &int_part);

    ts.tv_sec = int_part;
    ts.tv_nsec = fract_part * 1000*1000*1000;
    nanosleep(&ts, NULL);
  }
}


/* Returns a timestamp in the form YYYY-mm-dd:HH:MM:SS
   If use_utc_time is nonzero, use the current UTC/GMT time.
   Otherwise use local time. */
std::string timestamp(bool use_utc_time) {
  char buf[20];
  struct tm tm_struct;
  time_t now;

  time(&now);
  
  if (use_utc_time) {
    gmtime_r(&now, &tm_struct);
  } else {
    localtime_r(&now, &tm_struct);
  }

  strftime(buf, 20, "%Y-%m-%d:%H:%M:%S", &tm_struct);

  return buf;
}

  
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
int parseSize(const char *str, uint64_t *result) {
  const uint64_t SIZE_FACTORS[6] = {
    (uint64_t)1 << 10,
    (uint64_t)1 << 20,
    (uint64_t)1 << 30,
    (uint64_t)1 << 40,
    (uint64_t)1 << 50,
    (uint64_t)1 << 60
  };
  const char SIZE_SUFFIXES[6] = {'k', 'm', 'g', 't', 'p', 'x'};
  const int SIZES_COUNT = 6;
  
  uint64_t multiplier;
  const char *last;
  char suffix;
  int consumed, i;
  double mantissa;
  
  /* missing argument check */
  if (!str || !str[0] || (!isdigit((int)str[0]) && str[0] != '.')) return 1;

  if (1 != sscanf(str, "%lf%n", &mantissa, &consumed))
    return 1;
  last = str + consumed;

  /* these casts are to avoid issues with signed chars */
  suffix = (char)tolower((int)*last);
  
  if (suffix == 0) {

    multiplier = 1;

  } else {
    
    for (i=0; i < SIZES_COUNT; i++) {
      if (suffix == SIZE_SUFFIXES[i]) {
        break;
      }
    }
    
    /* unrecognized suffix */
    if (i == SIZES_COUNT)
      return 1;

    multiplier = SIZE_FACTORS[i];
  }

  *result = (uint64_t)(multiplier * mantissa);
  return 0;
}


/* Returns nonzero if (prefix) is a prefix of (s). */
int startsWith(const char *s, const char *prefix) {
  size_t len = strlen(prefix);
  return 0 == strncmp(s, prefix, len);
}


/* Return nonzero if the given filename exists and is a directory. */
int isDirectory(const char *filename) {
  struct stat statbuf;
  return !stat(filename, &statbuf) && (S_IFDIR & statbuf.st_mode);
}
