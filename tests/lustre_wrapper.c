#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#ifndef NO_LUSTRE
#include <lustre/lustreapi.h>
#endif

#include "lustre_wrapper.h"


int lustre_get_striping(const char *filename, int *stripe_count,
                        uint64_t *stripe_size) {
#ifndef NO_LUSTRE
  size_t v1_size, v3_size, buf_size;
  struct lov_user_md *stripe_params;
  int err;

  v1_size = sizeof(struct lov_user_md_v1) +
    LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);
  v3_size = sizeof(struct lov_user_md_v3) +
    LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);
  
  buf_size = (v1_size > v3_size) ? v1_size : v3_size;

  stripe_params = (struct lov_user_md *) malloc(buf_size);
  if (!stripe_params)
    return ENOMEM;

  err = llapi_file_get_stripe(filename, stripe_params);
  if (err)
    return errno;
  
  *stripe_count = stripe_params->lmm_stripe_count;
  *stripe_size = stripe_params->lmm_stripe_size;
  free(stripe_params);

  return 0;
#else
  struct stat s;
  if (stat(filename, &s)) {
    return errno;
  } else {
    /* return common dummy values */
    *stripe_count = 1;
    *stripe_size = 1<<20;
    return 0;
  }
#endif /* NO_LUSTRE */
}


/*
  Alternate method:

      struct lov_user_md opts = {0};
      opts.lmm_magic = LOV_USER_MAGIC;
      opts.lmm_stripe_size = file->stripe_length;
      opts.lmm_stripe_offset = -1;
      opts.lmm_stripe_count = file->stripe_count;

      file->fileno = open64(file->filename,
                            omode | O_EXCL | O_LOV_DELAY_CREATE,
                            0600);
      if (file->fileno >= 0) {
        ioctl(file->fileno, LL_IOC_LOV_SETSTRIPE, &opts);
        printf("File created on Lustre, %dx%d stripes\n",
               file->stripe_count, file->stripe_length);
      }

 */


int lustre_create_striped(const char *filename, mode_t mode,
                          int stripe_count,
                          uint64_t stripe_size, int stripe_offset) {
#ifndef NO_LUSTRE

  /* result >= 0: success, this is the file descriptor
     result < 0: error, this is errno */
  int fd = llapi_file_open(filename, O_CREAT | O_EXCL | O_WRONLY, mode,
                               stripe_size, stripe_offset, stripe_count, 0);
  if (fd >= 0) {
    /* we're just creating the file; close it right away */
    close(fd);
    return 0;
  }

  fd = abs(fd);
  if (fd == EALREADY) fd = EEXIST;
  return fd;

#else

  int fd = open(filename, O_CREAT|O_EXCL|O_WRONLY, mode);
  if (fd == -1) {
    return errno;
  } else {
    close(fd);
    return 0;
  }
  
#endif /* NO_LUSTRE */
}


