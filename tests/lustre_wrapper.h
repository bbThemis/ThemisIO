#ifndef __LUSTRE_WRAPPER_H__
#define __LUSTRE_WRAPPER_H__

/* Wrapper functions for Lustre API, because it currently fails
   when compiled with C++, and because it has badly designed
   interfaces such as the variable size of the buffer passed to 
   llapi_file_get_stripe().

   lustre header file fails with:
     gcc -std=c90  (doesn't like "inline") 
     C++ compilers (doesn't like converting ints to enums)

   lustre header file succeeds with:
     gcc
     gcc -std=c99 -D_GNU_SOURCE
     gcc -std=gnu90
     gcc -std=gnu99
     gcc -std=gnu11
     Cray "CC" C compiler with GNU or Cray programming environment

   To simplify testing on non-Lustre systems, this can be compiled
   with -DNO_LUSTRE and the functions will do nothing interesting.
   lustre_get_striping() will just check if the file exists, and if it
   does it will return (1, 1048576) and lustre_create_striped() will
   just run open(filename, O_CREAT|O_EXCL|O_WRONLY, 0644) and then
   close the file.

   Ed Karrels, edk@illinois.edu June 2021
*/

#include <stdint.h>
#include <fcntl.h>

#ifdef __cplusplus
extern "C" {
#endif


/* Returns 0 on success, otherwise one of:
     ENOMEM         failed to allocate memory.
     ENAMETOOLONG   path was too long.
     ENOENT         path does not point to a file or a directory.
     ENOTTY         path does not point to a Lustre filesystem.
     EFAULT         memory region pointed by lum is not properly mapped.
*/
int lustre_get_striping(const char *filename,
                        int *stripe_count,
                        uint64_t *stripe_size);

/* stripe_size must be a multiple of 65536 and less than or equal to 2^64

   Warning: Lustre seems to ignore the mode argument, always creating file with mode 0600,
   even when using open().

   Returns 0 on success, otherwise:
     EINVAL    stripe_count, stripe_size, or stripe_offset is invalid.
     EEXIST    File already exists.
     ENOTTY    name may not point to a Lustre filesystem.
*/
int lustre_create_striped(const char *filename,
                          mode_t mode,
                          int stripe_count,
                          uint64_t stripe_size,
                          int stripe_offset /* default -1 */ );

#ifdef __cplusplus
}
#endif

#endif /* __LUSTRE_WRAPPER_H__ */
