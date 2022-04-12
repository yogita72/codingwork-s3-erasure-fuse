// There are a couple of symbols that need to be #defined before
// #including all the headers.

#ifndef _PARAMS_H_
#define _PARAMS_H_

// The FUSE API has been changed a number of times.  So, our code
// needs to define the version of the API that we assume.  As of this
// writing, the most current API version is 26
#define FUSE_USE_VERSION 26

// need this to get pwrite().  I have to use setvbuf() instead of
// setlinebuf() later in consequence.
#define _XOPEN_SOURCE 500

// maintain bbfs state in here
#include <limits.h>
#include <stdio.h>
#include "s3_fuse_bridge.h"

struct s3_fuse_state {
    FILE 			*logfile;
    s3_cache 			*cache;
	s3_tree_node	*dirTree;
};
#define S3_FUSE_DATA ((struct s3_fuse_state *) fuse_get_context()->private_data)

#endif
