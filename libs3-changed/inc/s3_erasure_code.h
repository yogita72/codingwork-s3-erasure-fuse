#ifndef S3_ERASURE_CODE_H
#define S3_ERASURE_CODE_H

#include "s3.h"

/***************** structure Definitions ***************/
typedef struct erasure_policy {

	char		int_k[6];
	char		int_m[6];
	char		codingTechnique[1024];
	char		int_w[6];
	char		int_packetSize[6];
	char		int_bufferSize[6];	

} erasure_policy;

/****************** global variables ******************/
extern erasure_policy		gErasurePolicy;

/******************* function definitions ****************/
int saveErasurePolicy();
int getObjectAndDecode(char *path, char *cachedPath, s3_tree_node *foundNode);
int  encodeObjectAndPut(char* path, char *cachedPath);

#endif /* S3_ERASURE_CODE_H */
