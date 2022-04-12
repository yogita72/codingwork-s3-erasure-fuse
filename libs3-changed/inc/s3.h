#ifndef S3_H
#define S3_H

#include "libs3.h"

/********************Structure Definitions *************************/
typedef struct s3_file_info {
	char	*name;
	time_t	time;
	int64_t size;
	char	*versionId;

} s3_file_info;

/******************* Global Variables *****************************/

extern int statusG;
extern char errorDetailsG[4096];

/******************** Function Definitions ************************/


int list_service(int allDetails, int *pCount, s3_file_info** pS3FileInfoList);
int list_bucket(const char *bucketName, const char *prefix,
                        const char *marker, const char *delimiter,
                        int maxkeys, int allDetails,
			int *pCount, s3_file_info** pS3FileInfoList);

int saveSecurityCredentials();
int get_object(int argc, char **argv, int optindex);
int put_object(int argc, char **argv, int optindex);
int delete_object(int argc, char **argv, int optindex);
int create_bucket(int argc, char **argv, int optindex);
int set_versioning(int argc, char **argv, int optindex);
int get_versioning(int argc, char **argv, int optindex, char **pVersioning);
int list_versions(const char *bucketName, const char *prefix,
                        const char *keyMarker, const char *versionIdMarker,
                        const char *delimiter,
                        int maxkeys, int allDetails,
                        S3ListVersionsContent** pContents, int *pContentsCount);
#endif /* S3_H */
