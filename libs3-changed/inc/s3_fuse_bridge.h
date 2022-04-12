#ifndef S3_FUSE_BRIDGE_H
#define S3_FUSE_BRIDGE_H

#include "s3.h"
#include "libs3.h"

/********************Structure Definitions *************************/

//typedef struct s3_child_node  	s3_child_node;
typedef struct s3_tree_node	s3_tree_node;

#define		NODE_COMPLETE		1
#define		VERSION_COMPLETE	2

struct s3_tree_node {

	s3_file_info	*s3FileInfo;
	int		isFileNode;
	int		isComplete;
	char		*s3Name;
	s3_tree_node	*children;
	s3_tree_node	*parent;
	s3_tree_node 	*prev;
	s3_tree_node 	*next;
	
};

typedef struct s3_versioning_info {
	char		*bucket;
	char		*state;
} s3_versioning_info;

typedef struct s3_cache {

	char		*location;
	int		count;
	char		*flushList[50];

} s3_cache;

/********************Global Variables ******************************/
extern s3_tree_node		*gS3DirectoryTree;
extern s3_cache		*gS3Cache;
extern char		gExecuteDir[1024];
extern int		gEncodeFlag;

/******************** Function Definitions ************************/

/******* tree functions **********/

int	searchAndInsertPathInTree(const char *path, s3_tree_node **tree, 
									s3_tree_node **pathNode, int completeList );

int populateNodes(s3_tree_node **tree, const char * path, 
					int initialize,  int *pCount); 
int searchForPath(const char *path, s3_tree_node *tree, s3_tree_node **pathNode);
int getPathFromS3(const char *path, int *pCount, s3_file_info **pS3FileInfoList,
														int initialize);
int insertS3NodesInTree(s3_tree_node **tree, const char* path, int count, 
											s3_file_info *s3FileInfoList);

int searchNode(s3_tree_node *tree, char *name, 
				int insertFlag, s3_tree_node **pResultNode);

int  allocateTreeNode(s3_tree_node **pResultNode);


int fixEncodedFileInfo(s3_tree_node *node, char* path);

int updateDirTree(char *path, int isFileNode);

int getPathForNode(s3_tree_node *pathNode, char **pPath);
int	addDirectory(const char *path);
int	deletePath(char *path);
int deleteNode(s3_tree_node *node);
int deleteChildren(s3_tree_node *node);
int deleteThroughTree(char *path);
int deleteThroughS3(char *path);
int deleteObjectFromS3(char *key, char *versionId);
int deleteBucketFromS3(char *bucket);

/**************versioning functions ****************************/

int populateVersions(s3_tree_node *pathNode, const char *path);

int getVersioningInfo(char *bucket, char **pVersioningState);

int populateVersioningInfo(char *bucket, int *pIndex);

int insertVersionNodes(s3_tree_node *pathNode, const char * path ); 

int getVersionsFromS3(char *path, int *pContentsCount, 
				S3ListVersionsContent **pContents);

int prepareForInsertingVersions(char *path, s3_tree_node *node,
					s3_tree_node** pVersionNode );

int processVersionsList(s3_tree_node *versionNode, 
			s3_tree_node *child, 
			char *childPath,
			int count, 
		        S3ListVersionsContent	*s3VersionsList);
/************ cache functions *************/
int s3CacheInit(s3_cache **pCache, char* cacheLocation) ;
int s3CacheGetCachedPath(s3_cache * cache, const char *path, char **pCachedPath);
int s3CacheInCache(s3_cache * cache, const char* path, int *pInCache);
int s3CacheMarkForFlush(s3_cache * cache, const char* path);
int s3CacheFetch(s3_cache * cache, const char* path);
int s3CacheFlushCache(s3_cache * cache, char* path);

int mkpath(char *path);
int saveExecuteDir();
void logS3Errors(int status);
#endif /* S3_FUSE_BRIDGE_H */
