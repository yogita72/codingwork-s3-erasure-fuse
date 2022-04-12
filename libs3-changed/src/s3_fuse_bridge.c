
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include "s3_fuse_bridge.h"
#include "s3_erasure_code.h"
#include "log.h"
#include "util.h"

s3_tree_node		*gS3DirectoryTree = NULL;
s3_cache		*gS3Cache = NULL;
char			gExecuteDir[1024];
int			gEncodeFlag = 1;
s3_versioning_info		*gVersioningInfoList[50];

int	searchAndInsertPathInTree(const char *path, s3_tree_node **tree, 
									s3_tree_node **pathNode, int completeList )
{

	/* 
 	- if tree == NULL, build the tree by getting buckets from S3
	- search for path, if found and complete : return pathNode
	- if not found or not complete : get Path from S3 and add to tree
	- search for path, this time it should be found & complete : return pathNode
	*/

	int			ret = 0;
	int			count = 0;

	log_msg("searchAndInsertPathInTree\n" );
	if( *tree == NULL) {
		ret = populateNodes(tree, path, 1, &count);
		if(ret != 0 ) {
			goto ret;
		}
		gS3DirectoryTree = *tree;
		gVersioningInfoList[0] = NULL;
	} 	
	
	ret = searchForPath(path, *tree, pathNode);
	if(ret != 0 ) {
		goto ret;
	}

	if( (*pathNode == NULL) 
		|| (completeList 
			&&(((*pathNode)->isComplete & NODE_COMPLETE) == 0)) ) {

		if(*pathNode != NULL) {
			log_msg( "searchAndInsertPathInTree : pathNode not NULL,\n");
			log_msg( "name = %s isComplete = %d\n",
						(*pathNode)->s3FileInfo->name,
						(*pathNode)->isComplete);
		} else {	
			log_msg( "searchAndInsertPathInTree : pathNode NULL\n");
		}
		
		count = 0;

		ret = populateNodes(tree, path, 0, &count);
		if(ret != 0 ) {
			goto ret;
		}
		log_msg("searchAndInsertPathInTree path after S3 : %s\n",
								path);
		if((*pathNode) != NULL) {
			log_msg( "before search name = %s isComplete = %d\n",
						(*pathNode)->s3FileInfo->name,
						(*pathNode)->isComplete);
		}
		if( count != 0 ) {
			ret = searchForPath(path, *tree, pathNode);
			if(ret != 0 ) {
				goto ret;
			}
			if((*pathNode) != NULL) {
			log_msg( "after search name = %s isComplete = %d\n",
						(*pathNode)->s3FileInfo->name,
						(*pathNode)->isComplete);
			}
		}
	}

/*
	if( ((*pathNode) != NULL) 
		&& (((*pathNode)->isComplete & VERSION_COMPLETE) == 0 ) 
		&& ((*pathNode)->isFileNode  == 0 ) ) 
	{

		ret = populateVersions(*pathNode, path);
		if( ret != 0 ) {
			log_msg("populateVersions error\n" );
			goto ret;
		}
	}
	
*/

ret:
	return ret;
}

int populateNodes(s3_tree_node **tree, const char * path, 
					int initialize,  int *pCount) 
{
	int			count=0;
	s3_file_info		*s3FileInfoList=NULL;
	int			ret = 0; 

	log_msg( "populateNodes\n");
	ret = getPathFromS3(path, &count, &s3FileInfoList, initialize);
	if( ret != 0 ) {
		goto ret;
	}
	log_msg(" count = %d\n", count);
	if(count > 0 ) {
		ret = insertS3NodesInTree(tree, path, count, s3FileInfoList);
		if(ret != 0 ) {
			goto ret;
		}
	}
ret : 
	*pCount = count;
	if(s3FileInfoList != NULL ) {
		free(s3FileInfoList);
		s3FileInfoList = NULL;
	}
	return ret; 

}


int searchForPath(const char *path, s3_tree_node *tree, s3_tree_node **pathNode)
{
	s3_tree_node		*newTree = NULL;
	char			*tmpPath = NULL;
	char			*tmp = NULL;
	int			ret = 0 ;
	s3_tree_node		*foundNode = NULL;

	log_msg("searchForPath\n");

	tmpPath = strdup(path);
	tmp = strtok(tmpPath,"/");
	newTree = tree;
	
	while ( tmp != NULL ) {
		ret = searchNode( newTree, tmp, 0, &foundNode) ;
		if(ret != 0 ) {
			free (tmpPath);
			*pathNode = NULL;
			return ret ;
		}
		if(foundNode == NULL) {
			*pathNode = NULL;
			return 0;
		}
		newTree = foundNode;
		tmp = strtok(NULL, "/");
	}
	
	*pathNode = newTree;

	free(tmpPath);
	return 0;

}

int getPathFromS3(const char *path, int *pCount, s3_file_info **pS3FileInfoList,
													int initialize)
{

	log_msg("getPathFromS3\n");

		if( initialize == 1 ) {
 
	        list_service(0, pCount, pS3FileInfoList);

		} else {

			char 			*tmp = NULL;
			char			*bucket = NULL;
			char			*prefix = NULL;
			char			*tmpPath = NULL;

			
			tmpPath = strdup(path);
			log_msg("getPathFromS3 1 path = %s\n", path);
			tmp = strchr(tmpPath+1, '/');
				
			if( tmp != NULL) {

				*tmp = 0;
				prefix = malloc(strlen(tmp+1) +2);
				if( prefix == NULL) {
					return -ENOMEM;

				}
				/*S3 will honor all prefixes, whether it is part 
				of directory name, add a slash to get prefixes 
				which match directories */
				sprintf(prefix, "%s/", tmp+1);	
				bucket = strdup(tmpPath+1);
				*tmp = '/';
			} else {
				prefix = NULL;
				bucket = strdup(tmpPath+1);
			}

			log_msg("bucket = %s\n", bucket);
			if( prefix != NULL)
			log_msg("bucket = %s, prefix = %s\n", bucket, prefix);
			list_bucket(bucket, prefix, NULL, NULL, 10000, 0, 
										pCount, pS3FileInfoList ) ;
			free(bucket);
			free(prefix);
			free(tmpPath);
		}

	return 0;
}

int insertS3NodesInTree(s3_tree_node **tree, const char *path, int count, 
											s3_file_info *s3FileInfoList)
{
	log_msg("insertS3NodesInTree\n");
	/*
 	- if Tree == NULL, insert a node with /
	- for each s3_file_info, for each token search the tree
	- if TREE == NULL, s3FileInfoList will be bucket list, token parsing of
		s3_file-info not required
	- if found, search for next token in found treeNode
	- continue till not found, add the node  
 
  	*/

	s3_tree_node		*child=NULL;
	int			ret = 0 ;
	s3_file_info 		*tmpS3FileInfo = NULL;
	int			i = 0 ;

	if( *tree == NULL ) {
		ret = allocateTreeNode(tree); 		
		if( ret != 0 ) {
			return ret;
		}
		(*tree)->s3FileInfo->name= strdup("/");
		/* After a listing all buckets, children list of / is complete */
		(*tree)->isComplete |= NODE_COMPLETE;
		(*tree)->isComplete |= VERSION_COMPLETE;
		(*tree)->parent = NULL;

		for(i=0; i< count; i++) {
			ret = allocateTreeNode(&child);
			if(ret != 0 ) {
				return ret;
			}
		
			child->parent = (*tree);
			tmpS3FileInfo = ((s3_file_info *)&(s3FileInfoList[i]));
			child->s3FileInfo->name = (*tmpS3FileInfo).name; 
			child->s3FileInfo->time = (*tmpS3FileInfo).time; 
			child->s3FileInfo->size = (*tmpS3FileInfo).size; 

			child->next = (*tree)->children ;
			(*tree)->children = child; 
		}

	} else {

		/* 
		- s3FileInfoList->name contains full path of the object
		- for each token nodes need to be created and added to the tree
		- Node which matches the path needs to be marked complete
		- since all entries begin with path, let's add path first
		  and use the node to add rest of the tokens(children)
		-  

  		*/
		
		s3_tree_node		*newTree = NULL;
		char			*tmp = NULL, *tmp1 = NULL;
		s3_tree_node		*foundNode = NULL;
		s3_tree_node		*pathNode = NULL;
		char			*tmpName = NULL;
		char			*tmpPath = NULL;
		char			*pathPrefix = NULL;
		int			len =0;

		/* path will never be "/", atleast there will be bucket */	
		tmpPath = strdup(path);
		tmp = strtok(tmpPath, "/" ) ;
		newTree = (*tree) ;
		while ( tmp != NULL ) {
			ret = searchNode( newTree, tmp, 1, &foundNode) ;
			if(ret != 0 ) {
				return ret ;
			}
			/* foundNode will never be NULL, as insertFlag is 1 */
			newTree = foundNode;
			tmp = strtok(NULL, "/");
		}

		/* last foundnode is the path prefix for which all entries are 
  		 complete, mark it complete */
		if(foundNode != NULL)
			foundNode->isComplete |= NODE_COMPLETE;
		
		pathNode = foundNode;
		/* 
 		path includes bucket name, whereas S3FileInfoList starts with
		object name
		- remove first token from path to get pathPrefix
 		*/

		
		tmp1 = strchr(path+1, '/');	
		if( tmp1 != NULL ) {
			*tmp1 = 0;
			pathPrefix = tmp1+1;
			len = strlen(pathPrefix);
			*tmp1 = '/' ;
		} else {
			len = 0;
		}
	
		for(i=0; i < count ; i++ ) {

			log_msg("in for\n");
			newTree = pathNode;
			tmpS3FileInfo = ((s3_file_info *)&(s3FileInfoList[i]));
			log_msg("name %d : %s\n", i, tmpS3FileInfo->name);
			tmpName = strdup((tmpS3FileInfo->name) + len); 
			
			log_msg("name %d : %s\n", i, tmpName);
			tmp = strtok(tmpName, "/") ;
			
			while ( tmp != NULL ) {
				log_msg("tmp = %s newTree\n", tmp, 
						newTree->s3FileInfo->name);
				ret = searchNode( newTree, tmp, 1, &foundNode) ;
				if(ret != 0 ) {
					return ret ;
				}
			/* foundNode will never be NULL, as insertFlag is 1 */
				if( foundNode->s3FileInfo->time 
						< (*tmpS3FileInfo).time)
					foundNode->s3FileInfo->time
						= (*tmpS3FileInfo).time;

				foundNode->isComplete |= NODE_COMPLETE;
				newTree = foundNode;
				tmp = strtok(NULL, "/");
			}
		
			/* update the time if tmp is NULL to start */
			foundNode->s3FileInfo->time = (*tmpS3FileInfo).time;
			/* update the size of last node, the leaf node */
			foundNode->s3FileInfo->size = (*tmpS3FileInfo).size;
			foundNode->isFileNode = 1;

			if( isNodeMetaFile(foundNode))
			{
				char		*pathToMeta = NULL;
				ret = buildPathToMeta(path,
					tmpS3FileInfo->name,
					&pathToMeta);

				
				ret = fixEncodedFileInfo(foundNode, pathToMeta);
				log_msg("after FixEncodedFileInfo\n");

			}
			free(tmpS3FileInfo->name);
		}
	
		log_msg ("after for\n");
	}

	log_msg("returning %d\n", ret );
	return ret;
}

int isNodeMetaFile(s3_tree_node *node)
{

	char	metaSuffix[]= "_meta.txt";
	int	metaSuffixLen = 0 ;
	char	*fileName = NULL;
	int	fileNameLen = 0 ;
	int	j = 0;
	int	ret = 0;

	metaSuffixLen = strlen("_meta.txt");

	fileName = strdup(node->s3FileInfo->name);
	fileNameLen = strlen(fileName);
	for(j=1; j<= metaSuffixLen; j++) {
		if(metaSuffix[metaSuffixLen -j] 
			!= fileName[fileNameLen-j] )
			break;
	} 

	if(j >  metaSuffixLen) { 
		ret =  1;
	} else {
		ret = 0;
	}

ret: 
	
	if(fileName != NULL) {
		free(fileName);
		fileName = NULL;
	}
	return ret;
}

int buildPathToMeta(char*path, char *keyName, char **pPathToMeta)
{
	char 	*tmp2 = NULL;
	char	*tmpPath1 = NULL;
	char	*completePath = NULL;
	int	ret = 0 ;
		

	completePath = strdup(keyName);
	log_msg(
		"before fixEncodedFileInfo path :%s pathprefix %s\n",
						completePath,
						path);
				
	tmpPath1 = strdup(path);
	log_msg("tmpPath = %s\n", tmpPath1);
	tmp2 = strchr(tmpPath1+1, '/');
	if(tmp2 !=NULL)
		*tmp2 = 0 ;

	*pPathToMeta = malloc(strlen(tmpPath1) 
					+ strlen(completePath)+5);

	if(*pPathToMeta == NULL) {

		ret =  -ENOMEM;
		goto ret;
	}
				
	sprintf(*pPathToMeta, "%s/%s",
					tmpPath1,
					completePath);

	if(tmp2 != NULL)
		*tmp2 = '/';
	log_msg("PathToMeta = %s\n", *pPathToMeta);


ret:
	if(tmpPath1 != NULL ) {
		free(tmpPath1);
		tmpPath1 = NULL;
	}
	if(completePath != NULL) {
		free(completePath);
		completePath = NULL;
	}

	return ret; 
}
int searchNode(s3_tree_node *tree, char *name, 
				int insertFlag, s3_tree_node **pResultNode)
{
	s3_tree_node 		*child = NULL;
	s3_tree_node 		*prev = NULL;
	int			result=-1;
	int			ret = 0 ;
	
	log_msg("searchNode\n");
	if( tree == NULL ) {
		*pResultNode = NULL;
		return 0;

	}	

	child = tree->children;

	while ( child != NULL ){
		
		log_msg("searchNode strcmp  %s : %s\n",
				child->s3FileInfo->name, name);
		result = strcmp( child->s3FileInfo->name, name);
		if(result > 0 ) {
			prev = child;
			child = child->next;
		} else {
			break;
		}
	}	

	if(result == 0 ) {
		*pResultNode = child;
	} else if (insertFlag == 0 ) {
		*pResultNode = NULL;	
	} else {
		ret = allocateTreeNode(pResultNode);
		if( *pResultNode != NULL ) {
			(*pResultNode)->s3FileInfo->name = strdup(name);
			
			if(result > 0) {
				(*pResultNode)->next = NULL;
			} else {
				(*pResultNode)->next = child;
				if(child != NULL )
					child->prev = *pResultNode;
			}
			(*pResultNode)->parent = tree;
			if(prev == NULL) {
				tree->children = *pResultNode;
				(*pResultNode)->prev = NULL;
			} else {
				prev->next = *pResultNode;
				(*pResultNode)->prev = prev;
			}
			
			
		}
		
	}
	return ret;
}


int  allocateTreeNode(s3_tree_node **pResultNode)
{

	log_msg("allocateTreeNode\n");	
	*pResultNode = (s3_tree_node *) malloc(sizeof(s3_tree_node));
	if( *pResultNode == NULL ) {
		return -ENOMEM;
	}			
	(*pResultNode)->s3FileInfo = (s3_file_info *) malloc(sizeof(s3_file_info)) ;
	if( (*pResultNode)->s3FileInfo == NULL ) {
		return -ENOMEM;
	}			
	(*pResultNode)->s3FileInfo->name = NULL;
	(*pResultNode)->s3FileInfo->time = 0;
	(*pResultNode)->s3FileInfo->size = -1;
	(*pResultNode)->s3FileInfo->versionId = NULL;
	(*pResultNode)->isComplete = 0;
	(*pResultNode)->isFileNode = 0;
	(*pResultNode)->s3Name = NULL;
	(*pResultNode)->children = NULL;

	return 0;
}


int getPathForNode(s3_tree_node *pathNode, char **pPath)
{

	int		pathSize = 0;
	int		bufferSize = 0;
	s3_tree_node	*parent = NULL;
	int		ret = 0 ;
	char		*tmpPath = NULL;
	char		*tmpPath1 = NULL;

	parent = pathNode->parent;

	bufferSize = 1024;
	tmpPath = malloc(bufferSize);
	if(tmpPath== NULL ) {
		ret = -ENOMEM;
		goto ret ;
	}

	pathSize += strlen(pathNode->s3FileInfo->name);
	sprintf(tmpPath, "%s", pathNode->s3FileInfo->name);
	log_msg("path =%s\n", tmpPath);

	while ( parent != NULL ) {

		pathSize += strlen(parent->s3FileInfo->name);
		if(pathSize > bufferSize) {

			bufferSize += 1024;
			tmpPath= realloc(tmpPath, bufferSize);

			log_msg("realloc path\n");
			if(tmpPath == NULL ) {
				ret = -ENOMEM;
				goto ret ;
			}
		}
		
		log_msg("parent name %s, tmpPath = %s\n", 
					parent->s3FileInfo->name, tmpPath);

		tmpPath1 = strdup(tmpPath) ;
		sprintf(tmpPath, "%s/%s", parent->s3FileInfo->name, tmpPath1);
		free(tmpPath1);
		log_msg("path =%s\n", tmpPath);

		parent = parent->parent;

	}

	// there will be an extra slash, remove it
	
	log_msg("path =%s\n", tmpPath+1);
	*pPath = strdup(tmpPath + 1) ;	
	
	free(tmpPath);
	log_msg("returning path =%s\n", *pPath);
ret :
	return ret;

}
int fixEncodedFileInfo(s3_tree_node *node, char* path)
{

/*
 *	- node points to a _meta.txt
 *	- fetch it from S3 to cache to get parent size
 *	- unlink the file 
 *
 */

	int		ret = 0;
	char		*cachedPath = NULL;
	char		*parentCachedPath = NULL;
	char		*temp = NULL;
	int		fileSize = 0 ;
	FILE		*fp = NULL;
	char		*tmpPath = NULL;
	char		*tmp = NULL;
	int		inCache = 0 ;
	struct stat 	statbuf;

	tmpPath = strdup(path);
	log_msg("tmpPath = %s\n", tmpPath );

	
	ret = s3CacheGetCachedPath(gS3Cache, path, &cachedPath); 
	
	if( ret != 0 ) {
		log_msg("fixEncodedFileInfo : error s3CacheGetCachedPath\n");
		goto ret;
	}	


	tmp = strrchr(cachedPath, '/');
	if(tmp != NULL )
		*tmp = 0;
	parentCachedPath  = strdup(cachedPath) ;
	if(tmp != NULL )
		*tmp = '/';


	if ( (stat(parentCachedPath, &statbuf) == 0)
				&& (S_ISREG(statbuf.st_mode)) ) {

		node->parent->s3FileInfo->size = statbuf.st_size;
		node->parent->isFileNode = 1;
		free(parentCachedPath);	

	} else {
		ret = s3CacheFetch(gS3Cache, tmpPath);
		if( ret != 0 ) {
			log_msg("fixEncodedFileInfo : error s3CacheFetch\n");
			goto ret;
		}	


		fp = fopen(cachedPath, "rb");
        	temp = (char *)malloc(sizeof(char)*(1024));
        	fscanf(fp, "%s", temp);

        	if (fscanf(fp, "%d", &fileSize) != 1) {
                	log_msg("File size is not valid\n");
			return ret;
        	}

		free(temp);
		temp = NULL;
		node->parent->s3FileInfo->size = fileSize;
		node->parent->isFileNode = 1;
		fclose(fp);
		unlink(cachedPath);
		tmp = strrchr(cachedPath, '/');
		*tmp = 0;
		rmdir(cachedPath); 
		*tmp = '/';
	}
ret:
	if(cachedPath != NULL )
		free(cachedPath);
	if(tmpPath != NULL )
		free(tmpPath);	
	log_msg("returning from fixEncodedFileInfo\n");
	return ret;
}



int updateDirTree(char *path, int isFileNode)
{

	s3_tree_node		*foundNode = NULL;
	int			ret = 0 ;
	char			*tmpPath=NULL;
	char			*tmp = NULL;
	s3_tree_node 		*newTree = NULL;
	char			*cachedPath = NULL;
	struct stat 		statbuf;


	log_msg("updateDirTree\n");

	tmpPath = strdup(path);
	tmp = strtok(tmpPath,"/");
	newTree = gS3DirectoryTree;

	while ( tmp != NULL ) {
		log_msg("updateDirTree tmp = %s\n", tmp);
		ret = searchNode( newTree, tmp, 1, &foundNode) ;
		if(ret != 0 ) {
			goto ret ;
		}
		log_msg("found Node : %s\n", foundNode->s3FileInfo->name);
		newTree = foundNode;
		tmp = strtok(NULL, "/");
	}
	// update the size of leaf node
	
	if( isFileNode ) {
		ret = s3CacheGetCachedPath(gS3Cache, path, &cachedPath);
		if( ret != 0 ) {
			goto ret;
		}

		if ( stat(cachedPath, &statbuf) == 0) {
			foundNode->s3FileInfo->size = statbuf.st_size;
		}
		free(cachedPath);
		log_msg("marking not VERSION_COMPLETE\n");
		foundNode->isComplete &= !(VERSION_COMPLETE);
		foundNode->parent->isComplete &= !(VERSION_COMPLETE);
	} else {

		foundNode->isComplete |= NODE_COMPLETE;
	}

	foundNode->isFileNode = isFileNode;
	
ret: 
	if(tmpPath != NULL)
		free(tmpPath);
	return ret;
}

int	addDirectory(const char *path)
{

	char		*tmp  = NULL;
	char		*tmpPath = NULL;
	int		ret = 0 ;
	int		s3Status = 0 ;

	log_msg("addDirectory path = %s\n", path);
	tmpPath = strdup(path);
	tmp = strchr(tmpPath+1, '/') ;
	if(tmp == NULL ) {

	// create bucket
		int		argc =1 ;
		char		*argv[2] = { NULL, NULL } ;

		argv[0] = tmpPath+1;
		s3Status = create_bucket(argc, argv, 0);
		if(s3Status != 0 ) { 
			logS3Errors(s3Status);
			ret = -EINVAL;
			goto ret; 
		}

		log_msg("after create bucket\n");
	}
	
	log_msg("updating directory tree\n");
	ret = updateDirTree(tmpPath, 0) ;

ret :
	return ret ;
}

int	deletePath( char *path)
{
	int		ret = 0 ;

	if(strstr(path, ".versions") == NULL ) {
		ret = deleteThroughS3(path);
		if(ret != NULL) {

			log_msg("deletePath : deleteThroughS3 error\n");

			goto ret;

		}

	} else {

		ret = deleteThroughTree(path);
		if(ret != NULL) {

			log_msg("deletePath : deleteThroughTree error\n");

			goto ret;

		}
		

	}

ret:
	return ret;
}
int deleteChildren(s3_tree_node *node)
{

	s3_tree_node	*child = NULL;
	s3_tree_node	*next = NULL;

	log_msg("deleteChildren\n");
	child = node->children;

	while(child != NULL ) {

		next = child->next;
		deleteNode(child);
		child = next;

	}
	node->children = NULL;

}

int deleteNode(s3_tree_node *node)
{
	int		ret = 0 ;


	log_msg("deleteNode \n") ;

	if( node->children != NULL) {

		ret = deleteChildren(node);
	}


	if( node->next != NULL)
		node->next->prev = node->prev;
	else
		log_msg("%s : node->next ==  NULL\n", node->s3FileInfo->name);

	if( node->prev != NULL) {
		node->prev->next = node->next;
	} else {
		node->parent->children = node->next;
		log_msg("%s : node->prev ==  NULL\n", node->s3FileInfo->name);
	}

		
	free(node->s3FileInfo->name);
	if(node->s3FileInfo->versionId != NULL )
		free(node->s3FileInfo->versionId);
	if(node->s3Name != NULL)
		free(node->s3Name);
	free(node);
	return ret;

}


int deleteThroughTree(char *path)
{
	int 		ret = 0 ;
	s3_tree_node	*foundNode =  NULL;
	char		*tmp = NULL;
	char		key[4096];
	s3_tree_node	*child = NULL;

	ret = searchForPath(path, gS3DirectoryTree, &foundNode);

	if( ret != 0 ) {

		log_msg("deleteThroughTree : searchForPath %s error\n",
					path);
		goto ret;
	}


	if(foundNode == NULL ) {
		log_msg("deleteThroughTree : path %s not found\n",
							path);

		ret = -EINVAL;
		goto ret;
	}

	if( foundNode->children != NULL ) {

		child = foundNode->children;
		while ( child != NULL) {

			if( child->s3Name != NULL ) {

				tmp = strstr(path, ".versions");

				if( tmp != NULL)
					*tmp = 0;

				sprintf(key, "%s%s", path+1, child->s3Name);

				if(tmp != NULL )
					*tmp = '.';

			} else {

				sprintf(key, "%s", path+1);
			}
	
			deleteObjectFromS3(key, child->s3FileInfo->versionId);

			child = child->next;
		}

	} else {

		if( foundNode->s3Name != NULL ) {

			tmp = strstr(path, ".versions");

			if( tmp != NULL)
				*tmp = 0;

			sprintf(key, "%s%s", path+1, foundNode->s3Name);

			if(tmp != NULL )
				*tmp = '.';

		} else {

			sprintf(key, "%s", path+1);
		}
	
		deleteObjectFromS3(key, foundNode->s3FileInfo->versionId);
	}
	deleteNode(foundNode);
ret: 

	return ret;
}

int deleteThroughS3(char  *path)
{
	
	int		ret = 0 ;
	int		count = 0 ;
	s3_file_info	*s3FileInfoList = NULL;
	int		i = 0 ;
	s3_file_info  	*tmpS3FileInfo =  NULL;
	char		*bucket = NULL;
	char		key[4096];
	char		*tmp = NULL;
	s3_tree_node	*foundNode = NULL;
	char		*tmpPath =  NULL;

	tmpPath = strdup(path);

	ret = getPathFromS3(tmpPath, &count, &s3FileInfoList, 0);
	if( ret != 0 ) {
		log_msg("deleteThroughS3 : getPathFromS3 error\n");
		goto ret;
	}
	log_msg(" count = %d\n", count);
	
	tmp = strchr(tmpPath+1, '/');
	if(tmp != NULL)
		*tmp = 0;
	bucket = strdup(tmpPath+1);
	if(tmp != NULL )
		*tmp = '/';
	
	for(i=0; i < count; i++ ) {

		tmpS3FileInfo = (s3_file_info *)&(s3FileInfoList[i]);
		sprintf(key, "/%s/%s", bucket, tmpS3FileInfo->name);
		deleteObjectFromS3(key+1, NULL);
	}

	if( strcmp(path+1, bucket) == 0 ) {

		deleteBucketFromS3(bucket);

	}

	
	searchForPath(path, gS3DirectoryTree, &foundNode);
	deleteNode(foundNode);

ret: 
	if(s3FileInfoList != NULL)
		free(s3FileInfoList);
	if(bucket != NULL )
		free(bucket);
	if(tmpPath != NULL)
		free(tmpPath);
	return ret;
}

int deleteObjectFromS3(char *key, char *versionId)
{

	int		argc = 1;
	char		*argv[3] = { NULL, NULL, NULL };
	int		ret = 0 ;
	int		s3Status = 0 ;
	char		versionIdStr[4096];


	argv[0] = strdup(key);
	
	log_msg("deleteObjectFromS3 : argv[0] = %s\n",
						argv[0]);
	if(versionId != NULL) {
		sprintf(versionIdStr, "versionId=%s", versionId);
		argv[1] = strdup(versionIdStr);
		argc = 2;
		log_msg("deleteObjectFromS3 : argv[1] = %s\n",
						argv[1]);
	}


	s3Status = delete_object(argc, argv, 0 ) ;
	if( s3Status != 0 ) {
		logS3Errors(s3Status);
		ret = -EINVAL;
		goto ret; 
	}

ret:
	if(argv[0] != NULL)
		free(argv[0]);
	if(argv[1] != NULL)
		free(argv[1]);
	return ret;

}

int deleteBucketFromS3(char *bucket)
{
	int		argc = 1;
	char		*argv[2] = {NULL, NULL};
	int		s3Status = 0 ;
	int		ret = 0 ;


	argv[0] = strdup(bucket);
	
	s3Status = delete_bucket(argc, argv, 0 ) ;
	if( s3Status != 0 ) {
		logS3Errors(s3Status);
		ret = -EINVAL;
		goto ret; 
	}

ret :
	if(argv[0] != NULL )
		free(argv[0]);

	return ret;

}
/***************************versioning functions *****************************/

int populateVersions(s3_tree_node *pathNode, const char *path)
{
	int		ret=0;
	char		*tmp = NULL;
	char		*bucket = NULL;
	char		*tmpPath = NULL;
	char		*versioningState = NULL;

	log_msg("populateVersions\n");
	tmpPath = strdup(path);

	// extract bucket name from path
	tmp = strchr(tmpPath+1, '/');
	if(tmp != NULL )
		*tmp = 0;
	bucket = strdup(tmpPath+1);
	if(tmp != NULL )
		*tmp = '/';

	ret = getVersioningInfo(bucket, &versioningState);
	if( ret != 0 ) { 
		log_msg("getVersioningInfo error, bucket : %s\n", bucket );
		goto ret;
	}
	
	if( versioningState != NULL ) {

		ret = insertVersionNodes(pathNode, path ); 	
		if( ret != 0 ) {

			log_msg("insertVersionNodes error\n");
			goto ret;
		}


	}	

ret: 
	if(tmpPath != NULL)
		free(tmpPath);
	if(bucket != NULL )
		free(bucket);
	return ret;
}

int getVersioningInfo(char *bucket, char **pVersioningState)
{

	int		i=0;
	int		ret = 0 ;

	if( gVersioningInfoList[0] != NULL ) {

		while(gVersioningInfoList[i] != NULL ) {
			
			if(strcmp(bucket, gVersioningInfoList[i]->bucket) == 0 )
				break;

			i++;
		}

	}

	if( gVersioningInfoList[i] == NULL ) {

		// bucket info doesn't exist, populate versioning state

		ret = populateVersioningInfo(bucket, &i) ;
		if( ret != 0 ) {
			log_msg("populateVersiongInfo error\n") ;
			goto ret;
		}
	}

	*pVersioningState = gVersioningInfoList[i]->state;
ret:
	return ret;
}

int populateVersioningInfo(char *bucket, int *pIndex)
{

	int		argc = 1;
	char		*argv[2] = { NULL, NULL };
	char		*versioning = NULL;
	int		s3Status = 0 ;
	int		ret = 0 ;

	argv[0] = strdup(bucket);
	s3Status = get_versioning( argc, argv, 0, &versioning);
	if( s3Status != 0 ) {

		logS3Errors(s3Status);
		ret = -EINVAL;
		goto ret; 
	}

	log_msg("versioning on bucket %s : %s",
					argv[0], versioning );

	gVersioningInfoList[*pIndex] = (s3_versioning_info * )
					malloc(sizeof(s3_versioning_info) ) ;

	if(gVersioningInfoList[*pIndex] == NULL) {
		ret = -ENOMEM;
		goto ret;
	}

	gVersioningInfoList[*pIndex]->bucket = strdup(bucket);
	if( (strcmp(versioning, "Enabled") == 0 )
		|| (strcmp(versioning, "Suspended") == 0 ) ) {
		gVersioningInfoList[*pIndex]->state = strdup(versioning);
		log_msg("index : %d Versioning for %s : %s\n", 
			*pIndex,
			gVersioningInfoList[*pIndex]->bucket,
			gVersioningInfoList[*pIndex]->state);
			
	} else {
		gVersioningInfoList[*pIndex]->state = NULL;

	}
	gVersioningInfoList[(*pIndex) + 1 ] = NULL;

ret: 

	free(argv[0]);
	return ret;
}

int insertVersionNodes(s3_tree_node *pathNode, const char * path ) 
{
	s3_tree_node		*child = NULL;
	char			*tmpPath = NULL;
	char			childPath[4096];
	int			pathLen = 0 ;
	int			childPathLen = 0 ;
	int			count=0;
	S3ListVersionsContent	*s3VersionsList = NULL ;
	int			ret = 0 ;
	s3_tree_node		*versionNode = NULL;
	s3_tree_node		*childChild = NULL;


	tmpPath = strdup(path);
	pathLen = strlen(tmpPath); 
	child = pathNode->children ;
	strcpy(childPath, tmpPath);	

	if(child != NULL) {

		pathNode->isComplete |= VERSION_COMPLETE;
	}

	while(child != NULL ) {

		if((child->isFileNode == 0 ) 
			|| ((child->isComplete & VERSION_COMPLETE) != 0 )) {
			child = child->next ;
			continue;
		}

		strcat(childPath, "/");
		strcat(childPath, child->s3FileInfo->name);			

		ret = prepareForInsertingVersions(childPath, 
						child, 
						&versionNode);	
		if( ret != 0 ) {
			log_msg("prepareForInsertingVersions error : path %s\n",
							childPath);
			goto ret;
		}

		childChild = child->children;
		if( childChild == NULL ) {
			ret = getVersionsFromS3(childPath, 
						&count, 
						&s3VersionsList); 
			if( ret != 0 ) {
				log_msg("getVersionsFromS3 error : path %s\n",
							childPath);
				goto ret;

			}

			log_msg("getVersionsFromS3 path = %s  count = %d\n", 
								childPath,
								count);
		

			if( count != 0 ) {
				ret = processVersionsList(versionNode, 
							child, 
							childPath,
							count, 
							s3VersionsList);
				if( ret != 0 ) {
					log_msg("processVersionsList error\n");
					goto ret;

				}

			}

		} else {
		
			childPathLen = strlen(childPath); 
			while( childChild != NULL )  {

				strcat(childPath, "/");
				strcat(childPath, childChild->s3FileInfo->name);

				log_msg(
					"getVersionsFromS3 : path %s\n",
							childPath);
				ret = getVersionsFromS3(childPath, 
						&count, 
						&s3VersionsList); 
				if( ret != 0 ) {
					log_msg(
					"getVersionsFromS3 error : path %s\n",
							childPath);
					goto ret;

				}

				log_msg("getVersionsFromS3 childPath =%s count = %d\n", 
								childPath,
								count);
		

				if( count != 0 ) {
					ret = processVersionsList(versionNode, 
							child, 
							childPath,
							count, 
							s3VersionsList);
					if( ret != 0 ) {
						log_msg("processVersionsList error\n");
						goto ret;

					}

				}
				childChild = childChild->next;
				childPath[childPathLen] = 0;

			}
		}
		childPath[pathLen ] = 0;
		child->isComplete |= VERSION_COMPLETE;
		child = child->next;

	}

ret:


	if(tmpPath != NULL)
		free(tmpPath);
	if(s3VersionsList != NULL )
		free(s3VersionsList);
	return ret;
}

int getVersionsFromS3(char *path, int *pContentsCount, 
				S3ListVersionsContent **pContents)
{
	char		*bucketName = NULL;
	char		*prefix = NULL;
	char		*tmp = NULL;
	char		*tmpPath = NULL;
	int		s3Status = 0 ;
	int		ret = 0 ;

	tmpPath = strdup(path);

	tmp = strchr(tmpPath+1, '/');
	if(tmp != NULL)
		*tmp = 0;
	bucketName = strdup(tmpPath+1);
	if( tmp != NULL) {
		prefix = strdup(tmp+1);
		*tmp = '/';
	}  

	
	s3Status =  list_versions(bucketName, prefix,
                        NULL, NULL, NULL,
                        10, 0,
                        pContents, pContentsCount);

	if( s3Status != 0 ) {
		
		logS3Errors(s3Status);
		ret = -EINVAL;
		goto ret; 
	}

ret : 
	if(tmpPath != NULL)
		free(tmpPath);
	if(bucketName != NULL)
		free(bucketName);
	if(prefix != NULL)
		free(prefix);
	return ret;
}


int prepareForInsertingVersions(char *path, s3_tree_node *node,
					s3_tree_node** pVersionNode )
{

	char	*tmpPath = NULL;
	char    *tmp = NULL;
	char	versionNodeName[4096];
	int	ret = 0;
	
	
	tmpPath = strdup(path);

	tmp = strrchr(tmpPath, '/');
	
	sprintf(versionNodeName, ".versions-%s", tmp+1);

	ret = searchNode( node->parent, versionNodeName, 1, pVersionNode) ;

	if( ret != 0 ) {
		log_msg("prepareForInseringVersions : searchNode error\n");
		goto ret;
	}
	
	if((*pVersionNode)->children != NULL) {
		ret = deleteChildren(*pVersionNode);
		if( ret != 0 ) {
			log_msg(
		"prepareForInsertingVersions : deleteChildren error\n");
			goto ret;

		}
	}
	(*pVersionNode)->isComplete |= NODE_COMPLETE;
	(*pVersionNode)->isComplete |= VERSION_COMPLETE;

ret :
	if(tmpPath != NULL)
		free(tmpPath);
	return ret;
}


int processVersionsList(s3_tree_node *versionNode, 
			s3_tree_node *child, 
			char *childPath,
			int count, 
		        S3ListVersionsContent	*s3VersionsList)
{

	int			ret = 0 ;
	S3ListVersionsContent	*tmpS3VersionsContent=NULL;
	char			versionI[1024] ;
	s3_tree_node		*foundNode =  NULL;
	char			*childChildName =  NULL;
	char			parentChildName[1024];
	int			i = 0 ;
	int			j = 0 ;

/*
 *  - create a node for each version - name suffixed with @i
 *  - if node has children add a child node, else add s3Fileinfo
 * 
 */

	for(i=0; i < count; i++) {

		tmpS3VersionsContent = (S3ListVersionsContent *) 
						(s3VersionsList +i );

		if( tmpS3VersionsContent->deleteMarker == 1 ) {
			continue;

		}
		memset(versionI, 0, 1024);
		sprintf(versionI, "%d-%s", j, child->s3FileInfo->name) ;
		
		ret = searchNode( versionNode, versionI, 1, &foundNode) ;

		if( ret != 0 ) {
			log_msg("processVersionsList : searchNode error\n");
			goto ret;
		}

		foundNode->s3Name = strdup(child->s3FileInfo->name);
		foundNode->isFileNode = 1;

		if( child->children == NULL ) {
			foundNode->s3FileInfo->size =
					tmpS3VersionsContent->size ;
		
			foundNode->s3FileInfo->time =
					tmpS3VersionsContent->lastModified ;

			foundNode->s3FileInfo->versionId =
				strdup(tmpS3VersionsContent->versionId) ;
		} else {
		
			childChildName = strrchr(tmpS3VersionsContent->key, '/');		
			ret = searchNode( foundNode, childChildName+1 , 1, &foundNode) ;

			if( ret != 0 ) {
				log_msg("processVersionsList : searchNode error\n");
				goto ret;
			}

						
			foundNode->s3FileInfo->size =
					tmpS3VersionsContent->size ;
		
			foundNode->s3FileInfo->time =
					tmpS3VersionsContent->lastModified ;


			if( foundNode->s3FileInfo->time >
					foundNode->parent->s3FileInfo->time ) {
					
				foundNode->parent->s3FileInfo->time =
					foundNode->s3FileInfo->time; 
			}
			foundNode->s3FileInfo->versionId =
				strdup(tmpS3VersionsContent->versionId) ;

			sprintf(parentChildName, "%s/%s",
					child->s3FileInfo->name,
					foundNode->s3FileInfo->name);
	
			foundNode->s3Name = strdup(parentChildName); 
			
			if( isNodeMetaFile(foundNode)) {
			
				char	*pathToMeta = NULL;
				ret = getPathForNode(foundNode, &pathToMeta);
				log_msg("before FixEncodedFileInfo\n");
				ret = fixEncodedFileInfo(foundNode, pathToMeta);
				log_msg("after FixEncodedFileInfo\n");

			}
		}
		
		j++;

	}
ret:
	return ret;
}

/**************************** Cache Functions *******************************/


int s3CacheInit(s3_cache **pCache, char* cacheLocation)
{
	*pCache = (s3_cache *) malloc(sizeof(s3_cache));
	if( *pCache == NULL) {
		return -ENOMEM;
	}

	(*pCache)->location = cacheLocation;
	(*pCache)->count = 0;
	gS3Cache = *pCache;

	return 0;
}
int s3CacheGetCachedPath(s3_cache *cache, const char *path, char **pCachedPath)
{

	log_msg("s3CacheGetCachedPath\n");
	*pCachedPath = (char * ) malloc(strlen(path) + strlen(cache->location) +1 );

	if( *pCachedPath == NULL) {
		log_msg("out of Memory\n");
		return -ENOMEM;
	}

	if( *path == '/' )
		sprintf(*pCachedPath,"%s%s", cache->location, path);
	else
		sprintf(*pCachedPath,"%s/%s", cache->location, path);

	log_msg("cachedPath = %s\n", *pCachedPath);
	return 0;

}
int s3CacheInCache(s3_cache *cache, const char* path, int *pInCache)
{
	char 		*cachedPath =NULL;		
	struct stat 	statbuf;
	int		ret = 0 ;

	log_msg("s3CacheInCache\n");
	ret = s3CacheGetCachedPath(cache, path, &cachedPath);
	if(ret != 0 ) {
		return ret;
	}
	if (stat(cachedPath, &statbuf) == -1) {
		*pInCache = 0;
	} else {
		*pInCache = 1;
	}

	free(cachedPath);
	return 0;
}
int s3CacheMarkForFlush(s3_cache *cache, const char* path)
{
	
	log_msg("s3CacheMarkForFlush\n");
	cache->flushList[(cache->count)] = strdup(path);
	cache->count++ ;
	return 0;
}
	
int s3CacheFetch(s3_cache *cache, const char* path)
{
	int		argc = 2;
	char		*argv[4] = { NULL, NULL,NULL, NULL};
	char		*cachedPath = NULL;
	int		ret = 0 ;
	struct stat	statbuf;
	char		*tmp = NULL;
	char		*tmp1 = NULL;
	char		*tmpPath = NULL;
	s3_tree_node	*foundNode = NULL;
	int		s3Status = 0 ;
	char		*s3Name = NULL;
	
	
	log_msg("s3CacheFetch\n");

	tmpPath = strdup(path);

	ret = s3CacheGetCachedPath(cache, tmpPath, &cachedPath);
	if(ret != 0 ) {
		return ret;
	}

	log_msg("after GetCachedPath path = %s\n", tmpPath);
	log_msg("s3CacheFetch 1\n");

	tmp = strrchr(cachedPath, '/');
	*tmp = 0;
	ret = mkpath(cachedPath);
	if((ret != 0) && (errno != EEXIST)  )
		return ret;

	*tmp = '/';

	log_msg("mkpath\n");
	log_msg("after mkpath path = %s\n", tmpPath);



	ret = searchForPath(tmpPath, gS3DirectoryTree, &foundNode);

	if(( ret != 0 ) || (foundNode == NULL) ) {

		goto ret;
	} 

	
	if(foundNode->s3Name != NULL ) {

		// strip off .version-* 
		s3Name = strdup(tmpPath);	
		tmp = strstr(tmpPath, ".versions");
		*tmp = 0;

		sprintf(s3Name, "%s%s", tmpPath, foundNode->s3Name);		
		*tmp = '/';
		
	} else {
		s3Name = strdup(tmpPath);
	}
	if( (foundNode->isFileNode == 1) && (foundNode->children != NULL) ) {
	
		ret = getObjectAndDecode(s3Name, cachedPath, foundNode);
		if( ret != 0 ) {
			log_msg("Error : get_object_and_decode\n");
			goto ret;
		}
		

	} else { 
		argv[0] = malloc(strlen(s3Name) +1 ) ;
		if(argv[0] == NULL) {
			return -ENOMEM;
		}
	
	
		strcpy( argv[0], (s3Name+1)) ;
			
		log_msg("argv[0] :%s\n", argv[0]);


		argv[1] = malloc(strlen(cachedPath) + strlen("filename=") +1 ) ;
		if(argv[1] == NULL) {
			ret =  -ENOMEM;
			goto ret;
		}
	
		sprintf(argv[1], "filename=%s", cachedPath);

		log_msg("argv[1] = %s\n", argv[1]);

		if(foundNode->s3FileInfo->versionId != NULL) {

			argc = 3;
			argv[2] = malloc(strlen("versionId=") 
				+ strlen(foundNode->s3FileInfo->versionId) +1 ) ;
			if(argv[2] == NULL) {
				ret =  -ENOMEM;
				goto ret;
			}
	
			sprintf(argv[2], "versionId=%s", 
					foundNode->s3FileInfo->versionId);
			
			log_msg("argv[2] = %s\n", argv[2]);

		
		}
		s3Status = get_object(argc, argv, 0); 
		if(s3Status != 0 ) { 
			logS3Errors(s3Status);
			ret = -EINVAL;
			goto ret; 
		}
		log_msg("after getObject\n");

		free(argv[0]);
		free(argv[1]);
		if(argv[2] != NULL)
			free(argv[2]);
	}

	if (stat(cachedPath, &statbuf) == -1) {
		return -1;
	}
	
ret :
	if(s3Name != NULL)
		free(s3Name);
	free(tmpPath);
	free(cachedPath);
	log_msg("s3CacheFetch return\n");
	return ret;
}


int s3CacheFlushCache(s3_cache *cache, char* path)
{
	int			i;
	int			argc = 2;
	char			*argv[3];
	char			*cachedPath = NULL;
	int			ret = 0 ;
	int			s3Status = 0 ;

		log_msg("s3CacheFlushCache\n");
	for(i=(cache->count)-1 ; i >=0; i-- ){

		if( (path == NULL) || (strcmp(path,cache->flushList[i]) == 0) ) {

		log_msg("s3CacheFlushCache for\n");
		ret = s3CacheGetCachedPath(cache, cache->flushList[i], &cachedPath);
		if(ret != 0 ) {
			return ret;
		}

		if(gEncodeFlag == 1 ) {

			ret = encodeObjectAndPut(cache->flushList[i], cachedPath);
			log_msg("after encodeObjectAndPut\n");

		} else {
			argv[0] = strdup(cache->flushList[i]+1);
			argv[1] = malloc(strlen(cachedPath) + strlen("filename=") +1 ) ;
			if(argv[1] == NULL) {
				return -ENOMEM;
			}
	
			sprintf(argv[1], "filename=%s", cachedPath);
			argv[2] = 0;
	
			log_msg("argv[0] = %s argv[1] = %s\n", argv[0], argv[1]);
			s3Status = put_object(argc, argv, 0); 
			if(s3Status != 0 ) { 
				logS3Errors(s3Status);
				return -EINVAL;
			}


			free(argv[0]);
			argv[0] = NULL;
			free(argv[1]);
			argv[1] = NULL;
		}	
		free(cachedPath);
		cachedPath = NULL;

		updateDirTree(cache->flushList[i], 1);
		}
	}
	return 0;
}

	
/****************************************************************************/
int mkpath(char *path)
{
        char    tmp[1024];
        char    *p = NULL;
        size_t  len;
        int     status=0;

	log_msg("mkpath\n");
        snprintf(tmp, sizeof(tmp), "%s", path);
        len = strlen(tmp);
        if(tmp[len -1] == '/')
                tmp[len-1] = 0;

        for(p=tmp+1; *p; p++) {
                if(*p == '/') {
                        *p =0;
                        status = mkdir(tmp, S_IRWXU);
                        if( status== -1 && errno != EEXIST){
                                fprintf(stderr, "Unable to create directory %s\n", tmp);
				log_msg("Unable to create directory %s\n", tmp);
                                perror(0);
                                return status;
                        }
                        *p = '/';
                }
        }

        status = mkdir(tmp, S_IRWXU);
        if( status== -1 && errno != EEXIST){
                fprintf(stderr, "Unable to create directory %s\n", tmp);
		log_msg("Unable to create directory %s\n", tmp);
                perror(0);
                return status;

        }
	log_msg("mkpath end\n");

        return status;
}

int saveExecuteDir()
{

	getcwd(gExecuteDir, 1024);
	return 0;
}


void logS3Errors(int status)
{
    if (status < S3StatusErrorAccessDenied) {
        log_msg("\nERROR: %s\n", S3_get_status_name(status));
    }
    else {
        log_msg("\nERROR: %s\n", S3_get_status_name(status));
        log_msg("%s\n", errorDetailsG);
    }
}
