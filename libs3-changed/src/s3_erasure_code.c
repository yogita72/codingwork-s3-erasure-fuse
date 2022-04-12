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
#include <dirent.h>
#include "erasurecodes.h"
#include "s3_fuse_bridge.h"
#include "s3_erasure_code.h"
#include "log.h"
#include "util.h"

erasure_policy		gErasurePolicy;

int getObjectAndDecode(char *path, char *cachedPath, s3_tree_node *foundNode)
{

	char		*codingPath = NULL;
	char		*destinationPath = NULL;
	char		*sourcePath = NULL;
	char		*childName = NULL;
	int		argc = 2;
	char		*argv[4] = {NULL, NULL, NULL, NULL };
	int		decodeArgc = 2;
	char		*decodeArgv[3] = {"decoder", NULL, NULL};
	int		ret = 0 ;
	s3_tree_node	*child = NULL;
	DIR		*pDir = NULL;
	struct dirent	*entry = NULL;	
	char		decodedFileName[1024];
	char		*fileToDecode = NULL;
	char		curdir[1024];
	int 		ret1 = 0 ;
	int		s3Status = 0 ;
	
	

	codingPath = malloc(1024); 
	if( codingPath == NULL ) {
		ret = -ENOMEM;
		goto ret;
	}
	
	//getcwd(codingPath, 1024);
	sprintf(codingPath, "%s/", gExecuteDir);

	log_msg("cur dir = %s\n", codingPath);

	ret1 = chdir(codingPath);
	log_msg("chdir returned %d\n", ret1);
	getcwd(curdir, 1000);
	log_msg("curdir = %s\n", curdir);

	mkpath("Coding");
	log_msg("get_object_and_decode\n");
	child = foundNode->children;
	while( child != NULL ) {

		childName = child->s3FileInfo->name;
		log_msg("child = %s\n", childName);

		destinationPath = malloc(strlen(codingPath)
						+strlen("Coding/")
						+ strlen(childName) + 5); 

		if( destinationPath == NULL ) {
			ret = -ENOMEM;
			goto ret;
		}

		sprintf(destinationPath, "%sCoding/%s", codingPath, childName);

		// build sourcePath
		sourcePath = malloc( strlen(path) + strlen(childName) + 5) ;
		if( sourcePath == NULL ) {
			ret = -ENOMEM;
			goto ret;
		}

		sprintf(sourcePath, "%s/%s", path+1, childName);	

		argv[0] = sourcePath;
		log_msg("argv[0] = %s\n", argv[0]);
		argv[1] = malloc(strlen("filename=") + strlen(destinationPath) +5);
		if(argv[1] == NULL ) {
			ret= -ENOMEM ;
			goto ret;
		}

		sprintf(argv[1], "filename=%s", destinationPath);
		log_msg("argv[1] = %s\n", argv[1]);

		if(child->s3FileInfo->versionId != NULL) {

			argc = 3;
			argv[2] = malloc(strlen("versionId=") 
				+ strlen(child->s3FileInfo->versionId) +1 ) ;
			if(argv[2] == NULL) {
				ret =  -ENOMEM;
				goto ret;
			}
	
			sprintf(argv[2], "versionId=%s", 
					child->s3FileInfo->versionId);
			
			log_msg("argv[2] = %s\n", argv[2]);

		
		}
		log_msg("before get_object\n");
		
		s3Status = get_object(argc, argv, 0 );	
		if(s3Status != 0 ) { 
			logS3Errors(s3Status);
			ret = -EINVAL;
			goto ret; 
		}
		log_msg("after get_object\n");	


		free(destinationPath);
		destinationPath = NULL;
		free(sourcePath);
		sourcePath = NULL;
		free(argv[1]);
		argv[1] = NULL;

		child = child->next;
	}

	// all files are available in Coding directory, set arguments and decode
	
	fileToDecode = strrchr(path, '/');
	log_msg("fileToDecode %s\n", fileToDecode+1);
	decodeArgv[1] = strdup(fileToDecode+1) ; 	

	log_msg("before decode\n");
	decode(decodeArgc, decodeArgv);
	log_msg("after decode\n");



	// decoded file is available in Coding directory
	// traverse through directory, rename decoded file to cachedPath
	// unlink other files, remove Coding Directory
	//


	pDir =  opendir("Coding");
	if(pDir == NULL) {

		log_msg("\nERROR: Failed to open Coding directory");
                        perror(0);
                        ret = -errno;
	}

	while( (entry= readdir(pDir)) != NULL) {

		fprintf(stderr, "readdir\n");
		if((strcmp(entry->d_name, ".") == 0)
				|| ((strcmp(entry->d_name, "..") ==0))){
			continue;
		}

		sprintf(decodedFileName, "Coding/%s", entry->d_name);
		if(( strstr(entry->d_name, "decoded") != NULL)) {
			log_msg("decodedFileName : %s\n", decodedFileName);
                                                                           

			ret= rename(decodedFileName, cachedPath);
		} else {
			unlink(decodedFileName);
		}
	}
ret :
	if(codingPath != NULL)
		free(codingPath);
	if(sourcePath != NULL)
		free(sourcePath);
	return ret ;
}


int  encodeObjectAndPut(char* path, char *cachedPath)
{

	int		encodeArgc = 8;
	char		*encodeArgv[8] 
		= {"encode", NULL, NULL, NULL, NULL, NULL, NULL, NULL} ;
	DIR		*pDir = NULL;
	struct	dirent	*entry = NULL;
	char		*encodedFileName = NULL;
	char		*encodedKey = NULL;
	int		ret = 0 ;
	int		argc = 2;
	char		*argv[3];
	int		s3Status = 0 ;

	
	chdir(gExecuteDir);
	encodeArgv[1] = strdup(cachedPath);

	encodeArgv[2] = gErasurePolicy.int_k;
	encodeArgv[3] = gErasurePolicy.int_m;
	encodeArgv[4] = gErasurePolicy.codingTechnique;
	encodeArgv[5] = gErasurePolicy.int_w;
	encodeArgv[6] = gErasurePolicy.int_packetSize;
	encodeArgv[7] = gErasurePolicy.int_bufferSize;	

	log_msg("printing encode args\n" ) ;
	log_msg("%s, %s, %s, %s, %s, %s, %s, %s\n", 
					encodeArgv[0],
					encodeArgv[1],
					encodeArgv[2],
					encodeArgv[3],
					encodeArgv[4],
					encodeArgv[5],
					encodeArgv[6],
					encodeArgv[7]);
					
	log_msg("before encode\n");
	encode(encodeArgc, encodeArgv);
	log_msg("after encode\n");
	
	
	pDir =  opendir("Coding");
	if(pDir == NULL) {

		log_msg("\nERROR: Failed to open Coding directory");
                        perror(0);
                        ret = -errno;
	}

	while( (entry= readdir(pDir)) != NULL) {

		fprintf(stderr, "readdir\n");
		if((strcmp(entry->d_name, ".") == 0)
				|| ((strcmp(entry->d_name, "..") ==0))){
			continue;
		}

		
		encodedFileName = malloc(strlen("Coding/") 
					+ strlen(entry->d_name) +5 ) ;
		if(encodedFileName == NULL ) {
			ret = -ENOMEM;
			goto ret;
		}

		sprintf(encodedFileName, "Coding/%s", entry->d_name);

		encodedKey = malloc( strlen(path+1)
					+ strlen(entry->d_name) + 5 );

		if(encodedKey == NULL ) {
			ret = -ENOMEM;
			goto ret;
		}

		sprintf(encodedKey, "%s/%s", path +1, entry->d_name);

		argv[0] = encodedKey;
		argv[1] = malloc(strlen(encodedFileName) + strlen("filename=") +1 ) ;
		if(argv[1] == NULL) {
			return -ENOMEM;
		}
	
		sprintf(argv[1], "filename=%s", encodedFileName);
		argv[2] = 0;

		log_msg("argv[0] = %s argv[1] = %s\n", argv[0], argv[1]);
		s3Status = put_object(argc, argv, 0); 
		if(s3Status != 0 ) { 
			logS3Errors(s3Status);
			ret = -EINVAL;
			goto ret; 
		}
		log_msg("after put_object");
		if(unlink(encodedFileName) != 0 ) {
			log_msg("failed to unlink %s : %d\n", encodedFileName,
								errno);
		}
			
		free(argv[1]);
		free(encodedFileName);
		free(encodedKey);

	}

ret :
	return ret ;
}
int saveErasurePolicy()
{

	FILE		*fp = NULL;

	fp = fopen("erasure_policy", "r");
	if( fp == NULL ) {
		return -errno;
	}
	
	fscanf(fp, "%s", gErasurePolicy.int_k);
	fscanf(fp, "%s", gErasurePolicy.int_m);
	fscanf(fp, "%s", gErasurePolicy.codingTechnique);
	fscanf(fp, "%s", gErasurePolicy.int_w);
	fscanf(fp, "%s", gErasurePolicy.int_packetSize);
	fscanf(fp, "%s", gErasurePolicy.int_bufferSize);
	
	fclose(fp);
	return 0;

}
