/* Wayback Versioned Filesystem
 * Copyright (C) 2004  Brian Cornell (techie@northwestern.edu)
 *
 * This program can be distributed under the terms of the GNU GPL.
 * The GPL can be found at http://www.gnu.org/licenses/gpl.txt
 *
 * This program uses FUSE as a backend:
    *
	FUSE: Filesystem in Userspace
	Copyright (C) 2001  Miklos Szeredi (mszeredi@inf.bme.hu)

	This program can be distributed under the terms of the GNU GPL.
	See the file COPYING.
    *
*/

#ifndef WAYBACKFS_H
#define WAYBACKFS_H

#ifdef linux
/* For pread()/pwrite() */
#define _XOPEN_SOURCE 500
#endif

#include <fuse.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/statfs.h>
#include <ctype.h>
//#include <fuse.h>
//#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <linux/types.h>
//#include <fcntl.h>
//#include <unistd.h>

//#define DEBUG

#ifdef DEBUG
#define IFDEBUG(x) x
#else
#define IFDEBUG(x)
#endif


char** parseargs(int, char**);
void readsettings(FILE*);
//void ver_makecopy(char*);
void ver_updateversion(char*, size_t, off_t, char);
void ver_updatedirectory(int, char*, char*, struct stat*);
char* ver_deletedpath(char*);

/* actions for update directory */
#define VUD_CREATE	1
#define VUD_RM		2
#define VUD_MKDIR	4
#define VUD_RMDIR	5
#define VUD_RENAME	8
#define VUD_CHMOD	12
#define VUD_CHOWN	13
#define VUD_UTIME	14

extern char* redirectpath;
extern char* deletedmodifier;
extern char* versionmodifier;
extern int malloced_redir;
extern int malloced_deleted;
extern int malloced_ver;

#endif
