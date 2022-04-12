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

#include "waybackfs.h"

//Must add concatination of redirect path to all instances of path in this code

char* redirectpath = "/";
char* deletedmodifier = ".  versionfs! deleted";
char* versionmodifier = ".   versionfs! version";
int malloced_redir = 0, malloced_ver = 0, malloced_deleted = 0;

static int ver_getattr(const char *path, struct stat *stbuf)
{
    int res;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
#ifdef DEBUG
    printf("Get Attributes of (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    res = lstat(fullpath, stbuf);
    
    free(fullpath);
    
    if(res == -1)
        return -errno;

    return 0;
}

static int ver_readlink(const char *path, char *buf, size_t size)
{
    int res;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
#ifdef DEBUG
    printf("Read link (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    res = readlink(fullpath, buf, size - 1);

    free(fullpath);
    
    if(res == -1)
        return -errno;

    buf[res] = '\0';
    return 0;
}

int matchesdeleted(char* haystack, char* needle) {
	char* cursubstr = NULL;
	while ((haystack = strstr(haystack, needle))) {
		cursubstr = haystack;
		haystack = &haystack[1];
	}
	if (!cursubstr)
		return 0;
	cursubstr = &(cursubstr[strlen(needle)]);
	while (cursubstr[0] != '\0') {
		if (isdigit(cursubstr[0]) == 0)
			return 0;
		cursubstr = &(cursubstr[1]);
	}
	return 1;
}

static int ver_getdir(const char *path, fuse_dirh_t h, fuse_dirfil_t filler)
{
    DIR *dp;
    struct dirent *de;
    int res = 0;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
#ifdef DEBUG
    printf("Get directory (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    dp = opendir(fullpath);

    free(fullpath);
    
    if(dp == NULL)
        return -errno;

    while((de = readdir(dp)) != NULL) {
#ifdef DEBUG
	printf("Reading directory entry named %s\n", de->d_name);
#endif
	if (!(strlen(de->d_name) > strlen(deletedmodifier) 
				&& matchesdeleted(de->d_name, deletedmodifier))
			&& !(strlen(de->d_name) >= strlen(versionmodifier)
				&& !strcmp(versionmodifier,
					&(de->d_name[strlen(de->d_name) - strlen(versionmodifier)])))) {
            res = filler(h, de->d_name, de->d_type);
            if(res != 0)
	        break;
	}
#ifdef DEBUG
	else {
	    printf("Omitting directory entry for matching version/deleted string.\n");
	}
#endif
    }

    closedir(dp);
    return res;
}

static int ver_mknod(const char *path, mode_t mode, dev_t rdev)
{
    int res;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
#ifdef DEBUG
    printf("Make node (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    res = mknod(fullpath, mode, rdev);
    if (res == 0)
	    ver_updatedirectory(VUD_CREATE, fullpath, NULL, NULL);
    
    free(fullpath);
    
    if(res == -1)
        return -errno;

    return 0;
}

static int ver_mkdir(const char *path, mode_t mode)
{
    int res;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
#ifdef DEBUG
    printf("Make directory (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    res = mkdir(fullpath, mode);
    
    if (res == 0)
	    ver_updatedirectory(VUD_MKDIR, fullpath, NULL, NULL);

    free(fullpath);
    
    if(res == -1)
        return -errno;

    return 0;
}

static int ver_unlink(const char *path)
{
    int res;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
    char* linkdest = NULL;
    struct stat filestats;
#ifdef DEBUG
    printf("Unlink (delete) (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    if (!(strlen(fullpath) >= strlen(versionmodifier)
	  && !strcmp(versionmodifier,
		     &(fullpath[strlen(fullpath) - strlen(versionmodifier)])))) {
        res = lstat(fullpath, &filestats);
	if (res == 0 && S_ISREG(filestats.st_mode)) {
	    if (filestats.st_nlink == 1)
		ver_updateversion(fullpath, 0, 0, 1);
	    else {
		ver_updateversion(fullpath, 0, 0, 1);
	    }
        }
	if (res == 0 && S_ISLNK(filestats.st_mode)) {
	    linkdest = malloc(sizeof(char[1024]));
	    res = readlink(fullpath, linkdest, 1024);
	    if (res > 0)
		linkdest[(res < 1024) ? res : 1023] = '\0';
	    else
		linkdest[0] = '\0';
	}
    }
    
    res = unlink(fullpath);
    if (res == 0 && !(strlen(fullpath) >= strlen(versionmodifier)
		      && !strcmp(versionmodifier,
				 &(fullpath[strlen(fullpath) - strlen(versionmodifier)]))))
            ver_updatedirectory(VUD_RM, fullpath, linkdest, &filestats);

    free(fullpath);
    free(linkdest);
    
    if(res == -1)
        return -errno;

    return 0;
}

static int ver_rmdir(const char *path)
{
    int res;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
    char* deletedpath;
#ifdef DEBUG
    printf("Remove directory (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    deletedpath = ver_deletedpath(fullpath);

    res = rename(fullpath, deletedpath);
    if (res == 0)
	    ver_updatedirectory(VUD_RMDIR, fullpath, deletedpath, NULL);
    
    free(fullpath);
    free(deletedpath);
    
    if(res == -1)
        return -errno;

    return 0;
}

static int ver_symlink(const char *from, const char *to)
{
    int res;
    char* fullfrom = malloc(sizeof(char[strlen(from) + strlen(redirectpath) + 1]));
    char* fullto = malloc(sizeof(char[strlen(to) + strlen(redirectpath) + 1]));
#ifdef DEBUG
    printf("Symbolic link (%s)%s to %s\n", redirectpath, from, to);
#endif
    if (!fullfrom) {free(fullto); return -ENOMEM;}
    if (!fullto) {free(fullfrom); return -ENOMEM;}
    
    sprintf(fullfrom, "%s", from);
    sprintf(fullto, "%s%s", redirectpath, to);
    
    res = symlink(fullfrom, fullto);
    if (res == 0)
	    ver_updatedirectory(VUD_CREATE, fullto, fullfrom, NULL);

    free(fullfrom);
    free(fullto);
    
    if(res == -1)
        return -errno;

    return 0;
}

static int ver_rename(const char *from, const char *to)
{
    int res;
    char* fullfrom = malloc(sizeof(char[strlen(from) + strlen(redirectpath) + 1]));
    char* fullto = malloc(sizeof(char[strlen(to) + strlen(redirectpath) + 1]));
#ifdef DEBUG
    printf("Rename (%s)%s to %s\n", redirectpath, from, to);
#endif
    if (!fullfrom) {free(fullto); return -ENOMEM;}
    if (!fullto) {free(fullfrom); return -ENOMEM;}
    
    sprintf(fullfrom, "%s%s", redirectpath, from);
    sprintf(fullto, "%s%s", redirectpath, to);

    res = rename(fullfrom, fullto);
    if (res == 0)
	    ver_updatedirectory(VUD_RENAME, fullfrom, fullto, NULL);

    free(fullfrom);
    free(fullto);
    
    if(res == -1)
        return -errno;

    return 0;
}

static int ver_link(const char *from, const char *to)
{
    int res;
    char* fullfrom = malloc(sizeof(char[strlen(from) + strlen(redirectpath) + 1]));
    char* fullto = malloc(sizeof(char[strlen(to) + strlen(redirectpath) + 1]));
#ifdef DEBUG
    printf("Link (%s)%s to %s\n", redirectpath, from, to);
#endif
    if (!fullfrom) {free(fullto); return -ENOMEM;}
    if (!fullto) {free(fullfrom); return -ENOMEM;}
    
    sprintf(fullfrom, "%s%s", redirectpath, from);
    sprintf(fullto, "%s%s", redirectpath, to);

    res = link(fullfrom, fullto);
    if (res == 0)
	    ver_updatedirectory(VUD_CREATE, fullfrom, fullto, NULL);

    free(fullfrom);
    free(fullto);
    
    if(res == -1)
        return -errno;

    return 0;
}

static int ver_chmod(const char *path, mode_t mode)
{
    int res;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
    struct stat filestats;
#ifdef DEBUG
    printf("Change mode of (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    res = lstat(fullpath, &filestats);
    
    res = chmod(fullpath, mode);
    if (res == 0)
	    ver_updatedirectory(VUD_CHMOD, fullpath, NULL, &filestats);

    free(fullpath);
    
    if(res == -1)
        return -errno;
    
    return 0;
}

static int ver_chown(const char *path, uid_t uid, gid_t gid)
{
    int res;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
    struct stat filestats;
#ifdef DEBUG
    printf("Change owner of (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    res = lstat(fullpath, &filestats);
    
    res = lchown(fullpath, uid, gid);
    if (res == 0)
	    ver_updatedirectory(VUD_CHOWN, fullpath, NULL, &filestats);

    free(fullpath);
    
    if(res == -1)
        return -errno;

    return 0;
}

static int ver_truncate(const char *path, off_t size)
{
    int res;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
#ifdef DEBUG
    printf("Truncate (%s)%s, %d\n", redirectpath, path, sizeof(off_t));
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    ver_updateversion(fullpath, (size_t)0, size, 1);
    
    res = truncate(fullpath, size);

    free(fullpath);
    
    if(res == -1)
        return -errno;

    return 0;
}

static int ver_utime(const char *path, struct utimbuf *buf)
{
    int res;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
    struct stat filestats;
#ifdef DEBUG
    printf("Utime (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    res = lstat(fullpath, &filestats);
    
    res = utime(fullpath, buf);
    if (res == 0)
	    ver_updatedirectory(VUD_UTIME, fullpath, NULL, &filestats);

    free(fullpath);
    
    if(res == -1)
        return -errno;

    return 0;
}


static int ver_open(const char *path, int flags)
{
    int res;
    int existed;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
    struct stat filestats;
#ifdef DEBUG
    printf("Open (create) (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    res = lstat(fullpath, &filestats);
    if (res == -1 && errno == ENOENT)
	    existed = 0;
    else
	    existed = 1;
    
    res = open(fullpath, flags);
    if (res == 0 && existed == 0 && (flags & O_CREAT))
	    ver_updatedirectory(VUD_CREATE, fullpath, NULL, NULL);

    free(fullpath);
    
    if(res == -1) 
        return -errno;

    close(res);
    return 0;
}

static int ver_read(const char *path, char *buf, size_t size, off_t offset)
{
    int fd;
    int res;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
#ifdef DEBUG
    printf("Read (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    fd = open(fullpath, O_RDONLY);

    free(fullpath);
    
    if(fd == -1)
        return -errno;

    res = pread(fd, buf, size, offset);
    if(res == -1)
        res = -errno;
    
    close(fd);
    return res;
}

static int ver_write(const char *path, const char *buf, size_t size,
                     off_t offset)
{
    int fd;
    int res;
    char* fullpath = malloc(sizeof(char[strlen(path) + strlen(redirectpath) + 1]));
#ifdef DEBUG
    printf("Write (%s)%s\n", redirectpath, path);
#endif
    if (!fullpath) return -ENOMEM;
    
    sprintf(fullpath, "%s%s", redirectpath, path);

    ver_updateversion(fullpath, size, offset, 0);
    
    fd = open(fullpath, O_WRONLY);

    free(fullpath);
    
    if(fd == -1)
        return -errno;

    res = pwrite(fd, buf, size, offset);
    if(res == -1)
        res = -errno;
    
    close(fd);

    return res;
}

static int ver_statfs(struct fuse_statfs *fst)
{
	struct statfs st;
	int rv = statfs(redirectpath, &st);
	if (!rv) {
		fst->block_size  = st.f_bsize;
		fst->blocks      = st.f_blocks;
		fst->blocks_free = st.f_bfree;
		fst->files       = st.f_files;
		fst->files_free  = st.f_ffree;
		fst->namelen     = st.f_namelen;
	}
	return rv;
}

static int ver_release(const char *path, int flags)
{
#ifdef DEBUG
    printf("Release (%s)%s\n", redirectpath, path);
#endif

    return 0;
}

static int ver_fsync(const char *path, int isdatasync)
{
#ifdef DEBUG
	printf("Fsync (%s)%s\n", redirectpath, path);
#endif
	return 0;
}

static struct fuse_operations ver_oper = {
    getattr:	ver_getattr,
    readlink:	ver_readlink,
    getdir:     ver_getdir,
    mknod:	ver_mknod,
    mkdir:	ver_mkdir,
    symlink:	ver_symlink,
    unlink:	ver_unlink,
    rmdir:	ver_rmdir,
    rename:     ver_rename,
    link:	ver_link,
    chmod:	ver_chmod,
    chown:	ver_chown,
    truncate:	ver_truncate,
    utime:	ver_utime,
    open:	ver_open,
    read:	ver_read,
    write:	ver_write,
    statfs:	ver_statfs,
    release:	ver_release,
		fsync:  ver_fsync
};

int main(int argc, char **argv)
{
    FILE* settings;
    char* settingsfile;
    int tempint;
    char** parsedargs;
		char* realredirectpath;
#ifdef DEBUG
    
    printf("Main called with %d arguments:\n", argc);
    for (tempint = 0; tempint < argc; tempint++) {
        printf("\t%s\n", argv[tempint]);
    }
#endif
	parsedargs = parseargs(argc, argv);
	if (parsedargs == NULL) {
#ifdef DEBUG
		printf("Invalid arguments!\n");
#endif
		exit(1);
	}
	
	redirectpath = strdup(parsedargs[0]);
	if (!redirectpath) return -1;
	malloced_redir = 1;
	if (redirectpath[0] != '/') {
		realredirectpath = getcwd(NULL, 0);
		realredirectpath = realloc(realredirectpath, strlen(realredirectpath) + strlen(redirectpath) + 3 * sizeof(char));
		strcat(realredirectpath, "/");
		strcat(realredirectpath, redirectpath);
		free(redirectpath);
		redirectpath = realredirectpath;
	}
	if (redirectpath[strlen(redirectpath) - 1] != '/') {
		redirectpath = realloc(redirectpath, strlen(redirectpath) + 2 * sizeof(char));
		redirectpath[strlen(redirectpath) + 1] = '\0';
		redirectpath[strlen(redirectpath)] = '/';
	}
	settingsfile = malloc(sizeof(char[strlen(redirectpath) + 11]));
	sprintf(settingsfile, "%s%s", redirectpath, ".versionfs");
	settings = fopen(settingsfile, "r");
	if (settings) {
		readsettings(settings);
		fclose(settings);
	}
	free(settingsfile);
#ifdef DEBUG
	printf("Using redirection path: %s\n", redirectpath);
#endif
		
#ifdef DEBUG
	printf("New arguments:\n");
	for (tempint = 2; tempint <= atoi(parsedargs[1]) + 1; tempint++) {
		printf("\t%s\n", parsedargs[tempint]);
	}
#endif
	fuse_main(atoi(parsedargs[1]), &(parsedargs[2]), &ver_oper);

	for (tempint = atoi(parsedargs[1]) + 1; tempint >= 0; tempint--) {
		free(parsedargs[tempint]);
	}
    
	free(parsedargs);
    return 0;
}
