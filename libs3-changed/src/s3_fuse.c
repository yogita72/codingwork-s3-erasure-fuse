/*
  Big Brother File System

  The point of this FUSE filesystem is to provide an introduction to
  FUSE.  It was my first FUSE filesystem as I got to know the
  software; hopefully, the comments in this code will help people who
  follow later to get a gentler introduction.

  This might be called a no-op filesystem:  it doesn't impose
  filesystem semantics on top of any other existing structure.  It
  simply reports the requests that come in, and passes them to an
  underlying filesystem.  The information is saved in a logfile named
  s3_fuse_fs.log, in the directory from which you run s3_fuse_fs.

  gcc -Wall `pkg-config fuse --cflags --libs` -o s3_fuse_fs s3_fuse_fs.c
*/

#include "params.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>

#include "log.h"
#include "s3_fuse_bridge.h"
#include "s3_erasure_code.h"

// Report errors to logfile and give -errno to caller
static int s3_fuse_error(char *str)
{
    int ret = -errno;
    
    log_msg("    ERROR %s: %s\n", str, strerror(errno));
    
    return ret;
}

// Check whether the given user is permitted to perform the given operation on the given 

//  All the paths I see are relative to the root of the mounted
//  filesystem.  In order to get to the underlying filesystem, I need to
//  have the mountpoint.  I'll save it away early on in main(), and then
//  whenever I need a path for something I'll call this to construct
//  it.
static void s3_fuse_fullpath(char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, S3_FUSE_DATA->cache->location);
    strncat(fpath, path, PATH_MAX); // ridiculously long paths will
				    // break here

    log_msg("    s3_fuse_fullpath:  cache = \"%s\", path = \"%s\", fpath = \"%s\"\n",
	    S3_FUSE_DATA->cache->location, path, fpath);
}

///////////////////////////////////////////////////////////
//
// Prototypes for all these functions, and the C-style comments,
// come indirectly from /usr/include/fuse.h
//
/** Get file attributes.
 *
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.  The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 */
int s3_fuse_getattr(const char *path, struct stat *statbuf)
{
    int retstat = 0;
    char fpath[PATH_MAX];
	s3_tree_node	*node = NULL;
    
    log_msg("\ns3_fuse_getattr(path=\"%s\", statbuf=0x%08x)\n",
	  path, statbuf);


	retstat = searchAndInsertPathInTree(path, &(S3_FUSE_DATA->dirTree), &node, 1 );
    
	if( (retstat == 0 ) && (node != NULL)) {
		if( node->s3FileInfo->size == -1 ) {
			statbuf->st_mode = S_IFDIR | 0755 ;
			statbuf->st_nlink = 2;
			statbuf->st_atime = node->s3FileInfo->time;	
			statbuf->st_mtime = node->s3FileInfo->time;	
			statbuf->st_ctime = node->s3FileInfo->time;	
		} else {
			statbuf->st_mode = S_IFREG | 0755 ;
			statbuf->st_nlink = 1;
			statbuf->st_atime = node->s3FileInfo->time;	
			statbuf->st_mtime = node->s3FileInfo->time;	
			statbuf->st_ctime = node->s3FileInfo->time;	
			statbuf->st_size = node->s3FileInfo->size;	

		}
    log_stat(statbuf);
	} else {

    		s3_fuse_fullpath(fpath, path);
    
    		retstat = lstat(fpath, statbuf);
    		if (retstat != 0)
		retstat = s3_fuse_error("s3_fuse_getattr lstat");
	}

    
    return retstat;
}

/** Read the target of a symbolic link
 *
 * The buffer should be filled with a null terminated string.  The
 * buffer size argument includes the space for the terminating
 * null character.  If the linkname is too long to fit in the
 * buffer, it should be truncated.  The return value should be 0
 * for success.
 */
// Note the system readlink() will truncate and lose the terminating
// null.  So, the size passed to to the system readlink() must be one
// less than the size passed to s3_fuse_readlink()
// s3_fuse_readlink() code by Bernardo F Costa (thanks!)
#if 0
int s3_fuse_readlink(const char *path, char *link, size_t size)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("s3_fuse_readlink(path=\"%s\", link=\"%s\", size=%d)\n",
	  path, link, size);
    s3_fuse_fullpath(fpath, path);
    
    retstat = readlink(fpath, link, size - 1);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_readlink readlink");
    else  {
	link[retstat] = '\0';
	retstat = 0;
    }
    
    return retstat;
}

#endif

/** Create a file node
 *
 * There is no create() operation, mknod() will be called for
 * creation of all non-directory, non-symlink nodes.
 */
// shouldn't that comment be "if" there is no.... ?
int s3_fuse_mknod(const char *path, mode_t mode, dev_t dev)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\ns3_fuse_mknod(path=\"%s\", mode=0%3o, dev=%lld)\n",
	  path, mode, dev);
    s3_fuse_fullpath(fpath, path);
    
    // On Linux this could just be 'mknod(path, mode, rdev)' but this
    //  is more portable
    if (S_ISREG(mode)) {
        retstat = open(fpath, O_CREAT | O_EXCL | O_WRONLY, mode);
	if (retstat < 0)
	    retstat = s3_fuse_error("s3_fuse_mknod open");
        else {
            retstat = close(retstat);
	    if (retstat < 0)
		retstat = s3_fuse_error("s3_fuse_mknod close");
	}
    } else
	if (S_ISFIFO(mode)) {
	    retstat = mkfifo(fpath, mode);
	    if (retstat < 0)
		retstat = s3_fuse_error("s3_fuse_mknod mkfifo");
	} else {
	    retstat = mknod(fpath, mode, dev);
	    if (retstat < 0)
		retstat = s3_fuse_error("s3_fuse_mknod mknod");
	}
    
    return retstat;
}

/** Create a directory */
int s3_fuse_mkdir(const char *path, mode_t mode)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\ns3_fuse_mkdir(path=\"%s\", mode=0%3o)\n",
	    path, mode);

	retstat = addDirectory(path);

	if( retstat == 0 ) {
    	s3_fuse_fullpath(fpath, path);
    
    	retstat = mkdir(fpath, mode);
    	if (retstat < 0)
		retstat = s3_fuse_error("s3_fuse_mkdir mkdir");
    
	} else {

		retstat = s3_fuse_error("s3_fuse_mkdir addDirectory error");
	}
    return retstat;
}

/** Remove a file */
int s3_fuse_unlink(const char *path)
{
    int retstat = 0;
    char fpath[PATH_MAX];
	int	inCache = 0;

    log_msg("s3_fuse_unlink(path=\"%s\")\n",
	    path);

	retstat = deletePath(path);
    
	retstat = s3CacheInCache(S3_FUSE_DATA->cache, path, &inCache);
	if(retstat != 0 ) {
		log_msg("InCache error\n");
		return retstat;
	}
	if( inCache == 1 ) {
    s3_fuse_fullpath(fpath, path);
    retstat = unlink(fpath);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_unlink unlink");
   	} 
    return retstat;
}

/** Remove a directory */
int s3_fuse_rmdir(const char *path)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("s3_fuse_rmdir(path=\"%s\")\n",
	    path);

	retstat = deletePath(path);
/*
    s3_fuse_fullpath(fpath, path);
    
    retstat = rmdir(fpath);
*/
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_rmdir rmdir");
    
    return retstat;
}

/** Create a symbolic link */
// The parameters here are a little bit confusing, but do correspond
// to the symlink() system call.  The 'path' is where the link points,
// while the 'link' is the link itself.  So we need to leave the path
// unaltered, but insert the link into the mounted directory.

#if 0
int s3_fuse_symlink(const char *path, const char *link)
{
    int retstat = 0;
    char flink[PATH_MAX];
    
    log_msg("\ns3_fuse_symlink(path=\"%s\", link=\"%s\")\n",
	    path, link);
    s3_fuse_fullpath(flink, link);
    
    retstat = symlink(path, flink);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_symlink symlink");
    
    return retstat;
}
#endif

/** Rename a file */
// both path and newpath are fs-relative
int s3_fuse_rename(const char *path, const char *newpath)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    char fnewpath[PATH_MAX];
    
    log_msg("\ns3_fuse_rename(fpath=\"%s\", newpath=\"%s\")\n",
	    path, newpath);
    s3_fuse_fullpath(fpath, path);
    s3_fuse_fullpath(fnewpath, newpath);
    
    retstat = rename(fpath, fnewpath);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_rename rename");
    
    return retstat;
}

/** Create a hard link to a file */
int s3_fuse_link(const char *path, const char *newpath)
{
    int retstat = 0;
    char fpath[PATH_MAX], fnewpath[PATH_MAX];
    
    log_msg("\ns3_fuse_link(path=\"%s\", newpath=\"%s\")\n",
	    path, newpath);
    s3_fuse_fullpath(fpath, path);
    s3_fuse_fullpath(fnewpath, newpath);
    
    retstat = link(fpath, fnewpath);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_link link");
    
    return retstat;
}

/** Change the permission bits of a file */
int s3_fuse_chmod(const char *path, mode_t mode)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\ns3_fuse_chmod(fpath=\"%s\", mode=0%03o)\n",
	    path, mode);
    s3_fuse_fullpath(fpath, path);
    
    retstat = chmod(fpath, mode);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_chmod chmod");
    
    return retstat;
}

/** Change the owner and group of a file */
int s3_fuse_chown(const char *path, uid_t uid, gid_t gid)
  
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\ns3_fuse_chown(path=\"%s\", uid=%d, gid=%d)\n",
	    path, uid, gid);
    s3_fuse_fullpath(fpath, path);
    
    retstat = chown(fpath, uid, gid);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_chown chown");
    
    return retstat;
}

/** Change the size of a file */
int s3_fuse_truncate(const char *path, off_t newsize)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\ns3_fuse_truncate(path=\"%s\", newsize=%lld)\n",
	    path, newsize);
    s3_fuse_fullpath(fpath, path);
    
    retstat = truncate(fpath, newsize);
    if (retstat < 0)
	s3_fuse_error("s3_fuse_truncate truncate");
    
    return retstat;
}

/** Change the access and/or modification times of a file */
/* note -- I'll want to change this as soon as 2.6 is in debian testing */
int s3_fuse_utime(const char *path, struct utimbuf *ubuf)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\ns3_fuse_utime(path=\"%s\", ubuf=0x%08x)\n",
	    path, ubuf);
    s3_fuse_fullpath(fpath, path);
    
    retstat = utime(fpath, ubuf);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_utime utime");
    
    return retstat;
}

/** File open operation
 *
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  Optionally open may also
 * return an arbitrary filehandle in the fuse_file_info structure,
 * which will be passed to all file operations.
 *
 * Changed in version 2.2
 */
int s3_fuse_open(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    int fd;
    char fpath[PATH_MAX];
	int	inCache = 0;
    
    log_msg("\ns3_fuse_open(path\"%s\", fi=0x%08x)\n",
	    path, fi);

	retstat = s3CacheInCache(S3_FUSE_DATA->cache, path, &inCache);
	if(retstat != 0 ) {
		log_msg("InCache error\n");
		return retstat;
	}
	if( inCache == 0 ) {
		retstat = s3CacheFetch(S3_FUSE_DATA->cache, path);
		if(retstat != 0 ) {
			log_msg("Fetch error\n");
			return retstat;
		}
	}

    s3_fuse_fullpath(fpath, path);
    
    fd = open(fpath, fi->flags);
    if (fd < 0)
	retstat = s3_fuse_error("s3_fuse_open open");
    
    fi->fh = fd;
    log_fi(fi);
    
    return retstat;
}

/** Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the
 * 'direct_io' mount option is specified, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * Changed in version 2.2
 */
// I don't fully understand the documentation above -- it doesn't
// match the documentation for the read() system call which says it
// can return with anything up to the amount of data requested. nor
// with the fusexmp code which returns the amount of data also
// returned by read.
int s3_fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\ns3_fuse_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);
    
    retstat = pread(fi->fh, buf, size, offset);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_read read");
    
    return retstat;
}

/** Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the 'direct_io'
 * mount option is specified (see read operation).
 *
 * Changed in version 2.2
 */
// As  with read(), the documentation above is inconsistent with the
// documentation for the write() system call.
int s3_fuse_write(const char *path, const char *buf, size_t size, off_t offset,
	     struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\ns3_fuse_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi
	    );
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);
	
    retstat = pwrite(fi->fh, buf, size, offset);
    if (retstat < 0) {
	retstat = s3_fuse_error("s3_fuse_write pwrite");
	} else {
		s3CacheMarkForFlush(S3_FUSE_DATA->cache, path);

	}    


    return retstat;
}

/** Get file system statistics
 *
 * The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
 *
 * Replaced 'struct statfs' parameter with 'struct statvfs' in
 * version 2.5
 */
int s3_fuse_statfs(const char *path, struct statvfs *statv)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\ns3_fuse_statfs(path=\"%s\", statv=0x%08x)\n",
	    path, statv);
    s3_fuse_fullpath(fpath, path);
    
    // get stats for underlying filesystem
    retstat = statvfs(fpath, statv);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_statfs statvfs");
    
    log_statvfs(statv);
    
    return retstat;
}

/** Possibly flush cached data
 *
 * BIG NOTE: This is not equivalent to fsync().  It's not a
 * request to sync dirty data.
 *
 * Flush is called on each close() of a file descriptor.  So if a
 * filesystem wants to return write errors in close() and the file
 * has cached dirty data, this is a good place to write back data
 * and return any errors.  Since many applications ignore close()
 * errors this is not always useful.
 *
 * NOTE: The flush() method may be called more than once for each
 * open().  This happens if more than one file descriptor refers
 * to an opened file due to dup(), dup2() or fork() calls.  It is
 * not possible to determine if a flush is final, so each flush
 * should be treated equally.  Multiple write-flush sequences are
 * relatively rare, so this shouldn't be a problem.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * Changed in version 2.2
 */
int s3_fuse_flush(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\ns3_fuse_flush(path=\"%s\", fi=0x%08x)\n", path, fi);
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);
	
	s3CacheFlushCache(S3_FUSE_DATA->cache, path);
    return retstat;
}

/** Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 *
 * Changed in version 2.2
 */
int s3_fuse_release(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\ns3_fuse_release(path=\"%s\", fi=0x%08x)\n",
	  path, fi);
    log_fi(fi);

    // We need to close the file.  Had we allocated any resources
    // (buffers etc) we'd need to free them here as well.
    retstat = close(fi->fh);
    
    return retstat;
}

/** Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * Changed in version 2.2
 */
int s3_fuse_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\ns3_fuse_fsync(path=\"%s\", datasync=%d, fi=0x%08x)\n",
	    path, datasync, fi);
    log_fi(fi);
    
    if (datasync)
	retstat = fdatasync(fi->fh);
    else
	retstat = fsync(fi->fh);
    
    if (retstat < 0)
	s3_fuse_error("s3_fuse_fsync fsync");
    
    return retstat;
}

/** Set extended attributes */
int s3_fuse_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\ns3_fuse_setxattr(path=\"%s\", name=\"%s\", value=\"%s\", size=%d, flags=0x%08x)\n",
	    path, name, value, size, flags);
    s3_fuse_fullpath(fpath, path);
    
    retstat = lsetxattr(fpath, name, value, size, flags);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_setxattr lsetxattr");
    
    return retstat;
}

/** Get extended attributes */
int s3_fuse_getxattr(const char *path, const char *name, char *value, size_t size)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\ns3_fuse_getxattr(path = \"%s\", name = \"%s\", value = 0x%08x, size = %d)\n",
	    path, name, value, size);
    s3_fuse_fullpath(fpath, path);
    
    retstat = lgetxattr(fpath, name, value, size);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_getxattr lgetxattr");
    else
	log_msg("    value = \"%s\"\n", value);
    
    return retstat;
}

/** List extended attributes */
int s3_fuse_listxattr(const char *path, char *list, size_t size)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    char *ptr;
    
    log_msg("s3_fuse_listxattr(path=\"%s\", list=0x%08x, size=%d)\n",
	    path, list, size
	    );
    s3_fuse_fullpath(fpath, path);
    
    retstat = llistxattr(fpath, list, size);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_listxattr llistxattr");
    
    log_msg("    returned attributes (length %d):\n", retstat);
    for (ptr = list; ptr < list + retstat; ptr += strlen(ptr)+1)
	log_msg("    \"%s\"\n", ptr);
    
    return retstat;
}

/** Remove extended attributes */
int s3_fuse_removexattr(const char *path, const char *name)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\ns3_fuse_removexattr(path=\"%s\", name=\"%s\")\n",
	    path, name);
    s3_fuse_fullpath(fpath, path);
    
    retstat = lremovexattr(fpath, name);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_removexattr lrmovexattr");
    
    return retstat;
}

/** Open directory
 *
 * This method should check if the open operation is permitted for
 * this  directory
 *
 * Introduced in version 2.3
 */
int s3_fuse_opendir(const char *path, struct fuse_file_info *fi)
{
    DIR *dp;
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\ns3_fuse_opendir(path=\"%s\", fi=0x%08x)\n",
	  path, fi);
    s3_fuse_fullpath(fpath, path);
    
    dp = opendir(fpath);
    if (dp == NULL)
	retstat = s3_fuse_error("s3_fuse_opendir opendir");
    
    fi->fh = (intptr_t) dp;
    
    log_fi(fi);
    
    return retstat;
}

/** Read directory
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 *
 * Introduced in version 2.3
 */
int s3_fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
	       struct fuse_file_info *fi)
{
    int retstat = 0;
	s3_tree_node	*node=NULL;
	s3_tree_node 	*child=NULL;
	char			*childName = NULL;
    
    log_msg("\ns3_fuse_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n",
	    path, buf, filler, offset, fi);

	retstat = searchAndInsertPathInTree(path, &(S3_FUSE_DATA->dirTree), &node, 1 );

	if((retstat == 0 ) && (node != NULL)) {
	child = node->children;
	
    while(child != NULL) {
	childName = child->s3FileInfo->name;
	log_msg("calling filler with name %s\n", childName);
	if (filler(buf, childName, NULL, 0) != 0) {
	    log_msg("    ERROR s3_fuse_readdir filler:  buffer full");
	    return -ENOMEM;
	}
	child = child->next;
    }
  	} 
    log_fi(fi);
    
    return retstat;
}

#if 0
{
    int retstat = 0;
    DIR *dp;
    struct dirent *de;
    char fpath[PATH_MAX];
    
    log_msg("\ns3_fuse_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n",
	    path, buf, filler, offset, fi);
    // once again, no need for fullpath -- but note that I need to cast fi->fh
    //dp = (DIR *) (uintptr_t) fi->fh;
    //
	if( strcmp(path, "/") == 0 ) {
		int		count =0 ;
		s3_file_info  	*s3FileInfoList = NULL;
		int		i=0;
        list_service(0, &count, &s3FileInfoList);
		
		log_msg("num of buckets %d\n", count);
		for(i=0; i< count; i++ ) {

		log_msg(" bucket %d : %s\n", i, s3FileInfoList[i].name);
		log_msg("calling filler with name %s\n", s3FileInfoList[i].name);
		if (filler(buf, s3FileInfoList[i].name, NULL, 0) != 0) {
	    log_msg("    ERROR s3_fuse_readdir filler:  buffer full");
	    return -ENOMEM;
	}

		}
	} else {
		char			bucket[PATH_MAX] = "test-yogita-bijani";
		char			prefix[PATH_MAX] = "test";
		int				count=0;
		s3_file_info	*s3FileInfoList = NULL;
		
		list_bucket(bucket, prefix, NULL, NULL, 1000, 0, &count, &s3FileInfoList ) ;
	
	
    s3_fuse_fullpath(fpath, path);
    dp = opendir(fpath);
    if (dp == NULL) {
		retstat = s3_fuse_error("s3_fuse_opendir opendir");
    	return retstat;
	}
    // Every directory contains at least two entries: . and ..  If my
    // first call to the system readdir() returns NULL I've got an
    // error; near as I can tell, that's the only condition under
    // which I can get an error from readdir()
    de = readdir(dp);
    if (de == 0) {
	retstat = s3_fuse_error("s3_fuse_readdir readdir");
	return retstat;
    }

    // This will copy the entire directory into the buffer.  The loop exits
    // when either the system readdir() returns NULL, or filler()
    // returns something non-zero.  The first case just means I've
    // read the whole directory; the second means the buffer is full.
    do {
	log_msg("calling filler with name %s\n", de->d_name);
	if (filler(buf, de->d_name, NULL, 0) != 0) {
	    log_msg("    ERROR s3_fuse_readdir filler:  buffer full");
	    return -ENOMEM;
	}
    } while ((de = readdir(dp)) != NULL);
 	}   
    log_fi(fi);
    
    return retstat;
}

#endif
/** Release directory
 *
 * Introduced in version 2.3
 */
int s3_fuse_releasedir(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\ns3_fuse_releasedir(path=\"%s\", fi=0x%08x)\n",
	    path, fi);
    log_fi(fi);
    
    closedir((DIR *) (uintptr_t) fi->fh);
    
    return retstat;
}

/** Synchronize directory contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data
 *
 * Introduced in version 2.3
 */
// when exactly is this called?  when a user calls fsync and it
// happens to be a directory? ???
int s3_fuse_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\ns3_fuse_fsyncdir(path=\"%s\", datasync=%d, fi=0x%08x)\n",
	    path, datasync, fi);
    log_fi(fi);
    
    return retstat;
}

/**
 * Initialize filesystem
 *
 * The return value will passed in the private_data field of
 * fuse_context to all file operations and as a parameter to the
 * destroy() method.
 *
 * Introduced in version 2.3
 * Changed in version 2.6
 */
// Undocumented but extraordinarily useful fact:  the fuse_context is
// set up before this function is called, and
// fuse_get_context()->private_data returns the user_data passed to
// fuse_main().  Really seems like either it should be a third
// parameter coming in here, or else the fact should be documented
// (and this might as well return void, as it did in older versions of
// FUSE).
void *s3_fuse_init(struct fuse_conn_info *conn)
{
    log_msg("\ns3_fuse_init()\n");
    
    return S3_FUSE_DATA;
}

/**
 * Clean up filesystem
 *
 * Called on filesystem exit.
 *
 * Introduced in version 2.3
 */
void s3_fuse_destroy(void *userdata)
{
    log_msg("\ns3_fuse_destroy(userdata=0x%08x)\n", userdata);
}

/**
 * Check file access permissions
 *
 * This will be called for the access() system call.  If the
 * 'default_permissions' mount option is given, this method is not
 * called.
 *
 * This method is not called under Linux kernel versions 2.4.x
 *
 * Introduced in version 2.5
 */
int s3_fuse_access(const char *path, int mask)
{
    int retstat = 0;
    char fpath[PATH_MAX];
   
    log_msg("\ns3_fuse_access(path=\"%s\", mask=0%o)\n",
	    path, mask);
    s3_fuse_fullpath(fpath, path);
    
    retstat = access(fpath, mask);
    
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_access access");
    
    return retstat;
}

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * Introduced in version 2.5
 */
int s3_fuse_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    int fd;
	char *tmp = NULL;
    
    log_msg("\ns3_fuse_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n",
	    path, mode, fi);
    s3_fuse_fullpath(fpath, path);
    
	tmp = strrchr(fpath, '/');
	*tmp = 0;
	mkpath(fpath);
	*tmp = '/';
    fd = creat(fpath, mode);
    if (fd < 0)
	retstat = s3_fuse_error("s3_fuse_create creat");
    
    fi->fh = fd;
    
    log_fi(fi);
    
    return retstat;
}

/**
 * Change the size of an open file
 *
 * This method is called instead of the truncate() method if the
 * truncation was invoked from an ftruncate() system call.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the truncate() method will be
 * called instead.
 *
 * Introduced in version 2.5
 */
int s3_fuse_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\ns3_fuse_ftruncate(path=\"%s\", offset=%lld, fi=0x%08x)\n",
	    path, offset, fi);
    log_fi(fi);
    
    retstat = ftruncate(fi->fh, offset);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_ftruncate ftruncate");
    
    return retstat;
}

/**
 * Get attributes from an open file
 *
 * This method is called instead of the getattr() method if the
 * file information is available.
 *
 * Currently this is only called after the create() method if that
 * is implemented (see above).  Later it may be called for
 * invocations of fstat() too.
 *
 * Introduced in version 2.5
 */
// Since it's currently only called after s3_fuse_create(), and s3_fuse_create()
// opens the file, I ought to be able to just use the fd and ignore
// the path...
int s3_fuse_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\ns3_fuse_fgetattr(path=\"%s\", statbuf=0x%08x, fi=0x%08x)\n",
	    path, statbuf, fi);
    log_fi(fi);
    
    retstat = fstat(fi->fh, statbuf);
    if (retstat < 0)
	retstat = s3_fuse_error("s3_fuse_fgetattr fstat");
    
    log_stat(statbuf);
    
    return retstat;
}

struct fuse_operations s3_fuse_oper = {

  .getattr = s3_fuse_getattr,
  .readdir = s3_fuse_readdir,
  .init = s3_fuse_init,
  .open = s3_fuse_open,
  .read = s3_fuse_read,
  .create = s3_fuse_create,
  .write = s3_fuse_write,
  .flush = s3_fuse_flush,
  .chmod = s3_fuse_chmod,
  .chown = s3_fuse_chown,
  .utime = s3_fuse_utime,
  .truncate = s3_fuse_truncate,
  .unlink = s3_fuse_unlink,
  .rmdir = s3_fuse_rmdir,
  .mkdir = s3_fuse_mkdir

};

#if 0
struct fuse_operations s3_fuse_oper = {
  .getattr = s3_fuse_getattr,
//  .readlink = s3_fuse_readlink,
  // no .getdir -- that's deprecated
  .getdir = NULL,
  .mknod = s3_fuse_mknod,
  .mkdir = s3_fuse_mkdir,
  .unlink = s3_fuse_unlink,
  .rmdir = s3_fuse_rmdir,
//  .symlink = s3_fuse_symlink,
  .rename = s3_fuse_rename,
  .link = s3_fuse_link,
  .chmod = s3_fuse_chmod,
  .chown = s3_fuse_chown,
  .truncate = s3_fuse_truncate,
  .utime = s3_fuse_utime,
  .open = s3_fuse_open,
  .read = s3_fuse_read,
  .write = s3_fuse_write,
  /** Just a placeholder, don't set */ // huh???
  .statfs = s3_fuse_statfs,
  .flush = s3_fuse_flush,
  .release = s3_fuse_release,
  .fsync = s3_fuse_fsync,
  .setxattr = s3_fuse_setxattr,
  .getxattr = s3_fuse_getxattr,
  .listxattr = s3_fuse_listxattr,
  .removexattr = s3_fuse_removexattr,
  .opendir = s3_fuse_opendir,
  .readdir = s3_fuse_readdir,
  .releasedir = s3_fuse_releasedir,
  .fsyncdir = s3_fuse_fsyncdir,
  .init = s3_fuse_init,
  .destroy = s3_fuse_destroy,
  .access = s3_fuse_access,
  .create = s3_fuse_create,
  .ftruncate = s3_fuse_ftruncate,
  .fgetattr = s3_fuse_fgetattr
};
#endif

void s3_fuse_usage()
{
    fprintf(stderr, "usage:  s3_fuse_fs rootDir mountPoint\n");
    abort();
}

int main(int argc, char *argv[])
{
    int i, ret;
    int fuse_stat;
    struct s3_fuse_state *s3_fuse_data;
	char	*cacheLocation;

    // s3_fuse_fs doesn't do any access checking on its own (the comment
    // blocks in fuse.h mention some of the functions that need
    // accesses checked -- but note there are other functions, like
    // chown(), that also need checking!).  Since running s3_fuse_fs as root
    // will therefore open Metrodome-sized holes in the system
    // security, we'll check if root is trying to mount the filesystem
    // and refuse if it is.  The somewhat smaller hole of an ordinary
    // user doing it with the allow_other flag is still there because
    // I don't want to parse the options string.
    if ((getuid() == 0) || (geteuid() == 0)) {
	fprintf(stderr, "Running S3_FUSE_FS as root opens unnacceptable security holes\n");
	return 1;
    }

    s3_fuse_data = calloc(sizeof(struct s3_fuse_state), 1);
    if (s3_fuse_data == NULL) {
	perror("main calloc");
	abort();
    }
    
    s3_fuse_data->logfile = log_open();
    
    // libfuse is able to do most of the command line parsing; all I
    // need to do is to extract the cache; this will be the first
    // non-option passed in.  I'm using the GNU non-standard extension
    // and having realpath malloc the space for the path
    // the string.
    for (i = 1; (i < argc) && (argv[i][0] == '-'); i++)
	if (argv[i][1] == 'o') i++; // -o takes a parameter; need to
				    // skip it too.  This doesn't
				    // handle "squashed" parameters
    
    if ((argc - i) != 2) s3_fuse_usage();
    
    fprintf(stderr, "cache path = %s\n", argv[i]);

    cacheLocation = realpath(argv[i], NULL);
    fprintf(stderr, "cache realpath = %s\n", cacheLocation);
	ret = s3CacheInit(&(s3_fuse_data->cache), cacheLocation) ;
	if( ret != 0 ) {
		return 1;
	}

    argv[i] = argv[i+1];
    argc--;

	ret = saveSecurityCredentials();
	if( ret != 0 ) {
		return 1;
	}

	ret = saveExecuteDir();
	if( ret != 0 ) {
		return 1;
	}


	ret = saveErasurePolicy();
	if( ret != 0 ) {
		return 1;
	}

    fprintf(stderr, "about to call fuse_main\n");
    fuse_stat = fuse_main(argc, argv, &s3_fuse_oper, s3_fuse_data);
    fprintf(stderr, "fuse_main returned %d\n", fuse_stat);
    
    return fuse_stat;
}
