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

#define _XOPEN_SOURCE 500

#include "waybackfs.h"
#include <time.h>

void writebuffer(FILE* file, char* buffer, int len) {
    int ret;
#ifdef DEBUG
    printf("Writing %d bytes to file\n", len);
#endif
    while (len > 0) {
	ret = fwrite(buffer, sizeof(char), len, file);
	if (ret <= 0) return;
	len -= ret;
	buffer += ret;
    }
}

void ver_updateversion(char* path, size_t size, off_t offset, char truncate) {
    int originalfile;
    FILE* versionfile;
    char* verpath = malloc(sizeof(char[strlen(path) + strlen(versionmodifier) + 1]));
    char buffer[16384];
    int ret;
    time_t timestamp;
    char extendseof = 0;
    size_t tempsize = size;
    off_t tempoffset = offset;
#ifdef DEBUG
    printf("Creating/updating version file for %s(%s), %d\n", path, versionmodifier, sizeof(off_t));
    if (truncate) {
	printf("Writing info to undo truncate after %d bytes at %d\n", (int)size, (int)offset);
    } else {
        printf("Writing info to undo write of %d bytes to offset %ld\n", (int)size, (int)offset);
    }
#endif
    
    verpath = strcpy(verpath, path);
    verpath = strcat(verpath, versionmodifier);
    
    originalfile = open(path, O_RDONLY);
    versionfile = fopen(verpath, "ab");
    if (!versionfile) versionfile = fopen(verpath, "wb");
    
    free(verpath);

    if (!versionfile) {
	    close(originalfile);
	    return;
    }
    if (originalfile < 0) {
	    fclose(versionfile);
	    return;
    }

    timestamp = time(NULL);
    
    if (truncate) tempsize = 16384;
    while (tempsize > 0) {
#ifdef DEBUG
	printf("\t%d bytes left at offset %ld\n", (int)tempsize, (int)tempoffset);
#endif
	ret = pread(originalfile, buffer, (tempsize > 16384) ? 16384 : tempsize, tempoffset);
	if (ret < 0) {
	    fclose(versionfile);
	    close(originalfile);
	    return;
	}
	if (!truncate) tempsize -= ret;
	tempoffset += ret;
	if (ret == 0) {
	    extendseof = 1;
#ifdef DEBUG
	    printf("\tReaches eof at offset %ld\n", (int)tempoffset);
#endif
	    size = tempoffset - offset;
	    tempsize = 0;
	}
    }
    
    ret = sizeof(time_t) + sizeof(size_t) + sizeof(off_t) + sizeof(char);
    memcpy(buffer, &timestamp, sizeof(time_t));
    memcpy(buffer + sizeof(time_t), &offset, sizeof(off_t));
    memcpy(buffer + sizeof(time_t) + sizeof(off_t), &size, sizeof(size_t));
    memcpy(buffer + sizeof(time_t) + sizeof(off_t) + sizeof(size_t), &extendseof, sizeof(char));
#ifdef DEBUG
    printf("\tWriting data time: %ld, offset: %ld, size: %ld, extendseof: %d\n", (int)timestamp, (int)offset, (int)size, extendseof);
#endif
    writebuffer(versionfile, buffer, ret);
    
    while (size > 0) {
        ret = pread(originalfile, buffer, (size > 16384) ? 16384 : size, offset);
	if (ret < 0) {
	    fclose(versionfile);
	    close(originalfile);
	    return;
	}
	writebuffer(versionfile, buffer, ret);
	if (ferror(versionfile)) {
	    fclose(versionfile);
	    close(originalfile);
	    return;
	}
	size -= ret;
	offset += ret;
    }

    fclose(versionfile);
    close(originalfile);

}

char* getdirectorycatalog(char* path) {
    char* dircatalog = malloc(sizeof(char[strlen(path) + 1]));
    if (!dircatalog) return NULL;
    
    dircatalog = strcpy(dircatalog, path);
    if (dircatalog[strlen(dircatalog) - 1] == '/')
	dircatalog[strlen(dircatalog) - 1] = '\0';
    while (dircatalog[0] != '\0' && dircatalog[strlen(dircatalog) - 1] != '/')
	dircatalog[strlen(dircatalog) - 1] = '\0';
    if (dircatalog[0] == '\0')
	return NULL;
    dircatalog = realloc(dircatalog, sizeof(char[strlen(dircatalog) + strlen(versionmodifier) + 1]));
    if (!dircatalog) return NULL;
    
    dircatalog = strcat(dircatalog, versionmodifier);
    
    return dircatalog;
}

int bufferstats(char* buffer, struct stat* stats, int ret) {
    memcpy(buffer + ret, &stats->st_mode, sizeof(mode_t));
    ret += sizeof(mode_t);
    memcpy(buffer + ret, &stats->st_uid, sizeof(uid_t));
    ret += sizeof(uid_t);
    memcpy(buffer + ret, &stats->st_gid, sizeof(gid_t));
    ret += sizeof(gid_t);
    memcpy(buffer + ret, &stats->st_atime, sizeof(time_t));
    ret += sizeof(time_t);
    memcpy(buffer + ret, &stats->st_mtime, sizeof(time_t));
    ret += sizeof(time_t);
    memcpy(buffer + ret, &stats->st_ctime, sizeof(time_t));
    ret += sizeof(time_t);
    return ret;
}

char* makerelative(char* source) {
    char* needle = strstr(source, "/");
    if (!needle) return source;
    source = &needle[1];
    while (source) {
        needle = source;
        source = strstr(source, "/");
	if (source) source = &source[1];
    }
    return needle;
}

/* Actions to handle (X if completed):
 *X Create  (file, link (extra arg is destination))
 *X Rm	    (file, link) extra arg is link destination - stat struct
 *X Mkdir   no extra arg
 *X Rmdir   extra arg is deleted dir name
 *X Rename  extra arg is destination name
 *X Chmod   (also chown and utime) no extra arg - stat struct
 */
void ver_updatedirectory(int action, char* path, char* extra, struct stat* stats) {
    FILE* catalogfile;
    char buffer[1024];
    char* catalogpath = getdirectorycatalog(path);
    int ret = 0;
    time_t timestamp;
    
    IFDEBUG(printf("Creating/updating directory catalog for action %d to %s\n", action, path));
    
    if (!catalogpath) return;
    
    IFDEBUG(printf("\tDirectory catalog file is %s\n", catalogpath));
    catalogfile = fopen(catalogpath, "ab");
    if (!catalogfile) catalogfile = fopen(catalogpath, "wb");
    
    free(catalogpath);

    if (!catalogfile) return;

    timestamp = time(NULL);
    if (path) path = makerelative(path);
    if (extra && action != VUD_RM) extra = makerelative(extra);
    
    memcpy(buffer + ret, &timestamp, sizeof(time_t));
    ret += sizeof(time_t);
    memcpy(buffer + ret, &action, sizeof(int));
    ret += sizeof(int);
    writebuffer(catalogfile, buffer, ret);

    switch (action) {
	case VUD_CREATE:
	case VUD_MKDIR:
	    IFDEBUG(printf("\tWriting undo info for create or mkdir\n"));
	    ret = strlen(path) + 1;
	    writebuffer(catalogfile, (char*)&ret, sizeof(int));
	    writebuffer(catalogfile, path, ret);
	    break;
	case VUD_RM:
	    IFDEBUG(printf("\tWriting undo info for rm\n"));
	    ret = 0;
	    ret = bufferstats(buffer, stats, ret);
	    if (S_ISLNK(stats->st_mode)) {
		ret += strlen(path) + 1;
		ret += strlen(extra) + 1;
		writebuffer(catalogfile, (char*)&ret, sizeof(int));
		writebuffer(catalogfile, buffer, ret - strlen(path) - strlen(extra) - 2);
		writebuffer(catalogfile, path, strlen(path) + 1);
		writebuffer(catalogfile, extra, strlen(extra) + 1);
	    } else {
		ret += strlen(path) + 1;
		writebuffer(catalogfile, (char*)&ret, sizeof(int));
		writebuffer(catalogfile, buffer, ret - strlen(path) - 1);
		writebuffer(catalogfile, path, strlen(path) + 1);
	    }
	    break;
	case VUD_RMDIR:
	case VUD_RENAME:
	    IFDEBUG(printf("\tWriting undo info for rmdir or rename\n"));
	    ret = strlen(path) + strlen(extra) + 2;
	    writebuffer(catalogfile,(char*) &ret, sizeof(int));
	    writebuffer(catalogfile, path, strlen(path) + 1);
	    writebuffer(catalogfile, extra, strlen(extra) + 1);
	    break;
	case VUD_CHMOD:
	case VUD_CHOWN:
	case VUD_UTIME:
	    IFDEBUG(printf("\tWriting unfo info for chmod, chown, or utime\n"));
	    ret = 0;
	    ret = bufferstats(buffer, stats, ret);
	    ret += strlen(path) + 1;
	    writebuffer(catalogfile, (char*)&ret, sizeof(int));
	    writebuffer(catalogfile, buffer, ret - strlen(path) - 1);
	    writebuffer(catalogfile, path, strlen(path) + 1);
	    break;
	default:
	    IFDEBUG(printf("\tAction %d unknown!!! No undo info written!\n", action));
    }

    fclose(catalogfile);
}

char* ver_deletedpath(char* path) {
    char* deletedpath = malloc(sizeof(char[strlen(path) + strlen(deletedmodifier) + 1]));
    char* newdeletedpath = NULL;
    int res;
    int addedint = 1;
    struct stat* stats = NULL;
    if (!deletedpath) return NULL;

    if (path[strlen(path) - 1] == '/') path[strlen(path) - 1] = '\0';
    
    deletedpath = strcpy(deletedpath, path);
    deletedpath = strcat(deletedpath, deletedmodifier);
    if (!deletedpath) return NULL;

    stats = malloc(sizeof(struct stat));
    
    res = lstat(deletedpath, stats);
    if (res == -1 && errno == ENOENT) {free(stats); return deletedpath;}
    while (res == 0 || errno == EACCES) {
	    newdeletedpath = realloc(newdeletedpath, sizeof(char[strlen(deletedpath) + (addedint / 10) + 2]));
	    if (!newdeletedpath) {free(deletedpath); free(stats); return NULL;}
	    
	    sprintf(newdeletedpath, "%s%d", deletedpath, addedint++);
	    res = lstat(newdeletedpath, stats);
    }
    free(deletedpath);
    free(stats);
    if (errno == ENOENT) {
	    return newdeletedpath;
    } else {
	    free(newdeletedpath);
	    return NULL;
    }
}

char* gotovalue(char* parseline) {
    while (parseline[0] == ' ' || parseline[0] == '=' || parseline[0] == '\t') {
	parseline++;
    }
    strtok(parseline, "\n");
    return parseline;
}

void readsettings(FILE* settings) {
    char fileline[256];
    char* parseline;
    while (fgets(fileline, 256, settings)) {
	if (fileline[0] != '#') {
	    parseline = fileline;
	    while (parseline[0] == ' ' || parseline[0] == '\t') {
		parseline++;
	    }
	    if (!strncmp(parseline, "deletedmodifier", 12)) {
		parseline += 12;
		parseline = gotovalue(parseline);
		if (malloced_deleted == 1) free(deletedmodifier);
		deletedmodifier = malloc(sizeof(char[strlen(parseline) + 1]));
		if (!deletedmodifier) return;
		malloced_deleted = 1;
		strcpy(deletedmodifier, parseline);
	    } else if (!strncmp(parseline, "versionmodifier", 15)) {
		parseline += 15;
		parseline = gotovalue(parseline);
		if (malloced_ver == 1) free(versionmodifier);
		versionmodifier = malloc(sizeof(char[strlen(parseline) + 1]));
		if (!versionmodifier) return;
		malloced_ver = 1;
		strcpy(versionmodifier, parseline);
	    } else {
		fprintf(stderr, "Unknown line in config file: %s\n", parseline);
	    }
	}
    }
}

char** parseargs(int argc, char** argv) {
	char** parsed = NULL;
	int i;
	int parsednum = 0;

	// Find all non-option arguments
	for (i = 0; i < argc; i++) {
		if (argv[i][0] != '-') {
			parsed = realloc(parsed, (parsednum + 1) * sizeof(char*));
			parsed[parsednum] = strdup(argv[i]);
			parsednum++;
		}
	}
	
	// Three arguments, program name, source path, destination path
	if (parsednum != 3) {
		for (i = 0; i < parsednum; i++) {
			free(parsed[i]);
		}
		free(parsed);
		return NULL;
	}

	parsed = realloc(parsed, 6 * sizeof(char*));
	parsednum += 3;

	// Reorder to source path, placeholder, program name, destination path
	parsed[3] = parsed[2];
	parsed[2] = parsed[0];
	parsed[0] = parsed[1];
	parsed[4] = strdup("-s"); // Add the -s option to disable multithreading
	parsed[5] = strdup("-s"); // For some reason, this makes having options
	                          // to fusermount work
	for (i = 0; i < argc; i++) {
		if (argv[i][0] == '-') {
			parsed = realloc(parsed, (parsednum + 1) * sizeof(char*));
			parsed[parsednum] = strdup(argv[i]);
			parsednum++;
		}
	}

	// Put argument count in placeholder spot
	parsed[1] = malloc(16);
	snprintf(parsed[1], 15, "%d", parsednum - 2);
	
	return parsed;
}


