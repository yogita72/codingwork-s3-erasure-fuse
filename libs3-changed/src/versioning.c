
#include <stdlib.h> 
#include <string.h> 
#include "libs3.h"
#include "request.h"

// set versioning -----------------------------------------------------------
typedef struct SetVersioningData
{
    S3ResponsePropertiesCallback *responsePropertiesCallback;
    S3ResponseCompleteCallback *responseCompleteCallback;
    void *callbackData;

    char doc[4096];
    int docLen, docBytesWritten;
} SetVersioningData;

static S3Status setVersioningPropertiesCallback
    (const S3ResponseProperties *responseProperties, void *callbackData)
{
    SetVersioningData *svData = (SetVersioningData *) callbackData;

    return (*(svData->responsePropertiesCallback))
        (responseProperties, svData->callbackData);
}


static int setVersioningDataCallback(int bufferSize, char *buffer,
                                    void *callbackData)
{
    SetVersioningData *svData = (SetVersioningData *) callbackData;

    if (!svData->docLen) {
        return 0;
    }

    int remaining = (svData->docLen - svData->docBytesWritten);

    int toCopy = bufferSize > remaining ? remaining : bufferSize;

    if (!toCopy) {
        return 0;
    }

    memcpy(buffer, &(svData->doc[svData->docBytesWritten]), toCopy);

    svData->docBytesWritten += toCopy;
    return toCopy;
}

static void setVersioningCompleteCallback(S3Status requestStatus,
                                         const S3ErrorDetails *s3ErrorDetails,
                                         void *callbackData)
{
    SetVersioningData *svData = (SetVersioningData *) callbackData;

    (*(svData->responseCompleteCallback))
        (requestStatus, s3ErrorDetails, svData->callbackData);

    free(svData);
}

void S3_set_bucket_versioning(const S3BucketContext *bucketContext,
                                        int  versioning,
                                        S3RequestContext *requestContext,
                                        const S3ResponseHandler *handler,
                                        void *callbackData)
{
    // Create the callback data
	SetVersioningData *svData =
        (SetVersioningData *) malloc(sizeof(SetVersioningData));
    if (!svData) {
        (*(handler->completeCallback))(S3StatusOutOfMemory, 0, callbackData);
        return;
    }

    svData->responsePropertiesCallback = handler->propertiesCallback;
    svData->responseCompleteCallback = handler->completeCallback;
    svData->callbackData = callbackData;

    svData->docLen =
            snprintf(svData->doc, sizeof(svData->doc),
 "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>%s</Status></VersioningConfiguration>",
                     (versioning==1)? "Enabled" : "Suspended");

        fprintf(stderr, "doc = %s\n", svData->doc);
    svData->docBytesWritten = 0;

    // Set up the RequestParams
        RequestParams params =
   
           {
        HttpRequestTypePUT,                           // httpRequestType
        { bucketContext->hostName,                    // hostName
          bucketContext->bucketName,                  // bucketName
          bucketContext->protocol,                    // protocol
          bucketContext->uriStyle,                    // uriStyle
          bucketContext->accessKeyId,                 // accessKeyId
          bucketContext->secretAccessKey },           // secretAccessKey
        0,                                            // key
        0,                                            // queryParams
        "versioning",                                 // subResource
        0,                                            // copySourceBucketName
        0,                                            // copySourceKey
        0,                                            // getConditions
        0,                                            // startByte
        0,                                            // byteCount
        0,                                                // putProperties
        &setVersioningPropertiesCallback,              // propertiesCallback
        &setVersioningDataCallback,                    // toS3Callback
        svData->docLen,                               // toS3CallbackTotalSize
        0,                                            // fromS3Callback
        &setVersioningCompleteCallback,                // completeCallback
        svData                                        // callbackData
    };
    // Perform the request
         request_perform(&params, requestContext);
    
}
   
// get versioning -------------------------------------------------------------
typedef struct GetVersioningData
{
    SimpleXml simpleXml;

    S3ResponsePropertiesCallback *responsePropertiesCallback;
    S3ResponseCompleteCallback *responseCompleteCallback;
    void *callbackData;

    int versioningReturnSize;
    char *versioningReturn;

    string_buffer(versioning, 256);
} GetVersioningData;


static S3Status getVersioningXmlCallback(const char *elementPath,
                                      const char *data, int dataLen,
                                      void *callbackData)
{
    GetVersioningData *gvData = (GetVersioningData *) callbackData;


    int fit;

    if (data && !strcmp(elementPath, "VersioningConfiguration/Status")) {
        string_buffer_append(gvData->versioning, data, dataLen, fit);
    }

    /* Avoid compiler error about variable set but not used */
    (void) fit;

    return S3StatusOK;
}


static S3Status getVersioningPropertiesCallback
    (const S3ResponseProperties *responseProperties, void *callbackData)
{
    GetVersioningData *gvData = (GetVersioningData *) callbackData;

    return (*(gvData->responsePropertiesCallback))
        (responseProperties, gvData->callbackData);

}


static S3Status getVersioningDataCallback(int bufferSize, const char *buffer,
                                       void *callbackData)
{
    GetVersioningData *gvData = (GetVersioningData *) callbackData;

    return simplexml_add(&(gvData->simpleXml), buffer, bufferSize);
}

static void getVersioningCompleteCallback(S3Status requestStatus,
                                       const S3ErrorDetails *s3ErrorDetails,
                                       void *callbackData)
{
    GetVersioningData *gvData = (GetVersioningData *) callbackData;

    // Copy the location constraint into the return buffer
      snprintf(gvData->versioningReturn,
             gvData->versioningReturnSize, "%s",
             gvData->versioning);

    (*(gvData->responseCompleteCallback))
        (requestStatus, s3ErrorDetails, gvData->callbackData);

    simplexml_deinitialize(&(gvData->simpleXml));

    free(gvData);
}



void S3_get_bucket_versioning(const S3BucketContext *bucketContext,
                    int versioningReturnSize,
                    char *versioningReturn,
                    S3RequestContext *requestContext,
                    const S3ResponseHandler *handler, void *callbackData)
{
    // Create the callback data
	GetVersioningData *gvData =
        (GetVersioningData *) malloc(sizeof(GetVersioningData));
    if (!gvData) {
        (*(handler->completeCallback))(S3StatusOutOfMemory, 0, callbackData);
        return;
    }

    simplexml_initialize(&(gvData->simpleXml), &getVersioningXmlCallback, gvData);

    gvData->responsePropertiesCallback = handler->propertiesCallback;
    gvData->responseCompleteCallback = handler->completeCallback;
    gvData->callbackData = callbackData;

    gvData->versioningReturnSize = versioningReturnSize;
    gvData->versioningReturn = versioningReturn;
    string_buffer_initialize(gvData->versioning);

    // Set up the RequestParams
     RequestParams params =
    {
        HttpRequestTypeGET,                           // httpRequestType
        { bucketContext->hostName,                    // hostName
          bucketContext->bucketName,                  // bucketName
          bucketContext->protocol,                    // protocol
          bucketContext->uriStyle,                    // uriStyle
          bucketContext->accessKeyId,                 // accessKeyId
          bucketContext->secretAccessKey },           // secretAccessKey
        0,                                            // key
        0,                                            // queryParams
        "versioning",                                   // subResource
        0,                                            // copySourceBucketName
        0,                                            // copySourceKey
        0,                                            // getConditions
        0,                                            // startByte
        0,                                            // byteCount
        0,                                            // putProperties
        &getVersioningPropertiesCallback,                // propertiesCallback
        0,                                            // toS3Callback
        0,                                            // toS3CallbackTotalSize
        &getVersioningDataCallback,                      // fromS3Callback
        &getVersioningCompleteCallback,                  // completeCallback
        gvData                                        // callbackData
    };

    // Perform the request

	request_perform(&params, requestContext);
}




// list versions ------------------------------------------------------------	-
typedef struct ListVersionsContents
{
    string_buffer(key, 1024);
    string_buffer(versionId, 1024);
    string_buffer(isLatest, 64);
    string_buffer(lastModified, 256);
    string_buffer(eTag, 256);
    string_buffer(size, 24);
    string_buffer(ownerId, 256);
    string_buffer(ownerDisplayName, 256);
    string_buffer(deleteMarker, 64);

} ListVersionsContents;

static void initialize_list_versions_contents(ListVersionsContents *contents)
{
    string_buffer_initialize(contents->key);
    string_buffer_initialize(contents->versionId);
    string_buffer_initialize(contents->isLatest);
    string_buffer_initialize(contents->lastModified);
    string_buffer_initialize(contents->eTag);
    string_buffer_initialize(contents->size);
    string_buffer_initialize(contents->ownerId);
    string_buffer_initialize(contents->ownerDisplayName);
    string_buffer_initialize(contents->deleteMarker);
}

// We read up to 32 Contents at a time
#define MAX_CONTENTS 32
// We read up to 8 CommonPrefixes at a time
#define MAX_COMMON_PREFIXES 8


typedef struct ListVersionsData
{
    SimpleXml simpleXml;

    S3ResponsePropertiesCallback *responsePropertiesCallback;
    S3ListVersionsCallback *listVersionsCallback;
    S3ResponseCompleteCallback *responseCompleteCallback;
    void *callbackData;

    string_buffer(isTruncated, 64);
    string_buffer(nextKeyMarker, 1024);
    string_buffer(nextVersionIdMarker, 1024);

    int contentsCount;
    ListVersionsContents contents[MAX_CONTENTS];

    int commonPrefixesCount;
    char commonPrefixes[MAX_COMMON_PREFIXES][1024];
    int commonPrefixLens[MAX_COMMON_PREFIXES];
} ListVersionsData;



static void initialize_list_versions_data(ListVersionsData *lvData)
{
    lvData->contentsCount = 0;
    initialize_list_versions_contents(lvData->contents);
    lvData->commonPrefixesCount = 0;
    lvData->commonPrefixes[0][0] = 0;
    lvData->commonPrefixLens[0] = 0;
}


static S3Status make_list_versions_callback(ListVersionsData *lvData)
{
    int i;

  // Convert IsTruncated
  int isTruncated = (!strcmp(lvData->isTruncated, "true") ||
                       !strcmp(lvData->isTruncated, "1")) ? 1 : 0;

    // Convert the contents
     S3ListVersionsContent contents[lvData->contentsCount];

    int contentsCount = lvData->contentsCount;
    for (i = 0; i < contentsCount; i++) {
        S3ListVersionsContent *contentDest = &(contents[i]);
        ListVersionsContents *contentSrc = &(lvData->contents[i]);
        contentDest->key = contentSrc->key;
        contentDest->versionId = contentSrc->versionId;
        contentDest->isLatest =  (!strcmp(contentSrc->isLatest, "true") ||
                       !strcmp(contentSrc->isLatest, "1")) ? 1 : 0;
        contentDest->lastModified =
            parseIso8601Time(contentSrc->lastModified);
        contentDest->eTag = contentSrc->eTag;
        contentDest->size = parseUnsignedInt(contentSrc->size);
        contentDest->ownerId =
            contentSrc->ownerId[0] ?contentSrc->ownerId : 0;
        contentDest->ownerDisplayName = (contentSrc->ownerDisplayName[0] ?
                                         contentSrc->ownerDisplayName : 0);
        contentDest->deleteMarker =
                        ((!strcmp(contentSrc->deleteMarker, "true"))
                 || (!strcmp(contentSrc->deleteMarker, "1"))) ? 1 : 0;
    }

    // Make the common prefixes array

    int commonPrefixesCount = lvData->commonPrefixesCount;
    char *commonPrefixes[commonPrefixesCount];
    for (i = 0; i < commonPrefixesCount; i++) {
        commonPrefixes[i] = lvData->commonPrefixes[i];
    }

    return (*(lvData->listVersionsCallback))
        (isTruncated, lvData->nextKeyMarker, lvData->nextVersionIdMarker,
         contentsCount, contents, commonPrefixesCount,
         (const char **) commonPrefixes, lvData->callbackData);
}


static S3Status listVersionsXmlCallback(const char *elementPath,
                                      const char *data, int dataLen,
                                     void *callbackData)
{
    ListVersionsData *lvData = (ListVersionsData *) callbackData;

    int fit;

    if (data) {
        if (!strcmp(elementPath, "ListVersionsResult/IsTruncated")) {
            string_buffer_append(lvData->isTruncated, data, dataLen, fit);
        }
        else if (!strcmp(elementPath, "ListVersionsResult/NextKeyMarker")) {
            string_buffer_append(lvData->nextKeyMarker, data, dataLen, fit);
        }
        else if (!strcmp(elementPath,
                        "ListVersionsResult/NextVersionIdMarker")) {
            string_buffer_append(lvData->nextVersionIdMarker, data, dataLen, fit);
        }
        else if( (!strcmp(elementPath, "ListVersionsResult/Version/Key"))
        ||(!strcmp(elementPath, "ListVersionsResult/DeleteMarker/Key"))) {
            ListVersionsContents *contents =
                &(lvData->contents[lvData->contentsCount]);
            string_buffer_append(contents->key, data, dataLen, fit);
        }
        else if( (!strcmp(elementPath, "ListVersionsResult/Version/VersionId"))

                ||(!strcmp(elementPath,
                        "ListVersionsResult/DeleteMarker/VersionId"))) {
            ListVersionsContents *contents =
                &(lvData->contents[lvData->contentsCount]);
            string_buffer_append(contents->versionId, data, dataLen, fit);
        }
        else if( (!strcmp(elementPath, "ListVersionsResult/Version/IsLatest"))
                ||(!strcmp(elementPath,
                                "ListVersionsResult/DeleteMarker/IsLatest"))) {
            ListVersionsContents *contents =
                &(lvData->contents[lvData->contentsCount]);
            string_buffer_append(contents->isLatest, data, dataLen, fit);
        }
        else if( (!strcmp(elementPath,
                         "ListVersionsResult/Version/LastModified"))
                ||(!strcmp(elementPath,
                        "ListVersionsResult/DeleteMarker/LastModified"))) {
            ListVersionsContents *contents =
                &(lvData->contents[lvData->contentsCount]);
            string_buffer_append(contents->lastModified, data, dataLen, fit);
        }
        else if( (!strcmp(elementPath, "ListVersionsResult/Version/ETag"))
                ||(!strcmp(elementPath,
                        "ListVersionsResult/DeleteMarker/ETag"))) {
            ListVersionsContents *contents =
                &(lvData->contents[lvData->contentsCount]);
            string_buffer_append(contents->eTag, data, dataLen, fit);
        }
        else if( (!strcmp(elementPath, "ListVersionsResult/Version/Size"))
                ||(!strcmp(elementPath,
                        "ListVersionsResult/DeleteMarker/Size"))) {
            ListVersionsContents *contents =
                &(lvData->contents[lvData->contentsCount]);
            string_buffer_append(contents->size, data, dataLen, fit);
        }
        else if( (!strcmp(elementPath, "ListVersionsResult/Version/Owner/ID"))
                ||(!strcmp(elementPath,
                        "ListVersionsResult/DeleteMarker/Owner/ID"))) {
            ListVersionsContents *contents =
                &(lvData->contents[lvData->contentsCount]);
            string_buffer_append(contents->ownerId, data, dataLen, fit);
        }
        else if( (!strcmp(elementPath,
                         "ListVersionsResult/Version/Owner/DisplayName"))
                ||(!strcmp(elementPath,
                "ListVersionsResult/DeleteMarker/Owner/DisplayName"))) {
            ListVersionsContents *contents =
                &(lvData->contents[lvData->contentsCount]);
            string_buffer_append
                (contents->ownerDisplayName, data, dataLen, fit);
        }
        else if (!strcmp(elementPath,
                         "ListVersionsResult/CommonPrefixes/Prefix")) {
            int which = lvData->commonPrefixesCount;
            lvData->commonPrefixLens[which] +=
                snprintf(lvData->commonPrefixes[which],
                         sizeof(lvData->commonPrefixes[which]) -
                         lvData->commonPrefixLens[which] - 1,
                         "%.*s", dataLen, data);
            if (lvData->commonPrefixLens[which] >=
                (int) sizeof(lvData->commonPrefixes[which])) {
                return S3StatusXmlParseFailure;
            }
        }
    }
    else {
        if( (!strcmp(elementPath, "ListVersionsResult/Version")) ||
        (!strcmp(elementPath, "ListVersionsResult/DeleteMarker"))) {

                if((!strcmp(elementPath, "ListVersionsResult/DeleteMarker"))) {

                        ListVersionsContents *contents =
                        &(lvData->contents[lvData->contentsCount]);
                        string_buffer_append(contents->deleteMarker,
                                        "true", strlen("true"), fit);


                }
            // Finished a Contents
          lvData->contentsCount++;
            if (lvData->contentsCount == MAX_CONTENTS) {
                // Make the callback
                S3Status status = make_list_versions_callback(lvData);
                if (status != S3StatusOK) {
                    return status;
                }
                initialize_list_versions_data(lvData);
            }
            else {
                // Initialize the next one
                               initialize_list_versions_contents
                    (&(lvData->contents[lvData->contentsCount]));
            }
        }
        else if (!strcmp(elementPath,
                         "ListVersionsResult/CommonPrefixes/Prefix")) {
            // Finished a Prefix
            lvData->commonPrefixesCount++;
            if (lvData->commonPrefixesCount == MAX_COMMON_PREFIXES) {
                // Make the callback
                S3Status status = make_list_versions_callback(lvData);
                if (status != S3StatusOK) {
                    return status;
                }
                initialize_list_versions_data(lvData);
            }
            else {
                // Initialize the next one
                               lvData->commonPrefixes[lvData->commonPrefixesCount][0] = 0;
                lvData->commonPrefixLens[lvData->commonPrefixesCount] = 0;
            }
        }
    }

    /* Avoid compiler error about variable set but not used */
    (void) fit;

    return S3StatusOK;
}

static S3Status listVersionsPropertiesCallback
    (const S3ResponseProperties *responseProperties, void *callbackData)
{
    ListVersionsData *lvData = (ListVersionsData *) callbackData;

    return (*(lvData->responsePropertiesCallback))
        (responseProperties, lvData->callbackData);
}
static S3Status listVersionsDataCallback(int bufferSize, const char *buffer,
                                       void *callbackData)
{
    ListVersionsData *lvData = (ListVersionsData *) callbackData;

    return simplexml_add(&(lvData->simpleXml), buffer, bufferSize);
}

static void listVersionsCompleteCallback(S3Status requestStatus,
                                       const S3ErrorDetails *s3ErrorDetails,
                                       void *callbackData)
{
    ListVersionsData *lvData = (ListVersionsData *) callbackData;

    // Make the callback if there is anything
    if (lvData->contentsCount || lvData->commonPrefixesCount) {
        make_list_versions_callback(lvData);
    }

    (*(lvData->responseCompleteCallback))
        (requestStatus, s3ErrorDetails, lvData->callbackData);

    simplexml_deinitialize(&(lvData->simpleXml));

    free(lvData);
}

void S3_list_versions(const S3BucketContext *bucketContext, const char *prefix,
                    const char *keyMarker, const char *versionIdMarker,
                        const char *delimiter, int maxkeys,
                    S3RequestContext *requestContext,
                    const S3ListVersionsHandler *handler, void *callbackData)
{
    // Compose the query params
       string_buffer(queryParams, 4096);
    string_buffer_initialize(queryParams);

#define safe_append(name, value)                                        \
    do {                                                                \
        int fit;                                                        \
        if (amp) {                                                      \
            string_buffer_append(queryParams, "&", 1, fit);             \
            if (!fit) {                                                 \
                (*(handler->responseHandler.completeCallback))          \
                    (S3StatusQueryParamsTooLong, 0, callbackData);      \
                return;                                                 \
            }                                                           \
        }                                                               \
        string_buffer_append(queryParams, name "=",                     \
                             sizeof(name "=") - 1, fit);                \
        if (!fit) {                                                     \
            (*(handler->responseHandler.completeCallback))              \
                (S3StatusQueryParamsTooLong, 0, callbackData);          \
            return;                                                     \
        }                                                               \
        amp = 1;                                                        \
        char encoded[3 * 1024];                                         \
        if (!urlEncode(encoded, value, 1024)) {                         \
            (*(handler->responseHandler.completeCallback))              \
                (S3StatusQueryParamsTooLong, 0, callbackData);          \
            return;                                                     \
        }                                                               \
        string_buffer_append(queryParams, encoded, strlen(encoded),     \
                             fit);                                      \
        if (!fit) {                                                     \
            (*(handler->responseHandler.completeCallback))              \
                (S3StatusQueryParamsTooLong, 0, callbackData);          \
            return;                                                     \
        }                                                               \
    } while (0)


    int amp = 0;

   if (prefix) {
        safe_append("prefix", prefix);
    }
    if (keyMarker) {
        safe_append("key-marker", keyMarker);
    }
    if (versionIdMarker) {
        safe_append("version-id-marker", versionIdMarker);
    }
    if (delimiter) {
        safe_append("delimiter", delimiter);
    }
    if (maxkeys) {
        char maxKeysString[64];
        snprintf(maxKeysString, sizeof(maxKeysString), "%d", maxkeys);
        safe_append("max-keys", maxKeysString);
    }

    ListVersionsData *lvData =
        (ListVersionsData *) malloc(sizeof(ListVersionsData));

    if (!lvData) {
        (*(handler->responseHandler.completeCallback))
            (S3StatusOutOfMemory, 0, callbackData);
        return;
    }

    simplexml_initialize(&(lvData->simpleXml),
                                &listVersionsXmlCallback, lvData);

    lvData->responsePropertiesCallback =
        handler->responseHandler.propertiesCallback;
    lvData->listVersionsCallback = handler->listVersionsCallback;
    lvData->responseCompleteCallback =
        handler->responseHandler.completeCallback;
    lvData->callbackData = callbackData;

    string_buffer_initialize(lvData->isTruncated);
    string_buffer_initialize(lvData->nextKeyMarker);
    string_buffer_initialize(lvData->nextVersionIdMarker);
    initialize_list_versions_data(lvData);

    // Set up the RequestParams
      RequestParams params =
    {
        HttpRequestTypeGET,                           // httpRequestType
        { bucketContext->hostName,                    // hostName
          bucketContext->bucketName,                  // bucketName
          bucketContext->protocol,                    // protocol
          bucketContext->uriStyle,                    // uriStyle
          bucketContext->accessKeyId,                 // accessKeyId
          bucketContext->secretAccessKey },           // secretAccessKey
        0,                                            // key
        queryParams[0] ? queryParams : 0,             // queryParams
        "versions",                                            // subResource
        0,                                            // copySourceBucketName
        0,                                            // copySourceKey
        0,                                            // getConditions
        0,                                            // startByte
        0,                                            // byteCount
        0,                                            // putProperties
        &listVersionsPropertiesCallback,                // propertiesCallback
        0,                                            // toS3Callback
        0,                                            // toS3CallbackTotalSize
        &listVersionsDataCallback,                      // fromS3Callback
        &listVersionsCompleteCallback,                  // completeCallback
        lvData                                        // callbackData
    };

    // Perform the request
        request_perform(&params, requestContext);
}
