/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */
    
#include "backArgs.h"


int backDatabaseMeta(const char *dbName) {
    int code = TSDB_CODE_FAILED;

    // database
    code = backCreateDbSql(dbName);

    // super tables
    code = backCreateStbsSql(dbName);

    return code;
}

int backDatabaseData(const char *dbName) {
    int code = TSDB_CODE_FAILED;

    char ** stbNames = getDBSuperTableNames(dbName);
    for (int i = 0; stbNames[i] != NULL; i++) {
        printf("backup super table data: %s.%s\n", dbName, stbNames[i]);
        code = backStbData(dbName, stbNames[i]);
        if (code != TSDB_CODE_SUCCESS) {
            printf("backup super table data failed: %s.%s, code: %d\n", dbName, stbNames[i], code);
            return code;
        }
    }
    
    return code;
}


// backup database 
int backDatabase(const char *dbName) {
    // back up database meta
    int code = TSDB_CODE_FAILED;
    code = backDatabaeMeta(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        printf("backup database meta failed, code: %d\n", code);
        return code;
    }

    // back up super tables meta
    code = backDatabaseData(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        printf("backup super table meta failed, code: %d\n", code);
        return code;
    }



    return code;
}


// backup main function
int backupMain(){
    // init
    int code = TSDB_CODE_FAILED;

    char **backDB = argsGetBackDB();
    if (backDB == NULL) {
        printf("no database to backup\n");
        return TSDB_CODE_INVALID_PARAM;
    }

    for (int i = 0; backDB[i] != NULL; i++) {
        printf("backup database: %s\n", backDB[i]);

        // backup data
        code = backupDatabase(backDB[i]);
        if (code != TSDB_CODE_SUCCESS) {
            printf("backup data failed, code: %d\n", code);
            return code;
        }
    }

    return code;
}