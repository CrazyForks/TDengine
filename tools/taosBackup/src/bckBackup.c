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

int backDBMeta(const char *dbName) {
    return TSDB_CODE_SUCCESS;
}

int backDBData(const char *dbName) {
    return TSDB_CODE_SUCCESS;
}


// backup main function
int backupMain(){
    // init
    int code = TSDB_CODE_SUCCESS;

    char **backDB = argsGetBackDB();
    if (backDB == NULL) {
        printf("no database to backup\n");
        return TSDB_CODE_INVALID_PARAM;
    }

    for (int i = 0; backDB[i] != NULL; i++) {
        printf("backup database: %s\n", backDB[i]);

        // backup meta
        code = backDBMeta(backDB[i]);
        if (code != TSDB_CODE_SUCCESS) {
            printf("backup meta failed, code: %d\n", code);
            return code;
        }

        // backup data
        code = backDBData(backDB[i]);
        if (code != TSDB_CODE_SUCCESS) {
            printf("backup data failed, code: %d\n", code);
            return code;
        }
    }

    return code;
}