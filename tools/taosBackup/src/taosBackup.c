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
    
#include "taosBackup.h"
#include "bckArgs.h"

int main(int argc, char *argv[]) {
    printf("taosBackup tool v1.0\n");
    int code = TSDB_CODE_SUCCESS;

    //
    //  arguments 
    //
    if (argsInit(argc, argv) != 0) {
        printf("init args failed\n");
        return -1;
    }

    //
    // action
    //
    enum ActionType action = argsGetAction();
    switch (action) {
        case ACTION_BACKUP:
            printf("perform backup action\n");
            code = backupMain();
            break;
        case ACTION_RESTORE:
            printf("perform restore action\n");
            code = restoreMain();
            break;
        default:
            printf("unknown action\n");
            break;
    }

    //
    // destroy
    //
    argsDestroy();
    return 0;

}