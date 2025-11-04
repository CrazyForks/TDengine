/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "mndJson.h"
#include "mndStream.h"

static const char* jkStreamObjName      = "name";
static const char* jkStreamObjMainSnode = "mainSnodeId";
static const char* jkStreamObjUserDrop  = "userDropped";
static const char* jkStreamObjUserStop  = "userStopped";
static const char* jkStreamObjCreateTime = "createTime";
static const char* jkStreamObjUpdateTime = "updateTime";
static const char* jkStreamObjCreate    = "create";

int32_t mndStreamObjToJson(
  const SStreamObj* pObj, bool format, char** pStr, int32_t* pStrLen) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  STREAM_CHECK_NULL_GOTO(pObj, TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  STREAM_CHECK_NULL_GOTO(pStr, TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  STREAM_CHECK_NULL_GOTO(pStrLen, TSDB_CODE_MND_STREAM_INTERNAL_ERROR);

  SJson* pJson = tjsonCreateObject();
  STREAM_CHECK_NULL_GOTO(pJson, terrno);

  int64_t streamId = pObj->pCreate->streamId;

  TSDB_CHECK_CODE(tjsonAddStringToObject(
    pJson, jkStreamObjName, pObj->name), lino, end);

  TSDB_CHECK_CODE(tjsonAddIntegerToObject(
    pJson, jkStreamObjMainSnode, pObj->mainSnodeId), lino, end);

  TSDB_CHECK_CODE(tjsonAddIntegerToObject(
    pJson, jkStreamObjUserDrop, pObj->userDropped), lino, end);

  TSDB_CHECK_CODE(tjsonAddIntegerToObject(
    pJson, jkStreamObjUserStop, pObj->userStopped), lino, end);

  TSDB_CHECK_CODE(tjsonAddIntegerToObject(
    pJson, jkStreamObjCreateTime, pObj->createTime), lino, end);

  TSDB_CHECK_CODE(tjsonAddIntegerToObject(
    pJson, jkStreamObjUpdateTime, pObj->updateTime), lino, end);

  // encode SCMCreateStreamReq
  SJson* pCreateItem = tjsonCreateObject();
  STREAM_CHECK_NULL_GOTO(pCreateItem, TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  TSDB_CHECK_CODE(scmCreateStreamReqToJsonImpl(
    pObj->pCreate, pCreateItem), lino, end);
  TSDB_CHECK_CODE(tjsonAddItemToObject(
    pJson, jkStreamObjCreate, pCreateItem), lino, end);

  if (TSDB_CODE_SUCCESS == code) {
    *pStr = format ? tjsonToString(pJson) : tjsonToUnformattedString(pJson);
    if (*pStr == NULL) {
      code = terrno;
    } else {
      *pStrLen = strlen(*pStr);
    }
  }

end:
  tjsonDelete(pJson);
  if (TSDB_CODE_SUCCESS != code) {
    mstsError(
      "failed to convert SStreamObj to json, lino: %d, since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t jsonToMndStreamObj(const SJson* pJson, void* pObj) {
  SStreamObj* pStream = (SStreamObj*)pObj;

  TAOS_CHECK_RETURN(tjsonGetStringValue(
    pJson, jkStreamObjName, pStream->name));

  TAOS_CHECK_RETURN(tjsonGetIntValue(
    pJson, jkStreamObjMainSnode, &pStream->mainSnodeId));

  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkStreamObjUserDrop, &pStream->userDropped));

  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkStreamObjUserStop, &pStream->userStopped));

  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkStreamObjCreateTime, &pStream->createTime));

  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkStreamObjUpdateTime, &pStream->updateTime));

  pStream->pCreate = taosMemoryCalloc(1, sizeof(*pStream->pCreate));
  if (NULL == pStream->pCreate) {
    return terrno;
  }
  TAOS_CHECK_RETURN(tjsonToObject(
    pJson, jkStreamObjCreate, jsonToSCMCreateStreamReq, pStream->pCreate));

  return TSDB_CODE_SUCCESS;
}
