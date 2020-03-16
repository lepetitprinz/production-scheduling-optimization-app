# -*- coding: utf-8 -*-


# encoding: utf-8
import sys
import os
import time
import argparse
import enum
import cx_Oracle
from M06_Utility.comUtility import Utility

from M06_Utility import comUtility


class ConnectionManager(object):
    # 환경 설정
    os.environ["NLS_LANG"] = ".AL32UTF8"
    START_VALUE = u"Unicode \u3042 3".encode('utf-8')
    END_VALUE = u"Unicode \u3042 6".encode('utf-8')

    # 초기 Database 접근 정보
    def __init__(self):
        # ---- conf 파일 ----
        self.conf_path: str = ""
        # ---- DB 데이터 ----
        self._conIP: str = ""
        self._conPORT: str = ""
        self._conSID: str = ""
        self._conUID: str = ""
        self._conPWD: str = ""
        self._conTNS: str = ""
        self._pool: cx_Oracle.SessionPool = None

    def SetupObject(self, ip: str, port: str, sid: str, uid: str, pwd: str):
        '''
        연결정보를 전달하는 방식 파라미터 처리
        '''
        self._conIP = ip
        self._conPORT = port
        self._conSID = sid
        self._conUID = uid
        self._conPWD = pwd
        try:
            self._conTNS = cx_Oracle.makedsn(ip, port, sid)
        except cx_Oracle.DatabaseError as err:
            print(err)

    def LoadConInfo(self):
        ''''
        Config 파일에서 읽어오는 방식으로 파라미터 처리
        '''
        if len(self.conf_path) < 1:
            self._reset_conf_path()

        if not os.path.exists(self.conf_path):
            raise FileNotFoundError(
                f"[DB Server Configuration File Not Found] {self.conf_path}"
            )

        # DB연결정보 취득 및 연결
        self._conIP, self._conPORT, self._conSID, self._conUID, self._conPWD = self._getConfInfoArr(self.conf_path)

        rslt: bool = False
        try:
            self._conTNS = cx_Oracle.makedsn(self._conIP, self._conPORT, self._conSID)
            rslt = True
        except Exception as e:
            print(e)
        finally:
            print(f"rslt: {rslt}")
        # try:
        #     self._conTNS = cx_Oracle.makedsn(self._conIP, self._conPORT, self._conSID)
        # except Exception as e:
        #     pass

        # try:
        #     if len(self.conf_path) < 1:
        #         self._reset_conf_path()
        #
        #     assert os.path.exists(self.conf_path),  f"[FileNotFound] {self.conf_path}"
        #
        #
        #
        # except IOError as e:
        #     print("I/O error({}): {}".format(
        #         e.errno, e.strerror
        #     ))
        # except:
        #     print("Unexpected error:", sys.exc_info()[0])
        #
        # return True

    def GetDbData(self, sql: str, params=None):
        try:
            con = self._getConnection()
            cur = con.cursor()
            # print("$$ Query / Con: {}, Cur: {}".format(id(con), id(cur)))
            cur.execute(sql, params or ())
            rslt = cur.fetchall()
            # con.commit()
            self._releaseConnection(con)
            # print(rslt)
            return rslt

        except cx_Oracle.DatabaseError as err:
            print(err)
            print(sql)

        return None

    def ExecuteProc(self, procNm: str, params: list):
        con = self._getConnection()
        try:
            cur = con.cursor()
            cur.callproc(procNm, params)
            self._releaseConnection(con)
            return True
        except cx_Oracle.DatabaseError as e:
            self._releaseConnection(con)
            print(e)

        return False

    def loadData(self):
        raise Exception(
            f"Make Me !! from {self.__class__.loadData}"
        )

    def BatchQuery(self, sqlTemplate: str, dataArr: list, sqlDel: str=""):
        con = None
        errCode = ""
        try:
            errCnt = 0
            while errCnt < 11:
                con = self._getConnection()
                if con != None:
                    break
                else:
                    errCnt += 1
                    time.sleep(1)

            cur = con.cursor()
            if len(sqlDel) > 0:
                cur.execute(sqlDel, None or ())

            cur.executemany(sqlTemplate, dataArr)

            return True, errCode
        except cx_Oracle.DatabaseError as e:
            print(e)
            error, = e.args
            errCode = error.code
            # print(sqlTemplate)
            if len(dataArr) < 11:
                print(dataArr)
            else:
                print(dataArr[0])
        finally:
            self._releaseConnection(con)

        return False, errCode

    def _getConnection(self):
        try:
            if self._pool is None:
                print("$$ Create connection pool")
                self._pool = cx_Oracle.SessionPool(
                    user=self._conUID,
                    password=self._conPWD,
                    dsn=self._conTNS,
                    #min=1,
                    #max = 2,
                    #increment=1,
                    #threaded=True,
                    # encoding="UTF-8"
                )
            return self._pool.acquire()

        except cx_Oracle.DatabaseError as err:
            print(err)
            return None

    def _releaseConnection(self, con):
        try:
            con.commit()
            self._pool.release(con)

        except cx_Oracle.DatabaseError as err:
            print(err)
            return None

    def _get_server_config(self):
        # server_config 폴더 경로 찾는 처리
        for directory, folder, filename in os.walk(Utility.project_dir):

            if 'server.conf' in filename:
                server_conf: str = os.path.join(directory, 'server.conf')
                return server_conf
        return ""

    def _getConfInfoArr(self, server_conf_path: str):
        f = open(server_conf_path, "r")
        fl = f.readlines()
        ip = ""
        port = ""
        sid = ""
        uid = ""
        pwd = ""
        for line in fl:
            tmp = line.strip()
            if len(tmp) > 0 and tmp[0] != '#' and "=" in tmp:
                spArr = tmp.split("=")
                # rslt.update({spArr[0].upper(): spArr[1].strip()})
                if "IP" == spArr[0].upper():
                    ip = spArr[1].strip()
                elif "PORT" == spArr[0].upper():
                    port = spArr[1].strip()
                elif "SID" == spArr[0].upper():
                    sid = spArr[1].strip()
                elif "UID" == spArr[0].upper():
                    uid = spArr[1].strip()
                elif "PWD" == spArr[0].upper():
                    pwd = spArr[1].strip()

        return ip, port, sid, uid, pwd

    def _reset_conf_path(self):
        self.conf_path = self._get_server_config()

    # ================================================================================= #
    # DB에서 Data 불러오는 처리
    # ================================================================================= #
    # 공급 계획 정보
    def GetDpQtyDataSql(self):
        sql = """ SELECT MST.PLAN_YYMM AS YYYYMM
                         , MST.ITEM_CD AS PROD_CODE
                         , J01.ITEM_NM AS PRODUCT
                         , MST.MP_REQ_QTY AS QTY
                      FROM (
                            SELECT MP_VRSN_ID
                                 , ITEM_CD
                                 , PLAN_YYMM
                                 , MP_REQ_QTY
                            FROM SCMUSER.TB_MP_QTY_DATA
                            WHERE 1=1
                            AND MP_VRSN_ID = '{}'
                            AND PLAN_YYMM BETWEEN '{}' AND '{}'
                            AND MP_REQ_QTY > 0
                            ) MST
                     INNER JOIN SCMUSER.TB_CM_ITEM_MST J01
                        ON MST.ITEM_CD = J01.ITEM_CD
                    ORDER BY MST.PLAN_YYMM
                           , PROD_CODE """.format(Utility.MPVerId, Utility.PlanStartDay, Utility.PlanEndDay)
        return sql

    def GetDpQtyDataSql_Custom(self, plan_start: str, plan_end: str):
        sql = """ SELECT MST.PLAN_YYMM AS YYYYMM
                         , MST.ITEM_CD AS PROD_CODE
                         , J01.ITEM_NM AS PRODUCT
                         , MST.MP_REQ_QTY AS QTY
                      FROM (
                            SELECT MP_VRSN_ID
                                 , ITEM_CD
                                 , PLAN_YYMM
                                 , MP_REQ_QTY
                            FROM SCMUSER.TB_MP_QTY_DATA
                            WHERE 1=1
                            AND MP_VRSN_ID = '{}'
                            AND PLAN_YYMM BETWEEN '{}' AND '{}'
                            AND MP_REQ_QTY > 0
                            ) MST
                     INNER JOIN SCMUSER.TB_CM_ITEM_MST J01
                        ON MST.ITEM_CD = J01.ITEM_CD
                    ORDER BY MST.PLAN_YYMM
                           , PROD_CODE """.format(Utility.MPVerId, plan_start, plan_end)
        return sql

    def GetProdWheelDataSql(self):
        sql = """ SELECT FROM_MATRL_CD AS GRADE_FROM
                       , TO_MATRL_CD AS GRADE_TO
                       , OP_STOP_TIME AS STOP_TIME
                       , OG_OCCUR_QTY AS OG
                    FROM SCMUSER.TB_FP_PROD_WHEEL_MST
                   WHERE 1=1
                     AND PROD_WHEEL_GRP_CD = 'PROD1'"""
        return sql

    # Production Ton/hour Data
    def GetFpCapaMstDataSql(self):
        sql = """ SELECT MST.OPER
                       , MST.ITEM_CD AS PROD_CODE
                       , MST.GRADE
                       , J01.PROD_YIELD
                    FROM (
                          SELECT ITEM_CD
                               , ITEM_NM AS GRADE
                               , CASE WHEN ITEM_TYPE_CD LIKE '%P01%' THEN 'package'
                                     WHEN ITEM_TYPE_CD LIKE '%P02%' THEN 'reactor'
                                  END AS OPER
                            FROM SCMUSER.TB_CM_ITEM_MST
                         ) MST
                   INNER JOIN (
                               SELECT ITEM_CD
                                    , CAPA_QTY AS PROD_YIELD
                                 FROM SCMUSER.TB_FP_CAPA_MST
                              ) J01
                      ON MST.ITEM_CD = J01.ITEM_CD """
        return sql

    # 엔진 Configuration Data
    def GetEngineConfDataSql(self):
        sql = """ SELECT PARAM_CD
                , PARAM_NM
                , PARAM_VAL
            FROM SCMUSER.TB_FS_ENGINE_CONF """

        return sql

    # Machine 비가용 계획 Data
    def GetMacUnAvlTimeDataSql(self):
        sql = """
        SELECT CASE WHEN RES_CD = 'M1' THEN 'reactor'
                    WHEN RES_CD IN ('P2', 'P7', 'P9') THEN 'bagging'
                END AS OPER_ID
             , RES_CD AS MAC_ID
             , PLAN_FROM_YYMMDD || FROM_TIME_VAL AS FROM_TIME
             , PLAN_TO_YYMMDD || TO_TIME_VAL AS TO_TIME
        FROM SCMUSER.TB_FP_CAL_MST
        WHERE 1=1
          AND PLAN_FROM_YYMMDD BETWEEN '{}' AND '{}'
        ORDER BY OPER_ID
               , MAC_ID
               , FROM_TIME
                """.format(Utility.PlanStartTime, Utility.PlanEndTime)
        return sql

    def GetProdMstDataSql(self):
        sql = """ SELECT ITEM_CD AS PROD_CODE
                       , ITEM_NM AS PROD_NAME
                    FROM SCMUSER.TB_CM_ITEM_MST
        """
        return sql

    # def GetDpQtyDataSql(self):
    #     sql = """ SELECT MST.YYYYMM
    #                    , MST.PRODUCT
    #                    , MST.QTY
    #                    , J01.DOME_CD
    #                FROM (
    #                      SELECT PLAN_YYMM AS YYYYMM
    #                           , ITEM_NM AS PRODUCT
    #                           , DP_QTY AS QTY
    #                           , CUST_CD
    #                        FROM SCMUSER.TB_DP_QTY_DATA
    #                       WHERE 1=1
    #                         AND DP_VRSN_ID = 'DP_202003_V01'
    #                         AND SUBSTR(ITEM_NM,1,7) != 'GRADE_T'
    #                      ) MST
    #               INNER JOIN (
    #                           SELECT CUST_CD
    #                                , DOME_CD
    #                             FROM SCMUSER.TB_CM_CUST_MST
    #                          ) J01
    #                  ON MST.CUST_CD = J01.CUST_CD """
    #     return sql


    # def GetProdWheelDataSql(self):
    #     sql = """ SELECT FROM_MATRL_CD AS GRADE_FROM
    #                    , TO_MATRL_CD AS GRADE_TO
    #                    , OP_STOP_TIME AS STOP_TIME
    #                    , OG_OCCUR_QTY AS OG
    #                 FROM SCMUSER.TB_FP_PROD_WHEEL_MST
    #                WHERE 1=1
    #                  AND PROD_WHEEL_GRP_CD = 'PROD1'
    #                UNION ALL
    #               SELECT 'GRADE_A' AS GRADE_FROM
    #                    , 'GRADE_A' AS GRADE_TO
    #                    , 0 AS STOP_TIME
    #                    , 0 AS OG
    #                 FROM DUAL
    #                UNION ALL
    #               SELECT 'GRADE_B' AS GRADE_FROM
    #                    , 'GRADE_B' AS GRADE_TO
    #                    , 0 AS STOP_TIME
    #                    , 0 AS OG
    #                FROM DUAL
    #               UNION ALL
    #              SELECT 'GRADE_C' AS GRADE_FROM
    #                   , 'GRADE_C' AS GRADE_TO
    #                   , 0 AS STOP_TIME
    #                   , 0 AS OG
    #                FROM DUAL
    #               UNION ALL
    #              SELECT 'GRADE_D' AS GRADE_FROM
    #                   , 'GRADE_D' AS GRADE_TO
    #                   , 0 AS STOP_TIME
    #                   , 0 AS OG
    #                FROM DUAL
    #               UNION ALL
    #              SELECT 'GRADE_E' AS GRADE_FROM
    #                   , 'GRADE_E' AS GRADE_TO
    #                   , 0 AS STOP_TIME
    #                   , 0 AS OG
    #                FROM DUAL
    #               UNION ALL
    #              SELECT 'GRADE_F' AS GRADE_FROM
    #                   , 'GRADE_F' AS GRADE_TO
    #                   , 0 AS STOP_TIME
    #                   , 0 AS OG
    #                FROM DUAL
    #               UNION ALL
    #              SELECT 'GRADE_G' AS GRADE_FROM
    #                   , 'GRADE_G' AS GRADE_TO
    #                   , 0 AS STOP_TIME
    #                   , 0 AS OG
    #                FROM DUAL """
    #     return sql
