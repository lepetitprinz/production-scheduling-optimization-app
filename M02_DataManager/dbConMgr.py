# -*- coding: utf-8 -*-


# encoding: utf-8
import sys
import os
import argparse
import enum
import cx_Oracle

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

    def _getConnection(self):
        try:
            if self._pool is None:
                print("$$ Create connection pool")
                self._pool = cx_Oracle.SessionPool(
                    user=self._conUID,
                    password=self._conPWD,
                    dsn=self._conTNS,
                    min=1,
                    max = 2,
                    increment=1,
                    threaded=True,
                    encoding="UTF-8"
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

        for directory, folder, filename in os.walk(M06_Utility.comUtility.Utility.project_dir):
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
    def GetDpQtyDataSql(self):
        sql = """ SELECT 
        
        
        """

    def GetProdWheelDataSql(self):


    def GetFpCapaMstDataSql

class Queries:
    sample_sql: str = """
    SELECT * 
    FROM EDU.AIRPUSAN 
    WHERE 1 = 1
    """



def main():

    if_proc_dict = {
        "MP": [
            ""
        ],
        "DS": [
            ""
        ]
    }

    parser = argparse.ArgumentParser()

    parser.add_argument('--ip', required=True, help='IP Address')
    parser.add_argument('--port', required=True, help='Port Number')
    parser.add_argument('--sid', required=True, help='SID')
    parser.add_argument('--uid', required=True, help='User ID')
    parser.add_argument('--pwd', required=True, help='Password')
    parser.add_argument('--mode', required=True, help='MP or DS')
    parser.add_argument('--sc_type', required=True, help='Procedure Parameter/Varchar2')
    parser.add_argument('--in_time', required=True, help='Procedure Parameter/Varchar2')
    parser.add_argument('--del_period', required=True, help='Procedure Parameter/Number')
    parser.add_argument('--in_type', required=True, help='Procedure Parameter/Number')

    args = parser.parse_args()

    procExecutor = ConnectionManager()
    procExecutor.SetupObject(
        ip=args.ip,
        port=args.port,
        sid=args.sid,
        uid=args.uid,
        pwd=args.pwd
    )

    params = [args.sc_type, args.in_time, args.del_period, args.in_type]

    for procNm in if_proc_dict[args.mode]:
        procExecutor.ExecuteProc(procNm=procNm, params=params)


def test():

    conMgr: ConnectionManager = ConnectionManager()

    conMgr.LoadConInfo()

    print(conMgr.conf_path)

    print((conMgr._conIP, conMgr._conPORT, conMgr._conSID, conMgr._conUID, conMgr._conPWD))

    sample_arr: list = conMgr.GetDbData(sql=Queries.sample_sql)
    print(sample_arr)


if __name__ == '__main__':
    test()
