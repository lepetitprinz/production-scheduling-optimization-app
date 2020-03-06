# -*- coding: utf-8

from M02_DataManager import dbConMgr
# from M06_Utility import comUtility


class DataManager:

    def __init__(self):

        # 기본 정보
        self._a1: str = ""
        self._a2: str = ""
        self._conMgr: dbConMgr.ConnectionManager = None

        # 쿼리문 결과를 담을 Array 변수들을 선언
        self.dbSiteArr: list = []

    def SetupObject(self):
        self._conMgr = dbConMgr.ConnectionManager()
        self._conMgr.LoadConInfo()

        # comUtility.M06_Utility.SetRootPath(rootPath=self._conMgr.RootPath)
        # comUtility.M06_Utility.SetConfPath(confPath=self._conMgr.conf_path)
