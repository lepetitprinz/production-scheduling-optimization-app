# -*- coding: utf-8

import pandas as pd

import dbConMgr, fileConMgr
# from M06_Utility import comUtility


class DataManager:

    def __init__(self, source: str = "file"):

        # 기본 정보
        self._source: str = source
        self._conMgr = None

        # 쿼리 및 파일 Read 결과를 담을 Array 변수들을 선언
        self.df_demand: pd.DataFrame = None
        self.df_prod_wheel: pd.DataFrame = None
        self.df_prod_yield: pd.DataFrame = None

    def SetupObject(self):    # or source = "db"
        if self._source == "db":
            self._setup_db_connection()
        elif self._source == "file":
            self._setup_file_connection()

        # comUtility.Utility.SetRootPath(rootPath=self._conMgr.RootPath)
        # comUtility.Utility.SetConfPath(confPath=self._conMgr.conf_path)

    def _setup_db_connection(self):
        self._conMgr = dbConMgr.ConnectionManager()
        self._conMgr.LoadConInfo()

    def _setup_file_connection(self):
        self._conMgr: fileConMgr.FileManager = fileConMgr.FileManager()
        self._conMgr.setup_object()

        self._conMgr.set_csv_path(data_name="", csv_path="")

