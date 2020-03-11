# -*- coding: utf-8

import datetime
import pandas as pd
import time

from M02_DataManager import dbConMgr, fileConMgr
from M06_Utility import comUtility

class DataManager:

    def __init__(self, source: str = "db"):

        # 기본 정보
        self._source: str = source
        self._conMgr = None

        # 쿼리 및 파일 Read 결과를 담을 Array 변수들을 선언
        self.dbDemand: pd.DataFrame = None
        self.dbProdWheel: pd.DataFrame = None
        self.dbProdYield: pd.DataFrame = None

        # configuration 정보
        self.dbEngConf: pd.DataFrame = None  # Engine Configuration 정보
        self.dbMacUnAvlTime: pd.DataFrame = None

        # Dictionary 변수 선언
        self._dict_prod_yield: dict = {}
        self._dict_days_by_month: dict = {}

    def SetupEngConfData(self):
        self._conMgr = dbConMgr.ConnectionManager()
        self._conMgr.LoadConInfo()
        engConfig = self._conMgr.GetDbData(self._conMgr.GetEngineConfDataSql())
        engConfigColName = ['paramCode', 'paramName', 'paramVal']
        self.dbEngConf = pd.DataFrame(engConfig, columns=engConfigColName)

        return self.dbEngConf

    def SetupObject(self):    # or source = "db"
        # if self._source == "db":
        #     self._setup_db_connection()
        # elif self._source == "file":
        #     self._setup_file_connection()

        if self._source == "file":
            self._setup_file_connection()

            demand = self._conMgr.loadData("demand")
            prodWheel = self._conMgr.loadData("prod_wheel")
            prodYield = self._conMgr.loadData("prod_yield")

        if self._source == "db":

            # self._conMgr = dbConMgr.ConnectionManager()
            self._conMgr = dbConMgr.ConnectionManager()
            self._conMgr.LoadConInfo()
            
            # DB에서 Data Load 하는 처리
            demand = self._conMgr.GetDbData(self._conMgr.GetDpQtyDataSql())
            prodWheel = self._conMgr.GetDbData(self._conMgr.GetProdWheelDataSql())
            prodYield = self._conMgr.GetDbData(self._conMgr.GetFpCapaMstDataSql())
            macUnAvlTime = self._conMgr.GetDbData(self._conMgr.GetMacUnAvlTimeDataSql())
            # engConfig = self._conMgr.GetDbData(self._conMgr.GetEngineConfDataSql())

            # Data Column 정의
            dmdColName = ['yyyymm', 'prodCode', 'product', 'qty']
            prodWheelColName = ['grade_from', 'grade_to', 'hour', 'og']
            prodYieldColName = ['oper', 'grade', 'prod_yield']
            macUnAvlColName = ['operId', 'macId', 'fromTime', 'toTIme']
            # engConfigColName = ['paramCode', 'paramName', 'paramVal']

            self.dbDemand = pd.DataFrame(demand, columns=dmdColName)
            self.dbProdWheel = pd.DataFrame(prodWheel, columns=prodWheelColName)
            self.dbProdYield = pd.DataFrame(prodYield, columns=prodYieldColName)

            self.dbMacUnAvlTime =  pd.DataFrame(macUnAvlTime, columns=macUnAvlColName)
            # self.dbEngConf = pd.DataFrame(engConfig, columns=engConfigColName)

        # self.df_demand = self._conMgr.load_data(data_name="demand")
        # self.dfProdWheel = self._conMgr.load_data(data_name="prod_wheel")
        # self.df_prod_yield = self._conMgr.load_data(data_name="prod_yield")

        # self._preprocessing()
        self._build_dict_prod_yield()

        comUtility.Utility.ProdWheelDf = self.dbProdWheel.copy()
        # comUtility.Utility.SetRootPath(rootPath=self._conMgr.RootPath)
        # comUtility.Utility.SetConfPath(confPath=self._conMgr.conf_path)

    def CloseDataMgr(self):
        self._conMgr.CloseConnection()
        self.__init__()

    def SaveEngConfig(self):
        confArr = self._getEngConfDataArr()
        # self.UpdateEngConfHistory(dataArr=confArr, useTmpFlag=comUtility.Utility.TempTblUseFlag)

    def _getEngConfDataArr(self):
        # datasetId = comUtility.Utility.DataSetID
        # simulNum = comUtility.Utility.SimulNumber
        confHdr = ("PLAN_HORIZON", "PLAN_START_TIME", "HISTORY_SAVE_YN", "UOM_HORIZON", "ORP_DMD_LEVELING",
                   "ENGINE_MODE", "ORP_VER", "AI_MOD_YN", "EVENT_LEVEL", "LOG_LEVEL", "ORP_CONFIRM_PLAN_YN")
        rslt = []

        for i in range(len(self.dbEngConfArr)):
            rslt.append((confHdr[i], str(self.dbEngConfArr[i])))
            # rslt.append((datasetId, simulNum, confHdr[i], str(self.dbEngConfArr[i])))

        return rslt

    def build_demand_max_days_by_month(self):
        yyyy_mm_list: list = []
        for _, row in self.dbDemand.iterrows():
            yyyymm: str = row['yyyymm']
            yyyymm_date: datetime.datetime = datetime.datetime.strptime(yyyymm, "%Y%m")
            yyyy: int = yyyymm_date.year
            mm: int = yyyymm_date.month
            yyyy_mm_tuple: tuple = (yyyy, mm)
            yyyy_mm_list.append(yyyy_mm_tuple)
        self._dict_days_by_month = comUtility.Utility.GetMonthMaxDayDict(yyyy_mm_list)

    def _setup_db_connection(self):
        self._conMgr = dbConMgr.ConnectionManager()
        self._conMgr.LoadConInfo()

    def _setup_file_connection(self):
        self._conMgr: fileConMgr.FileManager = fileConMgr.FileManager()
        self._conMgr.setup_object()

    def _get_dict_prod_yield(self):
        return self._dict_prod_yield

    def _build_dict_prod_yield(self):
        self._dict_prod_yield: dict = {}
        for idx, row in self.dbProdYield.iterrows():
            if row['oper'] not in self._dict_prod_yield.keys():
                self._dict_prod_yield[row['oper']] = {row['grade']: row['prod_yield']}
            else:
                if row['grade'] not in self._dict_prod_yield[row['oper']].keys():
                    self._dict_prod_yield[row['oper']][row['grade']] = row['prod_yield']
                else:
                    raise KeyError(
                        "Production Yield 테이블의 키가 중복되었습니다."
                    )

    def _preprocessing(self):
        # Changing dtypes
        self._change_column_dtype(df_name="demand", col_name="yyyymm", dtype="str")
        self._change_column_dtype(df_name="demand", col_name="qty", dtype="float")

    def _change_column_dtype(self, df_name: str, col_name: str, dtype: str):
        if not self._chk_column_name(column_name=col_name, df_name=df_name):
            raise KeyError(
                f"데이터 프레임 {df_name} 에는 칼럼 [{col_name}] 가 존재하지 않습니다."
            )
        df: pd.DataFrame = self._get_attr(df_name)
        df[col_name] = df[col_name].astype(dtype)

    def _chk_column_name(self, column_name: str, df_name: str = "_df"):
        does_exists: bool = False
        if not self._check_is_df(df_name=df_name):
            return does_exists
        df: pd.DataFrame = self._get_attr(attr=df_name)
        if column_name in df.columns:
            does_exists = True
        return does_exists

    def _check_is_df(self, df_name: str):
        is_df: bool = False
        if not self._chk_exists(attr=df_name):
            return is_df
        attr = self._get_attr(attr=df_name)
        if type(attr) is pd.DataFrame:
            is_df = True
        return is_df

    def _get_attr(self, attr: str):
        attr_obj = None
        if self._chk_exists(attr=attr):
            attr_obj = self.__getattribute__(attr)
        return attr_obj

    def _chk_exists(self, attr: str):
        existence: bool = attr in self.__dict__.keys()
        return existence

    # ============================================================================================================== #
    # Data -> DB Upload 모듈
    # ============================================================================================================== #

    def SaveProdScheduleRslt(self, prodScheduleRslt:list):
        prodScheduleArr = prodScheduleRslt
        self.UpdateSchedHourRslt(schedHourRsltArr=prodScheduleArr)

    # Prduction Scheduling Result (Hourly)
    def UpdateSchedHourRslt(self, schedHourRsltArr: list):
        '''
        Send Production Schedule Hourly Result Array to DB.
        '''

        # self._conMgr = dbConMgr.ConnectionManager()
        # self._conMgr.LoadConInfo()

        strTemplate: str = """ insert into SCMUSER.TB_FS_QTY_HH_DATA(
                                    FS_VRSN_ID, PLANT_NAME, LINE_NAME, PLAN_CODE, SALE_MAN, PRODUCT, CUSTOMER,
                                    LOT_NO, DATE_FROM, DATE_TO, DATE_FROM_TEXT, DATE_TO_TEXT, COLOR, DURATION, QTY
                               )values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15) """

        totLen = len(schedHourRsltArr)
        flag = False
        errCnt = 0
        sqlDel= "delete from SCMUSER.TB_FS_QTY_HH_DATA"
        errCode = 0
        while flag == False:
            flag, errCode = self._conMgr.BatchQuery(sqlTemplate=strTemplate, dataArr=schedHourRsltArr, sqlDel=sqlDel)
            if flag == True:
                if errCnt > 0:
                    print("success - [재전송]")
                else:
                    print("success")
                errCnt = 0
                break
            else:
                errCnt += 1
                print("fail")
                # self._sendDataErrorProc(errCnt=errCnt, fnName="UpdateUnpegStatus")

        return flag, errCode

    # Prduction Scheduling Result (Daily)
    def UpdateSchedDailyRslt(self, schedDailyRsltArr: list):
        '''
        Send Production Schedule Daily Result Array to DB.
        '''

        # self._conMgr = dbConMgr.ConnectionManager()
        # self._conMgr.LoadConInfo()

        strTemplate: str = """ insert into SCMUSER.TB_FS_QTY_DD_DATA(
                                    FS_VRSN_ID, PLANT_NAME, LINE_NAME, MATRL_CD, MATRL_DESCR, PROD_DATE, DAILY_QTY,
                                    DAILY_DURATION, DEMAND_TYPE, DELETE_KEY)
                               )values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)"""

        totLen = len(schedDailyRsltArr)
        flag = False
        errCnt = 0
        sqlDel= "delete from SCMUSER.TB_FS_QTY_DD_DATA"
        errCode = 0
        while flag == False:
            flag, errCode = self._conMgr.BatchQuery(sqlTemplate=strTemplate, dataArr=schedDailyRsltArr, sqlDel=sqlDel)
            if flag == True:
                if errCnt > 0:
                    print("success - [재전송]")
                else:
                    print("success")
                errCnt = 0
                break
            else:
                errCnt += 1
                print("fail")
                # self._sendDataErrorProc(errCnt=errCnt, fnName="UpdateUnpegStatus")

        return flag, errCode

        # Engine Configuration Histroy(HT_CONFIG) DB에 저장
    def UpdateEngConfHistory(self, engConfArr: list):

        # self._conMgr = dbConMgr.ConnectionManager()
        # self._conMgr.LoadConInfo()

        strTemplate: str = """ insert into SCMUSER.TB_FS_PS_CONFIG(
                                    DATASET_ID, SIMUL_NUM, CONFIG_NAME, CONFIG_VALUE, CREATE_DATE
                                )values(:1, :2, :3, :4, sysdate) """
        flag = False
        errCnt = 0
        sqlDel = ""
        errCode = 0
        while flag == False:
            flag, errCode = self._conMgr.BatchQuery(sqlTemplate=strTemplate, dataArr=engConfArr, sqlDel=sqlDel)
            if flag == True:
                errCnt = 0
                break
            else:
                print(engConfArr)
                # self._sendDataErrorProc(errCnt=errCnt, fnName="UpdateEngConfHistory")

        return flag, errCode

    def _sendDataErrorProc(self, errCnt:int, fnName:str):
        if errCnt < 11:
            time.sleep(1)
        elif errCnt < 21:
            time.sleep(2)
        elif errCnt < 31:
            time.sleep(4)
        else:
            self.CloseDataMgr()
            assert errCnt < 31, "[DataManager.{}] Send Machine history to DB failed. stop program.".format(fnName)
