# -*- coding: utf-8

import datetime
from datetime import timedelta
import pandas as pd
import time

from M02_DataManager import dbConMgr, fileConMgr
from M03_Site import simFactoryMgr
from M05_ProductManager import objLot
from M06_Utility import comUtility

class DataManager:

    def __init__(self, source: str = "db", dmdMonth: int = None):

        # 기본 정보
        self._source: str = source
        self._conMgr = None

        #
        self._fsVerId: str = ''
        self.dmdMonth: int = dmdMonth

        # 쿼리 및 파일 Read 결과를 담을 Array 변수들을 선언
        self.dbDemand: pd.DataFrame = None
        self.dbProdWheel: pd.DataFrame = None
        self.dbProdYield: pd.DataFrame = None
        self.dbProdMst: pd.DataFrame = None

        # configuration 정보
        self.dbEngConf: pd.DataFrame = None  # Engine Configuration 정보
        self.dbMacUnAvlTime: pd.DataFrame = None

        # Dictionary
        self._dmdToQtyDict: dict = {}
        self._prodYieldDict: dict = {}
        self._dict_days_by_month: dict = {}
        self._prodToYieldDict: dict = {}

    def SetupEngConfData(self):
        self._conMgr = dbConMgr.ConnectionManager()
        self._conMgr.LoadConInfo()
        engConfig = self._conMgr.GetDbData(self._conMgr.GetEngineConfDataSql())
        engConfigColName = ['paramCode', 'paramName', 'paramVal']
        self.dbEngConf = pd.DataFrame(engConfig, columns=engConfigColName)

        return self.dbEngConf

    def SetupObject(self):
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
            if self.dmdMonth is None:
                demand = self._conMgr.GetDbData(self._conMgr.GetDpQtyDataSql())
            else:
                from_yyyy: str = comUtility.Utility.PlanStartDay[:4]
                from_mm: str = "%02d" % self.dmdMonth
                from_yyyymm: str = from_yyyy + from_mm
                to_dd: int = comUtility.Utility.GetMonthMaxDay(year=int(from_yyyy), month=self.dmdMonth)[-1]
                demand = self._conMgr.GetDbData(self._conMgr.GetDpQtyDataSql_Custom(from_yyyymm, from_yyyymm))

                comUtility.Utility.setDayStartDate(year=int(from_yyyy), month=int(from_mm), day=1)
                comUtility.Utility.SetDayHorizon(days=to_dd)
                comUtility.Utility.CalcDayEndDate()

            prodMst = self._conMgr.GetDbData(self._conMgr.GetProdMstDataSql())
            prodWheel = self._conMgr.GetDbData(self._conMgr.GetProdWheelDataSql())
            prodYield = self._conMgr.GetDbData(self._conMgr.GetFpCapaMstDataSql())

            macUnAvlTime = self._conMgr.GetDbData(self._conMgr.GetMacUnAvlTimeDataSql())
            # engConfig = self._conMgr.GetDbData(self._conMgr.GetEngineConfDataSql())

            # Data Column 정의
            dmdColName = ['yyyymm', 'prodCode', 'product', 'qty']
            prodMstColName = ['prodCode', 'prodName']
            prodWheelColName = ['grade_from', 'grade_to', 'hour', 'og']
            prodYieldColName = ['oper', 'prodCode', 'grade', 'prod_yield']

            macUnAvlColName = ['operId', 'macId', 'fromTime', 'toTime']
            # engConfigColName = ['paramCode', 'paramName', 'paramVal']

            self.dbDemand = pd.DataFrame(demand, columns=dmdColName)
            self.dbProdMst = pd.DataFrame(prodMst, columns=prodMstColName)
            self.dbProdWheel = pd.DataFrame(prodWheel, columns=prodWheelColName)
            self.dbProdYield = pd.DataFrame(prodYield, columns=prodYieldColName)

            self.dbMacUnAvlTime = pd.DataFrame(macUnAvlTime, columns=macUnAvlColName)

            # self.dbEngConf = pd.DataFrame(engConfig, columns=engConfigColName)

        # self.df_demand = self._conMgr.load_data(data_name="demand")
        # self.dfProdWheel = self._conMgr.load_data(data_name="prod_wheel")
        # self.df_prod_yield = self._conMgr.load_data(data_name="prod_yield")

        # self._preprocessing()
        self._getProdMstDict()
        self._getDmdQtyDict()
        self._getProdYieldDict()

        self._fsVerId = comUtility.Utility.FsVerId
        comUtility.Utility.DmdQtyDict = self._dmdToQtyDict.copy()
        comUtility.Utility.ProdWheelDf = self.dbProdWheel.copy()
        # comUtility.Utility.SetRootPath(rootPath=self._conMgr.RootPath)
        # comUtility.Utility.SetConfPath(confPath=self._conMgr.conf_path)

    def CloseDataMgr(self):
        self._conMgr.CloseConnection()
        self.__init__()

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
        return self._prodYieldDict

    # ================================================================================================ #
    # Data Dictionary 만드는 처리
    # ================================================================================================ #
    def _getDmdQtyDict(self):
        dmdToQtyDict = {}
        for idx, row in self.dbDemand.iterrows():
            dmdToQtyDict[(row['yyyymm'], row['prodCode'])] = row['qty']
        self._dmdToQtyDict = dmdToQtyDict

    def _getProdYieldDict(self):

        prodToYieldDict = {}
        for idx, row in self.dbProdYield.iterrows():
            if row['oper'] not in self._prodYieldDict.keys():
                self._prodYieldDict[row['oper']] = {row['grade']: row['prod_yield']}
            else:
                if row['grade'] not in self._prodYieldDict[row['oper']].keys():
                    self._prodYieldDict[row['oper']][row['grade']] = row['prod_yield']
                else:
                    raise KeyError(
                        "Production Yield 테이블의 키가 중복되었습니다."
                    )

        for idx, row in self.dbProdYield.iterrows():
            prodToYieldDict[row['prodCode']] = row['prod_yield']

        self._prodToYieldDict = prodToYieldDict

    def _getProdMstDict(self):
        prodMstDict = {}
        for idx, row in self.dbProdMst.iterrows():
            prodMstDict[row['prodName']] = row['prodCode']

        self._prodMstDict = prodMstDict
        comUtility.Utility.ProdMstDict = prodMstDict

    # ============================================================================================================== #
    # Data -> DB Upload Module
    # ============================================================================================================== #

    def SaveProdScheduleRslt(self, prodScheduleRslt:list):
        prodScheduleArr = prodScheduleRslt

        dailySchedArr = self._makeDailySchedRslt(prodScheduleArr=prodScheduleArr)

        self.UpdateSchedDailyRslt(schedDailyRsltArr=dailySchedArr)
        self.UpdateSchedHourRslt(schedHourRsltArr=prodScheduleArr)

    def _makeDailySchedRslt(self, prodScheduleArr: list):
        dailySchedArr = []
        # 일별 분할을 위한 데이터 전처리
        schedColumns = ['FS_VRSN_ID', 'PLANT_NAME' ,'LINE_NAME','PLAN_CODE',
                      'SALE_MAN', 'PRODUCT', 'CUSTOMER', 'LOT_NO',
                      'DATE_FROM', 'DATE_TO', 'DATE_FROM_TEXT', 'DATE_TO_TEXT',
                      'COLOR', 'DURATION', 'QTY']
        scheduleDf = pd.DataFrame(prodScheduleArr, columns=schedColumns)
        scheduleDf = scheduleDf.drop(['DATE_FROM', 'DATE_TO'], axis=1)

        for i in range(len(scheduleDf)):
            dateFrom = datetime.datetime.strptime(scheduleDf.loc[i, 'DATE_FROM_TEXT'], "%Y-%m-%d %H:%M:%S")
            scheduleDf.loc[i, 'Date'] = dateFrom.date()
        scheduleDf = scheduleDf.sort_values(['DATE_FROM_TEXT', 'DATE_TO_TEXT'])
        scheduleDf = scheduleDf.reset_index(drop=True)

        schedStartTime = comUtility.Utility.PlanStartTime
        schedEndTime = comUtility.Utility.PlanEndTime
        schedStartDateTime = datetime.datetime.strptime(schedStartTime, '%Y%m%d')
        schedEndDateTime = datetime.datetime.strptime(schedEndTime, '%Y%m%d')
        schedPeriod = str(schedEndDateTime-schedStartDateTime)
        schedPeriodDays = int(schedPeriod.split()[0]) + 1

        lineKind = []
        for idx, row in scheduleDf.iterrows():
            if row['LINE_NAME'] not in lineKind:
                lineKind.append(row['LINE_NAME'])

        for line in lineKind:
            prodDayLine, timeDict = self._getSchedLineDaily(scheduleDf=scheduleDf, line=line,
                                                            schedPeriod=schedPeriodDays, startDate=schedStartTime)
            for idx, row in prodDayLine.iterrows():
                prodDateStr = row['PROD_DATE'].strftime("%Y%m%d")
                dailySched = [
                    row['FS_VRSN_ID'],
                    row['PLANT_NAME'],
                    row['LINE_NAME'],
                    row['PRODUCT'],
                    '',
                    prodDateStr,
                    int(row['DAILY_QTY']),
                    round(row['DURATION_DAY'], 2),
                    ''
                ]
                dailySchedArr.append(dailySched)

        return dailySchedArr

    def _getSchedLineDaily(self, scheduleDf:pd.DataFrame, line:str, schedPeriod:int, startDate:str):
        idx = 0
        prodUpdate = scheduleDf.copy()
        prodUpdate['DURATION'] = prodUpdate['DURATION'] / 3600
        prodUpdate = prodUpdate[prodUpdate['LINE_NAME'] == line]
        prodUpdate.reset_index(drop=True, inplace=True)
        prodDayLine = pd.DataFrame(columns=prodUpdate.columns)

        period = schedPeriod
        startDatetime = datetime.datetime.strptime(startDate, '%Y%m%d')

        timeDict = {}
        for time in range(period):
            date = startDatetime + timedelta(days=time)
            date = date.date()
            timeDict.update({date: 24})

            for i in range(len(prodUpdate)):
                if prodUpdate.loc[i, 'Date'] == date:
                    if timeDict[date] > prodUpdate.loc[i, 'DURATION'] and prodUpdate.loc[i, 'DURATION'] > 0:
                        prodDayLine.loc[idx, :] = prodUpdate.loc[i, :]
                        prodDayLine.loc[idx, 'PROD_DATE'] = date
                        prodDayLine.loc[idx, 'DURATION_DAY'] = prodUpdate.loc[i, 'DURATION']

                        timeDict.update({date: timeDict[date] - prodUpdate.loc[i, 'DURATION']})
                        prodUpdate.loc[i, 'DURATION'] = 0
                        idx += 1

                    elif timeDict[date] < prodUpdate.loc[i, 'DURATION'] and prodUpdate.loc[i, 'DURATION'] > 0:
                        prodDayLine.loc[idx, :] = prodUpdate.loc[i, :]
                        prodDayLine.loc[idx, 'PROD_DATE'] = date
                        prodDayLine.loc[idx, 'DURATION_DAY'] = timeDict[date]

                        prodUpdate.loc[i, 'DURATION'] = prodUpdate.loc[i, 'DURATION'] - timeDict[date]
                        timeDict.update({date: 0})
                        prodUpdate.loc[i, 'Date'] += timedelta(days=1)
                        idx += 1
                        continue

        for idx, row in prodDayLine.iterrows():
            prodCode = prodDayLine.loc[idx, 'PRODUCT']
            prodDayLine.loc[idx, 'DAILY_QTY'] = round(prodDayLine.loc[idx, 'DURATION_DAY'] * self._prodToYieldDict[prodCode],0)

        return prodDayLine, timeDict

    # Prduction Scheduling Result (Hourly)
    def UpdateSchedHourRslt(self, schedHourRsltArr: list):
        '''
        Send Production Schedule Hourly Result Array to DB.
        '''

        strTemplate: str = """ insert into SCMUSER.TB_FS_QTY_HH_DATA(
                                    FS_VRSN_ID, PLANT_NAME, LINE_NAME, PLAN_CODE, SALE_MAN, PRODUCT, CUSTOMER,
                                    LOT_NO, DATE_FROM, DATE_TO, DATE_FROM_TEXT, DATE_TO_TEXT, COLOR, DURATION, QTY
                               )values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15) """

        totLen = len(schedHourRsltArr)
        flag = False
        errCnt = 0
        # sqlDel= ""
        sqlDel= "delete from SCMUSER.TB_FS_QTY_HH_DATA where FS_VRSN_ID = '{}'".format(comUtility.Utility.FsVerId)
        errCode = 0
        while flag == False:
            flag, errCode = self._conMgr.BatchQuery(sqlTemplate=strTemplate, dataArr=schedHourRsltArr, sqlDel=sqlDel)
            if flag == True:
                if errCnt > 0:
                    print("success - [재전송]")
                else:
                    print("Saving Hourly Schedule : success")
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

        strTemplate: str = """ insert into SCMUSER.TB_FS_QTY_DD_DATA(
                                    FS_VRSN_ID, PLANT_NAME, LINE_NAME, MATRL_CD, MATRL_DESCR,
                                    PROD_DATE, DAILY_QTY, DAILY_DURATION, DEMAND_TYPE)
                               values(:1, :2, :3, :4, :5, :6, :7, :8, :9)"""

        totLen = len(schedDailyRsltArr)
        flag = False
        errCnt = 0
        sqlDel= "delete from SCMUSER.TB_FS_QTY_DD_DATA where FS_VRSN_ID = '{}'".format(comUtility.Utility.FsVerId)
        errCode = 0
        while flag == False:
            flag, errCode = self._conMgr.BatchQuery(sqlTemplate=strTemplate, dataArr=schedDailyRsltArr, sqlDel=sqlDel)
            if flag == True:
                if errCnt > 0:
                    print("success - [재전송]")
                else:
                    print("Saving Daily Schedule : success")
                errCnt = 0
                break
            else:
                errCnt += 1
                print("fail")
                # self._sendDataErrorProc(errCnt=errCnt, fnName="UpdateUnpegStatus")

        return flag, errCode

    def SaveShortageRslt(self, shortageLotList):
        strTemplate: str = """ insert into SCMUSER.TB_FS_SHORTAGE_DATA(
                                    FS_VRSN_ID, PLAN_YYMM, PROD_CODE, LOT_ID, GRADE, PACK_SIZE, PACK_KIND,
                                    LOT_QTY, INPUT_QTY, LOCATION, CREATE_DATE
                                )values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, sysdate) """

        shortageRsltArr = self._getShortageLotArr(shortageLotList=shortageLotList)

        flag = False
        errCnt = 0
        sqlDel = "delete from SCMUSER.TB_FS_SHORTAGE_DATA where FS_VRSN_ID = '{}'".format(comUtility.Utility.FsVerId)
        errCode = 0
        while flag == False:
            flag, errCode = self._conMgr.BatchQuery(sqlTemplate=strTemplate, dataArr=shortageRsltArr, sqlDel=sqlDel)
            if flag == True:
                if errCnt > 0:
                    print("success - [재전송]")
                else:
                    print("Saving Shortage Result : success")
                errCnt = 0
                break
            else:
                errCnt += 1
                print("fail")

    def _getShortageLotArr(self, shortageLotList: list):
        shortageRsltArr = []

        for lot in shortageLotList:
            lotObj:objLot.Lot = lot
            lotDueDateStr = lotObj.DueDate.strftime('%Y%m%d')[:-2]
            inputQty = comUtility.Utility.DmdQtyDict[(lotDueDateStr, lotObj.ProdCode)]
            shortageLot = [
                self._fsVerId,      # FS Version Id
                lotDueDateStr,      # Due DaTE(YYYYMM)
                lotObj.ProdCode,    # Product Code
                lotObj.Id,          # Lot Id
                lotObj.Grade,       # Grade
                lotObj.PackSize,    # Package Size
                lotObj.PackType,    # Package Type
                lotObj.Qty,         # Lot Qty
                inputQty,           # Demand Qty
                lotObj.CurrLoc      # Current Location
            ]
            shortageRsltArr.append(shortageLot)

        return shortageRsltArr

    # Engine Configuration Histroy(HT_CONFIG) DB에 저장
    def UpdateEngConfHistory(self, engConfArr: list):

        strTemplate: str = """ insert into SCMUSER.TB_FS_PS_CONFIG(
                                    CONFIG_NAME, CONFIG_VALUE, CREATE_DATE
                                )values(:1, :2, :3) """
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


    # def _preprocessing(self):
    #     # Changing dtypes
    #     self._change_column_dtype(df_name="demand", col_name="yyyymm", dtype="str")
    #     self._change_column_dtype(df_name="demand", col_name="qty", dtype="float")
    #
    # def _change_column_dtype(self, df_name: str, col_name: str, dtype: str):
    #     if not self._chk_column_name(column_name=col_name, df_name=df_name):
    #         raise KeyError(
    #             f"데이터 프레임 {df_name} 에는 칼럼 [{col_name}] 가 존재하지 않습니다."
    #         )
    #     df: pd.DataFrame = self._get_attr(df_name)
    #     df[col_name] = df[col_name].astype(dtype)
    #
    # def _chk_column_name(self, column_name: str, df_name: str = "_df"):
    #     does_exists: bool = False
    #     if not self._check_is_df(df_name=df_name):
    #         return does_exists
    #     df: pd.DataFrame = self._get_attr(attr=df_name)
    #     if column_name in df.columns:
    #         does_exists = True
    #     return does_exists
    #
    # def _check_is_df(self, df_name: str):
    #     is_df: bool = False
    #     if not self._chk_exists(attr=df_name):
    #         return is_df
    #     attr = self._get_attr(attr=df_name)
    #     if type(attr) is pd.DataFrame:
    #         is_df = True
    #     return is_df
    #
    # def _get_attr(self, attr: str):
    #     attr_obj = None
    #     if self._chk_exists(attr=attr):
    #         attr_obj = self.__getattribute__(attr)
    #     return attr_obj
    #
    # def _chk_exists(self, attr: str):
    #     existence: bool = attr in self.__dict__.keys()
    #     return existence
