from DataManager import dbDataMgr
from Utility import comUtility

import numpy as np
from itertools import permutations

class Simulator:
    def __init__(self):
        self._util = comUtility.Utility
        self.DataMgr: dbDataMgr.DataManager = None


    def SetupDbObject(self):
        self.DataMgr = dbDataMgr.DataManager()
        self.DataMgr.SetupObject()
        self._util.SetupObject()

    def getDmdReactorProdGroup(self, gradeGroup:dict, dmdGradeList:list):
        '''
        gradeGroup: {gradeGroup1 : [GRADE_A, ...],
                    gradeGroup2 : [GRADE_D, ...],
                    ....}
        dmdGradeList: ['GRADE_A', 'GRADE_B', ...., 'GRADE_G]
        '''
        dmdReactorProdDict = {}

        for key in gradeGroup.keys():
            for grade in dmdGradeList:
                if grade in gradeGroup[key]:
                    if key not in dmdReactorProdDict.keys():
                        dmdReactorProdDict.update({key: [grade]})
                    else:
                        if grade not in dmdReactorProdDict[key]:
                            valList = dmdReactorProdDict[key].append(grade)
                            dmdReactorProdDict.update({key: valList})

        return dmdReactorProdDict

    def getMinProdWheelCost(self, dmdReactorProdDict:dict, gradeCostDict:dict, costCalStandard:str = 'hour'):

        '''
        dmdReactorProdDict : {gradeGroup1: [GRADE_A, ...],
                              gradeGroup2 : [GRADE_D, ...]}
        gradeCostDict : {(GRADE_A, GRADE_B) : [Hour, OG_Qty],
                         (GRADE_A, GRADE_C) : [Hour, OG_Qty],
                         ...}
        costCalStandard : hour or ogQty
        '''

        appliedGradeCost = {}

        if costCalStandard == 'hour':
            for key, val in gradeCostDict.items():
                appliedGradeCost.update({key, val[0]})
        else:
            for key, val in gradeCostDict.items():
                appliedGradeCost.update({key, val[1]})

        gradeGroupSeq = []
        gradeSeq = []

        for val in dmdReactorProdDict.values():
            gradeGroupSeq.append(list(permutations(val, len(val))))

        # Make production schedule sequence
        for group1 in gradeGroupSeq[0]:
            group1 = list(group1)
            for group2 in gradeGroupSeq[1]:
                group2 = list(group2)
                seq = np.append(group1, group2)
                gradeSeq.append(seq)""
                \
