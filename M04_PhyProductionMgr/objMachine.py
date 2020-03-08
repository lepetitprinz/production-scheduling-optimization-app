# -*- coding: utf-8 -*-

from M03_Site import simFactoryMgr


class Machine(object):
    def __init__(self, factory: simFactoryMgr, mac_id: str):
        self._factory: simFactoryMgr = factory
        self.Id: str = mac_id

        #
        self.Additive: str = ""

        # PACKAGING MACHINE PROPERTIES
        self.Uom: str = ""          # 25 KG / 750 KG / BULK
        self.PackKind: str = ""     # WV / FS / BK / SB

        # STATUS
        self.Status: str = "IDLE"   # IDLE / PROGRESS

        # CURRENT PROCESSING LOT
        self.Lot: str = ""

    def setup_object(self, status: str, additive: str, ):
        self.Status = status

    def RunMachine(self):
        pass
