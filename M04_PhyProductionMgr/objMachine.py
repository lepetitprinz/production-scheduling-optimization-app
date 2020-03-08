# -*- coding: utf-8 -*-

from M03_Site import simFactoryMgr


class Machine(object):
    def __init__(self, factory: simFactoryMgr, mac_id: str):
        self._factory: simFactoryMgr = factory
        self.id: str = mac_id

        #
        self.additive: str = ""

        # PACKAGING MACHINE PROPERTIES
        self.Uom: str = ""          # 25 KG / 750 KG / BULK
        self.PackKind: str = ""     # WV / FS / BK / SB

        # STATUS
        self.status: str = "IDLE"

        # CURRENT PROCESSING LOT
        self.Lot:  = ""

    def setup_object(self, status: str, additive: str, ):
        self.status = status

    def RunMachine(self):
        pass
