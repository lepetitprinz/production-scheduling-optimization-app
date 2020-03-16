# -*- coding: utf-8 -*-

from M01_Simulator.PE_Simulator import Simulator


def run_simulator(source: str, day_start_time: str, dmdMonth: int = None):
    simul: Simulator = Simulator()
    flag = simul.SetupDbObject(source=source, day_start_time=day_start_time, dmdMonth=dmdMonth)

    # Simulation 구동
    simul.run_simulator()
    print("\nSIMULATION END")

    # Simulation 가동 종료 후 Scheduling 결과 저장
    # simul.SaveSimulData()


if __name__ == '__main__':
    run_simulator(source="db", day_start_time="00:00:00", dmdMonth=6)