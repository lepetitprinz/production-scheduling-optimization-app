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


def test(source: str, day_start_time: str, dmdMonths: list):
    simul: Simulator = Simulator()
    for dmdMonth in dmdMonths:
        flag = simul.SetupDbObject(source=source, day_start_time=day_start_time, dmdMonth=dmdMonth)

        # Simulation 구동
        simul.run_simulator()


if __name__ == '__main__':
    test(source="db", day_start_time="00:00:00", dmdMonths=[3, 4, 5])
    # for i in [3, 4, 5]:
    #     run_simulator(source="db", day_start_time="00:00:00", dmdMonth=i)
