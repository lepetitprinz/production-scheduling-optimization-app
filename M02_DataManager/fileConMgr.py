# -*- coding: utf-8 -*-

import os


class FileManager(object):
    def __init__(self):
        self.csv_paths: dict = {}

    def setup_object(self):
        pass

    def set_csv_path(self, data_name: str, csv_path: str):
        if self._chk_is_csv(path=csv_path):
            raise TypeError(
                f"csv_path 파일 경로가 존재하지 않습니다. : {csv_path}"
            )
        if data_name in self.csv_paths.keys():
            raise TypeError(
                f"data_name = {data_name} 값은 기존 키 목록과 중복됩니다."
            )
        self.csv_paths[data_name] = csv_path

    def _chk_is_csv(self, path: str):
        is_csv: bool = False
        if self._chk_is_file(path=path):
            return is_csv
        if os.path.splitext(os.path.basename(path))[-1].lower() != ".csv":
            return is_csv
        is_csv = True
        return is_csv

    def _chk_is_file(self, path: str):
        is_file: bool = False
        if self._chk_existence(path=path):
            return is_file
        if not os.path.isfile(path):
            return is_file
        is_file = True
        return is_file

    def _chk_existence(self, path: str):
        does_exist: bool = False
        if not os.path.exists(path):
            return does_exist
        does_exist = True
        return does_exist

    def


def test():
    pass


if __name__ == '__main__':
    test()
