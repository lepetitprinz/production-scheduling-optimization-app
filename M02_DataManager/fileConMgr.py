import os
import pandas as pd

from M06_Utility import comUtility


class FileManager(object):
    def __init__(self):
        self._data_dir: str = ""
        self.csv_paths: dict = {}
        self.csv_data_dict: dict = {}

    def setup_object(self):
        self._data_dir = self._search_data_dir()
        self._find_csv_paths()
        self._reload_csv_batch()

    def loadData(self, data_name: str):
        if data_name not in self.csv_data_dict.keys():
            raise KeyError(
                f"data_name : {data_name} 은 File Connector 에서 검색되지 않았습니다."
            )
        return self.csv_data_dict[data_name]

    def set_csv_path(self, data_name: str, csv_path: str):
        if not self._chk_is_csv(path=csv_path):
            raise TypeError(
                f"csv_path 파일 경로가 존재하지 않습니다. : {csv_path}"
            )
        if data_name in self.csv_paths.keys():
            raise TypeError(
                f"data_name = {data_name} 값은 기존 키 목록과 중복됩니다."
            )
        self.csv_paths[data_name] = csv_path

    def _reload_csv_batch(self):
        for data_name in self.csv_paths.keys():
            self._reload_csv(data_name=data_name)

    def _reload_csv(self, data_name: str):
        if data_name not in self.csv_paths.keys():
            raise KeyError(
                f"No Such data_name Called: {data_name} "
            )
        self.csv_data_dict[data_name]: pd.DataFrame = pd.read_csv(self.csv_paths[data_name])

    def _chk_is_csv(self, path: str):
        is_csv: bool = False
        if not self._chk_is_file(path=path):
            return is_csv
        if os.path.splitext(os.path.basename(path))[-1].lower() != ".csv":
            return is_csv
        is_csv = True
        return is_csv

    def _chk_is_file(self, path: str):
        is_file: bool = False
        if not self._chk_existence(path=path):
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

    def _find_csv_paths(self):
        for directory, folder, filenames in os.walk(self._data_dir):
            for filename in filenames:
                filename_base: str = os.path.splitext(filename)[0]
                filename_ext: str = os.path.splitext(filename)[-1].lower()
                filepath: str = os.path.join(directory, filename)
                if filename_ext == ".csv":
                    self.set_csv_path(data_name=filename_base, csv_path=filepath)

    def _search_data_dir(self):
        # data 폴더 경로 찾는 처리
        data_dir: str = ""
        for fnm in os.listdir(comUtility.Utility.project_dir):
            if fnm == "data":
                data_dir = os.path.join(comUtility.Utility.project_dir, fnm)
                return data_dir
        return data_dir