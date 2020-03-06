# -*- coding: utf-8 -*-


class Lot(object):
    def __init__(self, id: str, grade:str, DueDate:str):
        self.id = id
        self.Grade = grade
        self.DueDate = DueDate