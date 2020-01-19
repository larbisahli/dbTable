#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright 2019 Larbi Sahli

__Author__ = "Larbi Sahli"
__Copyright__ = "Copyright \xa9 2019 Larbi Sahli => https://github.com/larbisahli"
__License__ = "Public Domain"
__Version__ = "1.7.0"
__Table_Rows_Limit__ = 1000
__all__ = ["dbTable"]

import shutil
import hashlib
import pickle
import os
import stat
import sys
import onetimepad
import signal
from threading import Thread
import pickledb

current_directory = os.path.dirname(os.path.realpath(__file__))
default_key = "37e1f5637615c8ab9474184a2970fdcb8814303f49e56deb6c31cb7a1c12655c"


def _hash_(dir_):
    return hashlib.sha256(dir_.encode()).hexdigest()[:40]


def _encrypt(data, key_, state):
    if state:
        return onetimepad.encrypt(data, key_)
    return data


def _decrypt(encrypted_data, key_, state):
    if state:
        return onetimepad.decrypt(encrypted_data, key_)
    return encrypted_data


def _is_backup(*, file):
    """ Check which of the files is available, file or its backup """
    with open(file, 'rb') as r:
        try:
            pickle.load(r)
            return False
        except EOFError:
            return True


def _F_B_switch(file, backup, change, /):
    """
    Instead of reading the whole file into the memory and write it again
    to make a small change or an update, it is better to toggle between
    a file and its backup if we use pickle.
    """
    # if fb_state is False write file to backup, else write backup to file
    fb_state = _is_backup(file=file)
    read = file if not fb_state else backup
    write = backup if not fb_state else file
    with open(read, 'rb') as r:
        os.chmod(write, stat.S_IWRITE)
        with open(write, 'ab') as a:
            try:
                while dictionary := pickle.load(r):
                    if dictionary.__contains__(change):
                        continue
                    pickle.dump(dictionary, a)
            except EOFError:
                pass
            os.chmod(write, stat.S_IREAD)
    os.chmod(read, stat.S_IWRITE)
    # clear the read file
    with open(read, 'wb'):
        os.chmod(read, stat.S_IREAD)


class _Manager:

    def __init__(self, db_path, db_name, table_name, enable_key, encrypt_key, sig):

        self.path = os.path.join(f"{db_path}", f"{db_name}")
        if not os.path.isdir(self.path):
            # create database dir
            os.mkdir(self.path)

        self._enable_key = enable_key
        self._TableName = table_name
        self._default_key = encrypt_key
        self._Table_dir = os.path.join(f"{self.path}", f"{self._TableName}")
        self.MetaData = os.path.join(f"{self._Table_dir}", "MetaData.db")
        self.db = pickledb.load(self.MetaData, True)

        if not os.path.isdir(self._Table_dir):
            # create Table's dir
            os.mkdir(self._Table_dir)
            self.db.dcreate("Metadata")
            self.db.dcreate("d_tree")
            self.db.dadd("Metadata", ("key", _hash_(self._default_key)))
            self.db.dadd("Metadata", ("deleted_id", []))
            self.db.dadd("Metadata", ("last_entry", ""))

        self.db.dadd("Metadata", ("enable_key", self._enable_key))

        self.dthread = None
        if sig:
            signal.signal(signal.SIGTERM, self.sigterm_handler)

    def sigterm_handler(self):
        ''' Assigns sigterm_handler for graceful shutdown '''
        if self.dthread is not None:
            self.dthread.join()
        sys.exit(0)

    def _F_B_switch(self, file, backup, change=None):

        self.dthread = Thread(
            target=_F_B_switch,
            args=(file, backup, change))
        self.dthread.start()
        self.dthread.join()

    def check_db(self, value):
        print(self.db.dgetall("d_tree"))
        return self.db.lexists(name="d_tree", value=value)

    def set(self, row, dictionary):

        del_state = False
        if len(del_id := self.db.dget("Metadata", "deleted_id")) != 0:
            counter, d = [int(i) for i in del_id.split(':') if i != ""]
            del_state = True
        elif (last_id := self.db.dget("Metadata", "last_entry")) != 0:
            counter, d = [int(i) for i in last_id.split(':') if i != ""]
        else:
            d = 1
            counter = 0
            value = []

        path = f"{counter}:{d}:"  # in case fb_state is not None
        if not del_state:
            counter += 1

            path = f"{counter}:{d}:"
            self.db.dadd("Metadata", ("last_entry", path))

            if counter == 101:  # 100 rows limit in each _D_ file
                d += 1
                path = f"{1}:{d}:"
                self.db.dadd("Metadata", ("last_entry", path))

            if d > 10:  # 10 _D_ files in Table folder => 1000 rows
                return False

        if del_state:
            del_ids = self.db.dget("Metadata", "deleted_id")
            del_ids.remove(":".join([counter, d]))
            self.db.dadd("Metadata", ("deleted_id", del_ids))

        print(row, dictionary)
        print(self.db.dgetall("Metadata"))


class dbTable(_Manager):
    row_error = TypeError("Error, the keyword row= is empty!")
    row_exist = TypeError("row already exist.")
    dict_error = TypeError("Error, the keyword _dict= must be a dictionary!")

    def __init__(self, db_path=current_directory, db_name="_dbTable_", table_name=None,
                 encrypt_key=default_key, enable_key=False, sig=True):
        super().__init__(db_path, db_name, table_name, enable_key, encrypt_key, sig)

    def insert(self, *, row=None, _dict=None, **kwargs):
        # xx = self.db.dget("Metadata", "deleted_id")
        # xx.append("34:2")
        # self.db.dadd("Metadata", ("deleted_id", xx))

        # print(self.db.dgetall("Metadata"))
        # print(self.db.dgetall("d_tree"))

        if self.db.dexists("Metadata", "key") and self.db.dget("Metadata", "key") != _hash_(self._default_key):
            raise TypeError(f"Access denied, the encryption key {self._default_key}"
                            f"is not valid for table {self._TableName}.")

        if self.check(row=row):
            raise self.row_exist
        if row is None:
            raise self.row_error
        if not isinstance(_dict, dict) and _dict is not None:
            raise self.dict_error

        if _dict is not None and len(kwargs) != 0:
            dictionary = _dict
            second = kwargs
            dictionary.update(second)
        else:
            if _dict is not None:
                dictionary = _dict
            else:
                dictionary = kwargs

        self.set(row, dictionary)

    def find(self, *, row, column):
        pass

    def check(self, *, row):
        return self.check_db(value=row)

    def remove(self, *, row, column):
        pass

    def drop(self, *, row):
        pass

    def row_stringify(self):
        pass

    def fetch_rows(self):
        pass

    def __len__(self):
        pass

    def drop_table(self):
        pass

    def pickle_it(self):
        pass


db = dbTable(table_name="myTable", encrypt_key="hh")
# db.check()
db.insert(row=1, _dict={'ww': 1}, a=2, b=3)
