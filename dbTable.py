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
        #xx = self.db.dget("Metadata", "deleted_id")
        #xx.append("34:2")
        #self.db.dadd("Metadata", ("deleted_id", xx))

        #print(self.db.dgetall("Metadata"))
        #print(self.db.dgetall("d_tree"))

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
<<<<<<< HEAD
            if _dict is not None:
                dictionary = _dict
            else:
                dictionary = kwargs

        self.set(row, dictionary)
=======
            raise dbTableError(f"Column's data type is not supported. {type(column)}.")
        output_data = None
        check = False
        pass_id = _hash_(str(row))
        pre_check = False

        with open(self.__D_Tree_backup if _is_backup(file=self.__D_Tree) else self.__D_Tree, "rb") as r:
            try:
                while dictionary := pickle.load(r):
                    if dictionary.__contains__(pass_id):
                        dictionary = dictionary.__getitem__(pass_id)
                        value = dictionary.split(':')
                        check = True
                        break
            except EOFError:
                pass

        if check:
            data_path = os.path.join(f"{self.__db}", f"_D_{value[0]}.pickle")
            backup_data_path = os.path.join(f"{self.__db}", f"backup_D_{value[0]}.pickle")

            with open(backup_data_path if _is_backup(file=data_path) else data_path, "rb") as R:
                try:
                    while data_dict := pickle.load(R):
                        if data_dict.__contains__(pass_id):
                            data_dict = data_dict.__getitem__(pass_id)
                            pre_check = True
                            break
                except EOFError:
                    pass

            if pre_check:
                data_dict = eval(_decrypt(data_dict, self.default_key))
                if column is None:
                    output_data = data_dict[f"{str(row)}"]
                    check = True
                else:
                    if data_dict[f"{str(row)}"].__contains__(str(column)):
                        output_data = data_dict[f"{row}"].__getitem__(str(column))
                        check = True
                    else:
                        if call:
                            raise NotFound(f"Column {column} does not exist.")
                        else:
                            check = False

        if call is True and check is False:
            raise NotFound(f"Row {row} does not exist.")

        return output_data, check, (data_dict if check else None), (dictionary if check else None)

    def find(self, row=None, column=None):
        # find data cell
        return self.__find(row, column)[0]

    def row_stringify(self, row=None, indent=2, sort_keys=False):
        if self.check(row):
            return json.dumps(self.__find(row)[0], indent=indent, sort_keys=sort_keys)
        else:
            raise NotFound(f"Row {row} does not exist.")
>>>>>>> parent of 73e9da8... fix

    def find(self, *, row, column):
        pass

    def check(self, *, row):
        return self.check_db(value=row)

    def remove(self, *, row, column):
        pass

    def drop(self, *,  row):
        pass

    def row_stringify(self):
        pass

    def fetch_rows(self):
        pass

    def __len__(self):
        pass

    def tables(self):
<<<<<<< HEAD
        pass
=======
        # search through all XML files for table's names
        files = []
        # r=root, d=directories, f=files
        for r, d, f in os.walk(self.path):
            for file in f:
                if '.xml' in file:
                    files.append(os.path.join(r, file))
        table_names = []
        for path in files:
            table_names.append(_XML(path).access(path=True))
        return table_names


class Generate(_Location):

    def __init__(self, db_path=current_directory, db_name="_dbTables_",
                 table_name=None, encrypt_key=default_key, columns=None):
        super().__init__(db_path, db_name)

        self.default_key = encrypt_key
        self.db_name = db_name
        self.db_path = db_path
        self.__TableName = table_name
        self.__Column = (columns,) if isinstance(columns, str) else columns
        self.__Table_location = os.path.join(f"{self.path}", f"{_hash_(self.__TableName)}")
        self.__db = os.path.join(f"{self.__Table_location}", "_Database_")
        self.__MetaData = os.path.join(f"{self.__Table_location}", "_MetaData_")
        self.__XML_ts = os.path.join(f"{self.__MetaData}", f"T_state.xml")
        self.__D_Tree = os.path.join(f"{self.__MetaData}", f"D_Tree.pickle")
        self.__D_Tree_backup = os.path.join(f"{self.__MetaData}", "D_Tree_backup.pickle")
        self.__row_collections = os.path.join(f"{self.__MetaData}", f"rows.pickle")
        self.__row_collections_backup = os.path.join(f"{self.__MetaData}", f"rows_backup.pickle")
        self.__Track = os.path.join(f"{self.__MetaData}", "Track.pickle")
        self.__insert = _Insert(db_path=self.db_path, db_name=self.db_name, table_name=self.__TableName,
                                encrypt_key=self.default_key)

        if self.__Column is None:
            raise TypeError("Column should not be None, use the key argument columns= .")

        if self.__TableName is None:
            raise TypeError("Table without a name, use the key argument table_name= .")

        if not os.path.isdir(self.__Table_location):
            # make all files and folders for table's metadata and database
            os.mkdir(self.__Table_location)
            os.mkdir(self.__db)
            os.mkdir(self.__MetaData)

            with open(self.__Track, 'wb') as w:
                track_dict = {"last_entry": 0, "deleted_id": [], "tree_track": 0}
                pickle.dump(track_dict, w)
                os.chmod(self.__Track, stat.S_IREAD)
            with open(self.__D_Tree_backup, 'wb'):
                os.chmod(self.__D_Tree_backup, stat.S_IREAD)
            with open(self.__D_Tree, "wb"):
                os.chmod(self.__D_Tree, stat.S_IREAD)
            with open(self.__row_collections, "wb"):
                os.chmod(self.__row_collections, stat.S_IREAD)
            with open(self.__row_collections_backup, "wb"):
                os.chmod(self.__row_collections_backup, stat.S_IREAD)

            """writing XML file for table's info"""
            root = ET.Element("Meta-Data")
            tree = ET.ElementTree(root)
            tree.write(self.__XML_ts)
            tree = ET.parse(self.__XML_ts)
            root = tree.getroot()
            doc = ET.SubElement(root, "Table", attrib={"Name": f"{self.__TableName}"})
            ET.SubElement(doc, "columns").text = f"{self.__Column}"
            ET.SubElement(doc, "Name").text = f"{self.__TableName}"
            ET.SubElement(doc, "hashed_key").text = f"{_hash_(self.default_key)}"
            tree.write(self.__XML_ts)
            os.chmod(self.__XML_ts, stat.S_IREAD)

    def __check(self, row, column_):
        # check if data ,rows, columns exist in table, to prevent duplication.
        check = False
        data_dict = None
        pass_id = _hash_(str(row))
        value = []
        with open(self.__D_Tree_backup if _is_backup(file=self.__D_Tree) else self.__D_Tree, "rb") as r:
            try:
                while dictionary := pickle.load(r):
                    if dictionary.__contains__(pass_id):
                        dictionary = dictionary.__getitem__(pass_id)
                        value = dictionary.split(':')
                        check = True
                        break
            except EOFError:
                pass

        if check:
            data_path = os.path.join(f"{self.__db}", f"_D_{value[0]}.pickle")
            backup_data_path = os.path.join(f"{self.__db}", f"backup_D_{value[0]}.pickle")

            with open(backup_data_path if _is_backup(file=data_path) else data_path, "rb") as R:
                try:
                    while data_dict := pickle.load(R):
                        if data_dict.__contains__(_hash_(str(row))):
                            data_dict = data_dict.__getitem__(_hash_(str(row)))
                            db_check = True
                            break
                except EOFError:
                    db_check = False
                    check = False

            if db_check:
                # check if a column already exists
                data_dict = eval(_decrypt(data_dict, self.default_key))
                for col in column_:
                    if data_dict[f"{row}"].__contains__(str(col)):
                        raise dbTableError(
                            f"the cell is already available for column: {col} and row: {row} ")

        return check, (data_dict if check and db_check else None), (dictionary if check else None)

    def _db_(self, data, row, columns):
        # this method store data in _bd folder

        if (check_id_state := self.__check(row, columns))[0]:
            # update
            table_dict = check_id_state[1]
            [table_dict[f"{row}"].__setitem__(str(columns[i]), data[i]) for i in range(len(data))]
            self.__insert.insert(_data_=table_dict, row=row, call=True, path=check_id_state[2])
            # call=True means update or add to a row that already exists
        else:
            fb_state_ = _is_backup(file=self.__row_collections)
            _file_ = self.__row_collections_backup if fb_state_ else self.__row_collections
            os.chmod(_file_, stat.S_IWRITE)
            with open(_file_, "ab") as a:
                pickle.dump(_encrypt(str(row), self.default_key), a)
            os.chmod(_file_, stat.S_IREAD)

            table_dict = {f"{row}": dict()}
            [table_dict[f"{row}"].__setitem__(str(columns[i]), data[i]) for i in range(len(data))]

            # insert
            id_path = self.__insert.insert(_data_=table_dict, row=row)  # call=False (default) means a new row
            # insert the data dictionary in db files and return its path
            with open(self.__Track, 'rb') as r:
                track_dict = pickle.load(r)

            counter = track_dict["tree_track"]
            if counter == 1000:
                # Table limit (1k rows) for speed and efficiency
                raise dbTableError("you have reached table limit of one Thousand rows.")

            self.__insert.track(key="tree_track")
            # D-Tree data scheme {"hashed row": path}
            (tree_dict := dict()).__setitem__(_hash_(str(row)), id_path) # ===========

            fb_state = _is_backup(file=self.__D_Tree)

            file = self.__D_Tree_backup if fb_state else self.__D_Tree
            os.chmod(file, stat.S_IWRITE)
            with open(file, "ab") as a:
                pickle.dump(tree_dict, a)
            os.chmod(file, stat.S_IREAD)

    def insert(self, data, row=None, columns=None):
        try:
            # check Table State
            table_state = _XML(self.__XML_ts).access()
            ts_column = table_state[0]

            if table_state[1] != _hash_(self.default_key):
                # check is the encryption key is valid
                raise dbTableError(f"Access denied, the encryption key {self.default_key} "
                                   f"is not valid for table {self.__TableName}.")
        except Exception:
            raise dbTableError(f"No such Table {self.__TableName}")

        # Converting the input

        columns = (columns,) if isinstance(columns, str) else (str(columns),) if isinstance(columns, int) else columns
        row = str(row) if isinstance(row, int) else row
        data = data if isinstance(data, tuple) else (data,)
>>>>>>> parent of 73e9da8... fix

    def drop_table(self):
        pass

    def pickle_it(self):
        pass


db = dbTable(table_name="myTable", encrypt_key="hh")
#db.check()
db.insert(row=1, _dict={'ww': 1}, a=2, b=3)
