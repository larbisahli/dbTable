
__Author__ = "Larbi Sahli"
__Copyright__ = "Copyright \xa9 2019 Larbi Sahli => https://github.com/larbisahli"
__License__ = "Public Domain"
__Version__ = "1.6.0"
__Table_Rows_Limit__ = 1000
__all__ = ["Extract", "Generate"]

import shutil
import hashlib
import pickle
import os
import stat
import json
import onetimepad
import xml.etree.cElementTree as ET

"""
dbTable is a lightweight and efficient SQL database file management that can store encrypted data in Tables 
using rows and columns.
With dbTable you can store data in files and manage it, each Table can store up to one Thousand Rows
for speed and efficiency.
The data inside files is stored using pickle and encrypted using "onetimepad",
you can use your own key to encrypt and decrypt data for security purposes.

======= Generate =======

from dbTable import Generate

Generate class => creates Table metadata and inserts data in database.

== Making a Table ==

>> conn = Generate(db_path="C:\\Users\\User\\folder", db_name="my_database", table_name="my_table", encrypt_key=key,
   columns=("column-1", "column-2", etc))

> db_path=""               # Database directory path, the default path is the current directory of dbTable.
> db_name=""               # Database name, the default database name is _dbTables_
> table_name=""            # Table name.
> encrypt_key="my_key"     # encryption key, default key is default_key variable in the program.
> columns=""               # Columns (declare all the columns to create the table).

== inserting data in database ==

>> conn.insert(data=("data-1", "data-2", etc), row="row-1", columns=(column-1, column-2, etc))


> row=""      # you can enter one row at time.
> columns=""  # you can enter multiple columns at once in a tuple.
> data=""     # you can enter multiple data at once in a tuple.

warning: you need to enter the data in the same order as columns  
data=(data-1, data-2, etc) => columns=(column-1, column-2, etc) see bellow

-----------------------------------
      | column-1 |  column-2 | etc
-----------------------------------
row-1 | data-1   |  data-2   | etc


========= Extract ==========

from dbTable import Extract

Extract class =>  find data, check data, update data, fetchall rows, fetchall columns, remove data cell,
                  remove rows, len(table rows), drop table, row_stringify json.

>> conn = Extract(db_path="C:\\Users\\User\\folder", db_name=""my_database"", table_name="my_table",
               decrypt_key=key)
               
>> conn.find(row="row-1", column="column-1")   # a method that can find data cell in row-1 and column-1,
                                                 using conn.find(row="row-1") will give you dictionary of all 
                                                 the columns and the data in row-1.

>> conn.check(row="row-1",column="column-1")    # a method that will return True if data exists False if not,
                                                  you can also use conn.check(row="row-1") to check if a row exist.
                                   
>> conn.update(row="row-1", column="column-1",  # a method that can update data cell in row-1 and column-1.
               data="data-2") 

>> conn.remove(row="row-1", column="column-1")  # a method that can remove a data in cell in row-1 and column-1.

>> conn.drop_row(row="row-1")                   # a method that can remove rows.

>> conn.fetchall_columns                        # a property that will return a list of all the table's columns.

>> conn.fetchall_rows                           # a property that will return a list of all the table's rows.

>> conn.row_stringify(row="row-1", indent=2,    # using json.dumps on a dictionary of all columns and cells in a row.
                      sort_keys=False):  

>> len(conn)                                    # returns how many rows inside a table.

>> conn.tables                                  # a property that will return a list of all tables that were created
                                                  in database.

>> conn.drop_table                              # a property that will remove the whole table from database.

"""

current_directory = os.path.dirname(os.path.realpath(__file__))
default_key = "37e1f5637615c8ab9474184a2970fdcb8814303f49e56deb6c31cb7a1c12655c"


def _hash_(dir_):
    return hashlib.sha256(dir_.encode()).hexdigest()[:40]


def _encrypt(data, key_):
    return onetimepad.encrypt(data, key_)


def _decrypt(encrypted_data, key_):
    return onetimepad.decrypt(encrypted_data, key_)


class dbTableError(Exception):
    pass


class NotFound(dbTableError):
    pass


class KeyError(dbTableError):
    pass


class _Location:
    def __init__(self, db_path, db_name):
        self.path = os.path.join(f"{db_path}", f"{db_name}")
        if not os.path.isdir(self.path):
            os.mkdir(self.path)


class _XML:
    # Storing table's state in XML
    def __init__(self, xml_ts):
        self.xml_ts = xml_ts

    def access(self, *, path=False):
        if not path:
            col = (tree := ET.parse(self.xml_ts)).find("Table/columns").text
            hashed_key = tree.find("Table/hashed_key").text
            return eval(col), hashed_key
        else:
            return ET.parse(self.xml_ts).find("Table/Name").text


def _is_backup(*, file):
    """ Check which of the files is available, file or its backup """
    with open(file, 'rb') as r:
        try:
            pickle.load(r)
            return False
        except EOFError:
            return True


def _F_B_switch(*, file, backup, change=None):
    """
    Instead of reading the whole file into the memory and write it again
    to make a small change or an update, it is a better practice to toggle between
    a file and its backup if we use pickle.
    """

    fb_state = _is_backup(file=file)
    # if fb_state is False write file to backup, else write backup to file

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
    # clear the file
    with open(read, 'wb'):
        os.chmod(read, stat.S_IREAD)


class _Insert(_Location):
    """ initializing data (using pickle) to be stored in files """

    def __init__(self, db_path=current_directory, db_name="_dbTables_", encrypt_key=default_key, table_name=None):
        super().__init__(db_path, db_name)

        self.Key = encrypt_key
        self.__db_name = db_name
        self.__TableName = table_name
        self.__Table_location = os.path.join(f"{self.path}", f"{_hash_(self.__TableName)}")
        self.__db = os.path.join(f"{self.__Table_location}", f"_Database_")
        self.__MetaData = os.path.join(f"{self.__Table_location}", f"_MetaData_")
        self.__Track = os.path.join(f"{self.__MetaData}", "Track.pickle")

    def delete(self, path, row):

        value = path.split(':')
        data_path = os.path.join(f"{self.__db}", f"_D_{value[0]}.pickle")
        backup_data_path = os.path.join(f"{self.__db}", f"backup_D_{value[0]}.pickle")
        _F_B_switch(file=data_path, backup=backup_data_path, change=_hash_(str(row)))
        self.track(key="deleted_id", data=path)

    def track(self, key, data=None, remove=False, sa=True):

        with open(self.__Track, 'rb') as r:
            track_dict = pickle.load(r)
        if key == "deleted_id":
            if remove:
                data_ = track_dict[key]
                data_.remove(data)
            else:
                data_ = track_dict[key]
                data_.append(data)
        elif key == "tree_track":  # track the D_tree rows, limit is 1k.
            if sa:  # add
                data_ = int(track_dict[key]) + 1
            else:  # subtract
                data_ = int(track_dict[key]) - 1
        else:
            data_ = data

        track_dict.__setitem__(key, data_)
        os.chmod(self.__Track, stat.S_IWRITE)
        with open(self.__Track, 'wb') as a:
            pickle.dump(track_dict, a)
        os.chmod(self.__Track, stat.S_IREAD)

    def insert(self, row, _data_, update=False, path=None):
        if not update:
            with open(self.__Track, 'rb') as r:
                track_dict = pickle.load(r)

            d_state = True
            if len(d_id := track_dict["deleted_id"]) != 0:
                value = d_id[0].split(':')
                counter = int(value[1]); d = int(value[0])
                d_state = False
            elif (s_id := track_dict["last_entry"]) != 0:
                value = s_id.split(':')
                counter = int(value[1]); d = int(value[0])
            else:
                d = 1; counter = 0; value = []

            path = f"{d}:{counter}:"  # in case fb_state is not None
            if d_state:

                counter += 1
                path = f"{d}:{counter}:"
                self.track(key="last_entry", data=path)

                if counter == 101:  # 100 rows limit in each _D_ file
                    d += 1
                    path = f"{d}:{1}:"
                    self.track(key="last_entry", data=path)

                if d > 10:  # 10 _D_ files in Table folder => 1000 rows
                    raise dbTableError(f"You have reached Table limit of 'one thousand rows' in {self.__db_name}.\n"
                                       "Try to create another Table.")

            if not d_state:
                self.track(key="deleted_id", data=":".join(value), remove=True)
        else:
            d, _, _ = path.split(":")

        file = os.path.join(f"{self.__db}", f"_D_{d}.pickle")
        file_backup = os.path.join(f"{self.__db}", f"backup_D_{d}.pickle")

        (data_dict := dict()).__setitem__(_hash_(str(row)), _encrypt(str(_data_), self.Key))

        if not os.path.isfile(file):
            with open(file, 'wb'):
                os.chmod(file, stat.S_IREAD)
            with open(file_backup, 'wb'):
                os.chmod(file_backup, stat.S_IREAD)

        if update:
            _F_B_switch(file=file,
                        backup=file_backup,
                        change=_hash_(str(row)))

        _file_ = file_backup if _is_backup(file=file) else file

        os.chmod(_file_, stat.S_IWRITE)
        with open(_file_, "ab") as a:
            pickle.dump(data_dict, a)
        os.chmod(_file_, stat.S_IREAD)

        return path


class Extract(_Location):

    def __init__(self, db_path=current_directory, db_name="_dbTables_", table_name=None, decrypt_key=default_key):
        super().__init__(db_path, db_name)

        self.default_key = decrypt_key
        self.__TableName = table_name
        self.__db_path = db_path
        self.__db_name = db_name
        self.__Table_location = os.path.join(f"{self.path}", f"{_hash_(self.__TableName)}")
        self.__db = os.path.join(f"{self.__Table_location}", f"_Database_")
        self.__MetaData = os.path.join(f"{self.__Table_location}", f"_MetaData_")
        self.__XML_ts = os.path.join(f"{self.__MetaData}", "T_state.xml")
        self.__D_Tree = os.path.join(f"{self.__MetaData}", "D_Tree.pickle")
        self.__D_Tree_backup = os.path.join(f"{self.__MetaData}", "D_Tree_backup.pickle")
        self.__row_collections = os.path.join(f"{self.__MetaData}", "rows.pickle")
        self.__row_collections_backup = os.path.join(f"{self.__MetaData}", f"rows_backup.pickle")
        self.__Track = os.path.join(f"{self.__MetaData}", "Track.pickle")
        self.__insert = _Insert(db_path=self.__db_path, db_name=self.__db_name, table_name=self.__TableName,
                                encrypt_key=self.default_key)

        try:
            self.__ts_column = _XML(self.__XML_ts).access()[0]
            self.__hashed_key = _XML(self.__XML_ts).access()[1]
        except Exception:
            raise dbTableError(f"No such Table {self.__TableName}")

        if self.__hashed_key != _hash_(self.default_key):
            raise KeyError(f"Access denied, the decryption key {self.default_key} "
                           f"is not the table {self.__TableName}'s decryption key.")

    def __find(self, row=None, column=None, call=True):
        # search for data in database
        if row is None:
            raise dbTableError("Row should not be empty, use the key argument row=.")

        if column not in self.__ts_column and column is not None:
            raise dbTableError(f"{column} does not exist in table {self.__TableName}'s columns"
                               f"\n The available columns:  {self.__ts_column}")

        if isinstance(column, str) or isinstance(column, float) or isinstance(column, int) or column is None:
            column = column if column is None else str(column)
        else:
            raise dbTableError(f"Column's data type is not supported. {type(column)}.")
        output_data = None
        value = []
        pass_id = _hash_(str(row))
        data_dict = False

        with open(self.__D_Tree_backup if _is_backup(file=self.__D_Tree) else self.__D_Tree, "rb") as r:
            try:
                while dictionary := pickle.load(r):
                    if dictionary.__contains__(pass_id):
                        dictionary = dictionary.__getitem__(pass_id)
                        value = dictionary.split(':')
                        break
            except EOFError:
                pass

        if value:
            data_path = os.path.join(f"{self.__db}", f"_D_{value[0]}.pickle")
            backup_data_path = os.path.join(f"{self.__db}", f"backup_D_{value[0]}.pickle")

            with open(backup_data_path if _is_backup(file=data_path) else data_path, "rb") as R:
                try:
                    while data_dict := pickle.load(R):
                        if data_dict.__contains__(pass_id):
                            data_dict = data_dict.__getitem__(pass_id)
                            break
                except EOFError:
                    pass

            if data_dict:
                data_dict = eval(_decrypt(data_dict, self.default_key))
                if column is None:
                    output_data = data_dict[f"{str(row)}"]
                else:
                    if data_dict[f"{str(row)}"].__contains__(str(column)):
                        output_data = data_dict[f"{row}"].__getitem__(str(column))
                    else:
                        if call:
                            raise NotFound(f"Column {column} does not exist.")
                        else:
                            value = False

        if call is True and value is False:
            raise NotFound(f"Row {row} does not exist.")

        return output_data, bool(value), (data_dict if value else None), (dictionary if value else None)

    def find(self, row=None, column=None):
        # find data cell
        return self.__find(row, column)[0]

    def row_stringify(self, row=None, indent=2, sort_keys=False):
        if self.check(row):
            return json.dumps(self.__find(row)[0], indent=indent, sort_keys=sort_keys)
        else:
            raise NotFound(f"Row {row} does not exist.")

    def check(self, row, column=None):
        # check is data exist
        return self.__find(row, column, call=False)[1]

    def remove(self, row=None, column=None):
        # remove column's data cell
        if column is None:
            raise TypeError("Column should not be empty, use the key argument column= .")

        if (check_row_state := self.__find(row, column))[1]:
            col_dict = check_row_state[2]
            col_dict[str(row)].__delitem__(str(column))
            self.__insert.insert(_data_=col_dict, row=row, update=True, path=check_row_state[3])
        else:
            raise NotFound(f"Row {row} or the column {column} does not exist.")

    def __len__(self):
        # return the number of rows inside a table
        with open(self.__Track, 'rb') as r:
            try:
                counter = int(pickle.load(r)["tree_track"])
            except EOFError:
                counter = 0
        return counter

    @property
    def fetchall_rows(self):
        # returns all table's rows in row_collections file
        fb_state = _is_backup(file=self.__row_collections)
        rows = []
        with open(self.__row_collections_backup if fb_state else self.__row_collections, "rb") as read:
            while True:
                try:
                    rows.append(_decrypt(pickle.load(read), self.default_key))
                except EOFError:
                    break
        return rows

    @property
    def fetchall_columns(self):
        # returns all columns of a table from XML file
        return [col for col in self.__ts_column]

    def update(self, data=None, row=None, column=None):
        # updating data cell using row and columns
        if data is None:
            raise TypeError("Data should not be empty, use the key argument data= .")

        if column is None:
            raise TypeError("Column should not be empty, use the key argument column= .")

        if (check_row_state := self.__find(row, column))[1]:
            col_dict = check_row_state[2]
            col_dict[str(row)].__setitem__(str(column), data)
            self.__insert.insert(_data_=col_dict, row=row, update=True, path=check_row_state[3])
        else:
            raise NotFound(f"Row {row} or the column {column} does not exist.")

    def drop_row(self, row=None):
        # removing rows
        check = False
        pass_row = _hash_(str(row))
        with open(self.__D_Tree_backup if _is_backup(file=self.__D_Tree) else self.__D_Tree, "rb") as read:
            try:
                while dictionary := pickle.load(read):
                    if dictionary.__contains__(pass_row):
                        path_id = dictionary.__getitem__(pass_row)
                        check = True
                        break
            except EOFError:
                pass

        if check:
            # remove the row from D_tree file
            _F_B_switch(file=self.__D_Tree,
                        backup=self.__D_Tree_backup,
                        change=pass_row)

            # remove the row from database
            self.__insert.delete(path_id, row=row)

            self.__insert.track(key="tree_track", sa=False)

            # remove the row from row_collections file
            _F_B_switch(file=self.__row_collections,
                        backup=self.__row_collections_backup,
                        change=_encrypt(str(row), self.default_key))
        else:
            raise NotFound(f"Row {row} does not exist.")

    def drop_table(self):
        # remove all files and folders that makes a table
        for _, d, _ in os.walk(path := self.__Table_location):
            for folder in d:
                for _, _, f in os.walk(folder_ := os.path.join(f"{path}", f"{folder}")):
                    for file in f:
                        file_ = os.path.join(f"{folder_}", f"{file}")
                        os.chmod(file_, stat.S_IWRITE)
                        os.remove(file_)
        try:
            shutil.rmtree(path)
        except OSError:
            pass

    @property
    def tables(self):
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
        data_dict = None
        pass_id = _hash_(str(row))
        value = []
        with open(self.__D_Tree_backup if _is_backup(file=self.__D_Tree) else self.__D_Tree, "rb") as r:
            try:
                while dictionary := pickle.load(r):
                    if dictionary.__contains__(pass_id):
                        dictionary = dictionary.__getitem__(pass_id)
                        value = dictionary.split(':')
                        break
            except EOFError:
                pass

        if value:
            data_path = os.path.join(f"{self.__db}", f"_D_{value[0]}.pickle")
            backup_data_path = os.path.join(f"{self.__db}", f"backup_D_{value[0]}.pickle")

            with open(backup_data_path if _is_backup(file=data_path) else data_path, "rb") as R:
                try:
                    while data_dict := pickle.load(R):
                        if data_dict.__contains__(_hash_(str(row))):
                            data_dict = data_dict.__getitem__(_hash_(str(row)))
                            break
                except EOFError:
                    data_dict = None
                    value = []

            if data_dict is not None:
                # check if a column already exists
                data_dict = eval(_decrypt(data_dict, self.default_key))
                for col in column_:
                    if data_dict[f"{row}"].__contains__(str(col)):
                        raise dbTableError(
                            f"the cell is already available for column: {col} and row: {row} ")

        return bool(value), data_dict, (dictionary if value else None)

    def _db_(self, data, row, columns):
        # this method store data in _bd folder

        if (check_id_state := self.__check(row, columns))[0]:
            # update
            table_dict = check_id_state[1]
            [table_dict[f"{row}"].__setitem__(str(columns[i]), data[i]) for i in range(len(data))]
            self.__insert.insert(_data_=table_dict, row=row, update=True, path=check_id_state[2])
            # update=True means update or add to a row that already exists
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
            id_path = self.__insert.insert(_data_=table_dict, row=row)  # update=False (default) means a new row
            # insert the data dictionary in db files and return its path
            with open(self.__Track, 'rb') as r:
                track_dict = pickle.load(r)

            counter = track_dict["tree_track"]
            if counter == 1000:
                # Table limit (1k rows) for speed and efficiency
                raise dbTableError("you have reached table limit of one Thousand rows.")

            self.__insert.track(key="tree_track")
            # D-Tree data scheme {"hashed row": path}
            (tree_dict := dict()).__setitem__(_hash_(str(row)), id_path)

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

        # row and column's data type handling.

        if columns is None:
            raise TypeError("Column should not be empty, use the key argument columns= .")

        if row is None:
            raise TypeError("Row should not be empty, use the key argument row= .")

        # inspect row's data type.

        if not isinstance(row, str):
            raise TypeError("Row must be str or int.")

        if isinstance(data, tuple):
            # check if len(data) == len(column), if both are tuple.
            if len(data) != len(columns):
                raise TypeError(f"Not enough arguments in data variable (expected {len(columns)} got {len(data)})")
        else:
            # if string or int.
            if 1 != len(columns):
                raise TypeError(f"Not enough arguments in data variable (expected {len(columns)} got 1)")

        index_tracker = [i for i in range(len(columns) if isinstance(columns, tuple) else 1) if
                         (columns[i] if isinstance(columns, tuple) else (columns,)) not in ts_column]

        if len(index_tracker) != 0:
            """ index_tracker gives a list of the column's index that does not exist in table's columns (ts_column)"""

            non_existed_columns = [columns[i] for i in index_tracker]
            raise dbTableError(f"{non_existed_columns} does not exist in table {self.__TableName}'s columns"
                               f"\n available columns:  {ts_column}")

        self._db_(data, row, columns)
