#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sqlite3
import time
import sys
from itertools import zip_longest
import numpy
from cleep.core import CleepModule
from cleep.exception import CommandError, MissingParameter, InvalidParameter

__all__ = ["Charts"]


class Charts(CleepModule):
    """
    Module that provides data storage in local database for charts generation
    Also unlock sensors and system charts
    """

    MODULE_AUTHOR = "Cleep"
    MODULE_VERSION = "1.1.3"
    MODULE_DEPS = []
    MODULE_DESCRIPTION = "Follow easily your sensors values graphically."
    MODULE_LONGDESCRIPTION = (
        "Charts application automatically generates charts according to your connected sensors values.<br>"
        "It allows you to follow in time the evolution of the measurements of your sensors"
    )
    MODULE_CATEGORY = "APPLICATION"
    MODULE_TAGS = ["sensors", "graphs", "charts", "database"]
    MODULE_URLINFO = "https://github.com/CleepDevice/cleepmod-charts"
    MODULE_URLHELP = "https://github.com/CleepDevice/cleepmod-charts/wiki"
    MODULE_URLSITE = None
    MODULE_URLBUGS = "https://github.com/CleepDevice/cleepmod-charts/issues"

    MODULE_CONFIG_FILE = "charts.conf"

    DATABASE_PATH = "/etc/cleep/charts"
    DATABASE_NAME = "charts.db"
    # for tests only set to False, to avoid exception during session closing
    CHECK_SAME_THREAD = True
    MAX_DATA_SIZE = 1000000  # in bytes

    def __init__(self, bootstrap, debug_enabled):
        """
        Constructor

        Args:
            bootstrap (dict): bootstrap objects
            debug_enabled (bool): flag to set debug level to logger
        """
        # init
        CleepModule.__init__(self, bootstrap, debug_enabled)

        # member
        self._cnx = None
        self._cur = None

        # make sure database path exists
        if not os.path.exists(Charts.DATABASE_PATH):  # pragma: no cover
            self.cleep_filesystem.mkdir(Charts.DATABASE_PATH, True)

    def _configure(self):
        """
        Configure module
        """
        # make sure database file exists
        database_path = os.path.join(Charts.DATABASE_PATH, Charts.DATABASE_NAME)
        if not os.path.exists(database_path):
            self.logger.debug("Database file not found")
            self._init_database()

        self.logger.debug('Connect to database "%s"', database_path)
        self._cnx = sqlite3.connect(
            database_path, check_same_thread=Charts.CHECK_SAME_THREAD
        )
        self._cur = self._cnx.cursor()

    def _on_stop(self):
        """
        Stop module
        """
        if self._cnx:
            self._cnx.close()

    def _init_database(self):
        """
        Init database
        """
        path = os.path.join(Charts.DATABASE_PATH, Charts.DATABASE_NAME)
        self.logger.debug('Initialize database "%s"', path)

        # create database file
        cnx = sqlite3.connect(path)
        cur = cnx.cursor()

        # create devices table (handle number of values associated to device)
        # format:
        #  - uuid: store device uuid (string) (primary key)
        #  - event: event type stored for the device (string)
        #  - valuescount: number of values saved for the device
        #  - value1: field name for value1
        #  - value2: field name for value2
        #  - value3: field name for value3
        #  - value4: field name for value4
        cur.execute(
            (
                "CREATE TABLE devices("
                "uuid TEXT PRIMARY KEY UNIQUE, "
                "event TEXT, "
                "valuescount INTEGER, "
                "value1 NUMBER DEFAULT NULL, "
                "value2 TEXT DEFAULT NULL, "
                "value3 TEXT DEFAULT NULL, "
                "value4 TEXT DEFAULT NULL);"
            )
        )

        # create data1 table (contains 1 field to store value, typically light/humidity... sensors)
        # format:
        #  - id: unique id (primary key)
        #  - timestamp: timestamp when value was inserted
        #  - uuid: device uuid that pushes values
        #  - value, value1, value2, value3, value4: values of device
        cur.execute(
            "CREATE TABLE data1(id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, timestamp INTEGER, uuid TEXT, value1 NUMBER);"
        )
        cur.execute("CREATE INDEX data1_device_index ON data1(uuid);")
        cur.execute("CREATE INDEX data1_timestamp_index ON data1(timestamp);")

        # create data2 table (contains 2 fields to store values, typically gps positions, temperature (C° and F°))
        cur.execute(
            (
                "CREATE TABLE data2("
                "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, "
                "timestamp INTEGER, "
                "uuid TEXT, "
                "value1 NUMBER, "
                "value2 NUMBER);"
            )
        )
        cur.execute("CREATE INDEX data2_device_index ON data2(uuid);")
        cur.execute("CREATE INDEX data2_timestamp_index ON data2(timestamp);")

        # create data3 table (contains 3 fields to store values)
        cur.execute(
            (
                "CREATE TABLE data3("
                "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, "
                "timestamp INTEGER, "
                "uuid TEXT, "
                "value1 NUMBER, "
                "value2 NUMBER, "
                "value3 NUMBER);"
            )
        )
        cur.execute("CREATE INDEX data3_device_index ON data3(uuid);")
        cur.execute("CREATE INDEX data3_timestamp_index ON data3(timestamp);")

        # create data4 table (contains 4 fields to store values)
        cur.execute(
            (
                "CREATE TABLE data4("
                "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, "
                "timestamp INTEGER, "
                "uuid TEXT, "
                "value1 NUMBER, "
                "value2 NUMBER, "
                "value3 NUMBER, "
                "value4 NUMBER);"
            )
        )
        cur.execute("CREATE INDEX data4_device_index ON data4(uuid);")
        cur.execute("CREATE INDEX data4_timestamp_index ON data4(timestamp);")

        cnx.commit()
        cnx.close()

    def __restore_field_name(self, current_field, fields):
        """
        Restore field name as stored in database

        Args:
            current_field (string): current field to replace
            fields (string): fields mapping (dict)

        Returns:
            string: field name
        """
        try:
            if current_field == "timestamp":
                # return reduced string of timestamp
                return "ts"
            return fields[current_field]
        except Exception:  # pragma: no cover
            # field name not found
            return current_field

    def _save_data(self, device_uuid, event, values):
        """
        Save data into database

        Args:
            device_uuid (string): device uuid
            event (string): event name
            values (list): values to save (must be an list of dict(<field>,<value>))

        Raises:
            InvalidParameter: if invalid parameter is specified
        """
        self.logger.debug(
            "Set_data device_uuid=%s event=%s values=%s",
            device_uuid,
            event,
            str(values),
        )
        if device_uuid is None or len(device_uuid) == 0:
            raise MissingParameter('Parameter "device_uuid" is missing')
        if event is None or len(event) == 0:
            raise MissingParameter('Parameter "event" is missing')
        if values is None:
            raise MissingParameter('Parameter "values" is missing')
        if not isinstance(values, list):
            raise InvalidParameter('Parameter "values" must be a list')
        if len(values) == 0:
            raise InvalidParameter("No value to save")
        if len(values) > 4:
            raise InvalidParameter(
                f'Too many values to save for event "{event}". It is limited to 4 values for now: {values}'
            )

        def get_value(value):
            """
            Prevent bool value to be stored (replace it with 1 or 0)
            """
            if not isinstance(value, bool):
                return value
            return 1 if value is True else 0

        # save device_uuid infos at first insert
        self._cur.execute("SELECT * FROM devices WHERE uuid=?", (device_uuid,))
        row = self._cur.fetchone()
        if row is None:
            # no infos yet, insert new entry for this device
            if len(values) == 1:
                self._cur.execute(
                    "INSERT INTO devices(uuid, event, valuescount, value1) VALUES(?,?,?,?)",
                    (device_uuid, event, len(values), values[0]["field"]),
                )
            elif len(values) == 2:
                self._cur.execute(
                    "INSERT INTO devices(uuid, event, valuescount, value1, value2) VALUES(?,?,?,?,?)",
                    (
                        device_uuid,
                        event,
                        len(values),
                        values[0]["field"],
                        values[1]["field"],
                    ),
                )
            elif len(values) == 3:
                self._cur.execute(
                    "INSERT INTO devices(uuid, event, valuescount, value1, value2, value3) VALUES(?,?,?,?,?,?)",
                    (
                        device_uuid,
                        event,
                        len(values),
                        values[0]["field"],
                        values[1]["field"],
                        values[2]["field"],
                    ),
                )
            elif len(values) == 4:
                self._cur.execute(
                    "INSERT INTO devices(uuid, event, valuescount, value1, value2, value3, value4) VALUES(?,?,?,?,?,?,?)",
                    (
                        device_uuid,
                        event,
                        len(values),
                        values[0]["field"],
                        values[1]["field"],
                        values[2]["field"],
                        values[3]["field"],
                    ),
                )
        else:
            # entry exists, check it
            infos = dict(
                (self._cur.description[i][0], value) for i, value in enumerate(row)
            )
            if infos["event"] != event:
                raise CommandError(
                    f"Device {device_uuid} cannot store values from event {event} (stored for event {infos['event']})"
                )
            if infos["valuescount"] != len(values):
                raise CommandError(
                    f"Event {event} is supposed to store {infos['valuescount']} values not {len(values)}"
                )

        # save values
        if len(values) == 1:
            self._cur.execute(
                "INSERT INTO data1(timestamp, uuid, value1) values(?,?,?)",
                (int(time.time()), device_uuid, get_value(values[0]["value"])),
            )
        elif len(values) == 2:
            self._cur.execute(
                "INSERT INTO data2(timestamp, uuid, value1, value2) values(?,?,?,?)",
                (
                    int(time.time()),
                    device_uuid,
                    get_value(values[0]["value"]),
                    get_value(values[1]["value"]),
                ),
            )
        elif len(values) == 3:
            self._cur.execute(
                "INSERT INTO data3(timestamp, uuid, value1, value2, value3) values(?,?,?,?,?)",
                (
                    int(time.time()),
                    device_uuid,
                    get_value(values[0]["value"]),
                    get_value(values[1]["value"]),
                    get_value(values[2]["value"]),
                ),
            )
        elif len(values) == 4:
            self._cur.execute(
                "INSERT INTO data4(timestamp, uuid, value1, value2, value3, value4) values(?,?,?,?,?,?)",
                (
                    int(time.time()),
                    device_uuid,
                    get_value(values[0]["value"]),
                    get_value(values[1]["value"]),
                    get_value(values[2]["value"]),
                    get_value(values[3]["value"]),
                ),
            )

        # commit changes
        self._cnx.commit()

        return True

    def __get_device_infos(self, device_uuid):
        """
        Return device infos (read from "devices" table)

        Args:
            device_uuid (string): device uuid

        Returns:
            dict: list of devices table fields::

                {
                    event (string): event name associated to device
                    valuescount (int): number of values saved for this device (used to get data table)
                    value1 (string): value1 field name
                    value2 (string): value2 field name. Can be None
                    value3 (string): value3 field name. Can be None
                    value4 (string): value4 field name. Can be None
                }

        """
        self._cur.execute(
            "SELECT event, valuescount, value1, value2, value3, value4 FROM devices WHERE uuid=?",
            (device_uuid,),
        )
        row = self._cur.fetchone()
        if row is None:  # pragma: no cover
            raise CommandError(f"Device {device_uuid} not found!")
        return dict((self._cur.description[i][0], value) for i, value in enumerate(row))

    def _average_data(self, data, column_size):
        """
        Average data

        Args:
            data (list): list of values
            column_size (int): number of columns
        """
        # compute reduce factor according to variable memory size
        factor = int(round(sys.getsizeof(data) / self.MAX_DATA_SIZE))
        if factor <= 0:
            # no average needed, return specified data
            self.logger.debug("No data average computation needed")
            return data

        # group and average data
        args = [iter(data)] * factor
        # cast to int ? return [numpy.nanmean(v, axis=0).astype(int).tolist() for v in ...
        return [
            numpy.nanmean(v, axis=0).tolist()
            for v in list(zip_longest(*args, fillvalue=[numpy.nan] * column_size))
        ]

    def get_data(self, device_uuid, timestamp_start, timestamp_end, options=None):
        """
        Return data from data table

        Args:
            device_uuid (string): device uuid
            timestamp_start (int): start of range
            timestamp_end (int): end of range
            options (dict): command options::

                {
                    output (string): output format ('list'|'dict'[default])
                    fields (list): list of fields to return
                    sort (string): sort value ('asc'[default]|'desc')
                    limit (int): limit number
                    average (bool): return average data instead of all ones (default True).
                                    Can't work if data other than numbers are stored.
                }

        Returns:
            dict: data::

                {
                    uuid (string): device uuid
                    event (string): event name
                    names (list): list of column names
                    data (list|dict): content can be a list or a dict according to options.output value
                }

        Raises:
            InvalidParameter: if invalid parameter is specified
            MissingParameter: if parameter is missing
        """
        # check parameters
        if device_uuid is None or len(device_uuid) == 0:
            raise MissingParameter('Parameter "device_uuid" is missing')
        if timestamp_start is None:
            raise MissingParameter('Parameter "timestamp_start" is missing')
        if timestamp_start < 0:
            raise InvalidParameter("Timestamp_start value must be positive")
        if timestamp_end is None:
            raise MissingParameter('Parameter "timestamp_end" is missing')
        if timestamp_end < 0:
            raise InvalidParameter("Timestamp_end value must be positive")

        # prepare options
        options_fields = []
        options_output = "dict"
        options_sort = "asc"
        options_limit = ""
        options_average = True
        if options is not None:
            if "fields" in options:
                options_fields = options["fields"]
            if "output" in options and options["output"] in ("list", "dict"):
                options_output = options["output"]
            if "sort" in options and options["sort"] in ("asc", "desc"):
                options_sort = options["sort"]
            if "limit" in options and isinstance(options["limit"], int):
                options_limit = f"LIMIT {options['limit']}"
            if "average" in options and isinstance(options["average"], bool):
                options_average = options["average"]
        self.logger.trace(
            "options: fields=%s output=%s sort=%s limit=%s average=%s",
            options_fields,
            options_output,
            options_sort,
            options_limit,
            options_average,
        )

        # get device infos
        infos = self.__get_device_infos(device_uuid)
        self.logger.trace("infos=%s", infos)

        # prepare query options
        columns = []
        names = ["timestamp"]
        if len(options_fields) == 0:
            # no field filtered, add all existing fields
            columns.append("value1")
            names.append(infos["value1"])
            if infos["value2"] is not None:
                columns.append("value2")
                names.append(infos["value2"])
            if infos["value3"] is not None:
                columns.append("value3")
                names.append(infos["value3"])
            if infos["value4"] is not None:
                columns.append("value4")
                names.append(infos["value4"])
        else:
            # get column associated to field name
            for options_field in options_fields:
                for (key, value) in infos.items():
                    if key.startswith("value") and value == options_field:
                        columns.append(key)
                        names.append(options_field)

        # get device data for each request columns
        data = None
        if options_output == "dict":
            # output as dict
            columns_str = ",".join(["timestamp"] + columns)
            table_str = f"data{infos['valuescount']}"
            query = f"SELECT {columns_str} FROM {table_str} WHERE uuid=? AND timestamp>=? AND timestamp<=? ORDER BY timestamp {options_sort} {options_limit}"
            self.logger.debug("Select query: %s", query)
            self._cur.execute(query, (device_uuid, timestamp_start, timestamp_end))
            # @see http://stackoverflow.com/a/3287775
            results = self._cur.fetchall()
            values = (
                self._average_data(results, len(columns))
                if options_average
                else results
            )
            data = [
                dict(
                    (
                        self.__restore_field_name(self._cur.description[i][0], infos),
                        value,
                    )
                    for i, value in enumerate(row)
                )
                for row in values
            ]

        else:
            # output as list
            data = {}
            for column in columns:
                columns_str = ",".join(["timestamp"] + columns)
                table_str = f"data{infos['valuescount']}"
                query = f"SELECT {columns_str} FROM {table_str} WHERE uuid=? AND timestamp>=? AND timestamp<=? ORDER BY timestamp {options_sort} {options_limit}"
                self.logger.debug("Select query: %s", query)
                self._cur.execute(query, (device_uuid, timestamp_start, timestamp_end))
                values = self._cur.fetchall()
                data[infos[column]] = {
                    "name": infos[column],
                    "values": self._average_data(values, len(columns))
                    if options_average
                    else values,
                }

        return {
            "uuid": device_uuid,
            "event": infos["event"],
            "names": names,
            "data": data,
        }

    def purge_data(self, device_uuid, timestamp_until):
        """
        Purge device data until specified time

        Args:
            device_uuid (string): device uuid (string)
            timestamp_until (int): timestamp to delete data before (int)

        Returns:
            bool: always True

        Raises:
            MissingParameter: if parameter is missing
            InvalidParameter: if invalid parameter is specified
        """
        # check parameters
        if device_uuid is None or len(device_uuid) == 0:
            raise MissingParameter('Parameter "device_uuid" is missing')
        if timestamp_until is None:
            raise MissingParameter('Parameter "timestamp_until" is missing')
        if timestamp_until < 0:
            raise InvalidParameter("Timestamp_until value must be positive")

        # get device infos
        infos = self.__get_device_infos(device_uuid)
        self.logger.debug("infos=%s", infos)

        # prepare query parameters
        tablename = ""
        if infos["valuescount"] == 1:
            tablename = "data1"
        if infos["valuescount"] == 2:
            tablename = "data2"
        if infos["valuescount"] == 3:
            tablename = "data3"
        if infos["valuescount"] == 4:
            tablename = "data4"

        # prepare sql query
        query = f"DELETE FROM {tablename} WHERE uuid=? AND timestamp<?"
        self.logger.debug(
            "Purge query: %s with device_uuid=%s, timestamp=%s",
            query,
            device_uuid,
            timestamp_until,
        )

        # execute query
        self._cur.execute(query, (device_uuid, timestamp_until))
        self._cnx.commit()

        return True

    def _delete_device(self, device_uuid):
        """
        Delete device from database

        Args:
            device_uuid (string): device uuid

        Returns:
            bool: always True

        Raises:
            MissingParameter: if parameter is missing
        """
        # check parameters
        if device_uuid is None or len(device_uuid) == 0:
            raise MissingParameter('Parameter "device_uuid" is missing')

        # get device infos
        infos = self.__get_device_infos(device_uuid)
        self.logger.debug("infos=%s", infos)

        # prepare query parameters
        tablename = ""
        if infos["valuescount"] == 1:
            tablename = "data1"
        if infos["valuescount"] == 2:
            tablename = "data2"
        if infos["valuescount"] == 3:
            tablename = "data3"
        if infos["valuescount"] == 4:
            tablename = "data4"

        # delete device data
        query = f"DELETE FROM {tablename} WHERE uuid=?"
        self.logger.debug("Data query: %s", query)
        self._cur.execute(query, (device_uuid,))
        self._cnx.commit()

        # delete device entry
        query = "DELETE FROM devices WHERE uuid=?"
        self.logger.debug("Devices query: %s", query)
        self._cur.execute(query, (device_uuid,))
        self._cnx.commit()

        return True

    def event_received(self, event):
        """
        Event received, stored sensor data if possible

        Args:
            event (MessageRequest): event
        """
        self.logger.debug("Event received %s", event)
        if event["device_id"] is None:  # pragma: no cover
            # no device associated
            return

        # split event
        (event_module, event_type, event_action) = event["event"].split(".")

        # delete device data
        if (
            event_module == "system"
            and event_type == "device"
            and event_action == "delete"
        ):
            self._delete_device(event["device_id"])
            return

        # get event instance
        event_instance = self.events_broker.get_event_instance(event["event"])
        if not event_instance:
            self.logger.debug('No event instance found for "%s"', event["event"])
            return

        # get and check chart values
        values = event_instance.get_chart_values(event["params"])
        if values is None:
            self.logger.trace('No chart values for event "%s"', event["event"])
            return
        if not isinstance(values, list) or len(values) == 0:
            self.logger.debug(
                'Invalid chart values for event "%s": %s', event["event"], values
            )
            return

        if len(values) == 1 and isinstance(values[0]["value"], bool):
            # handle differently single bool value to make possible chart generation:
            # we inject opposite value just before current value
            current_value = values[0]["value"]
            self._save_data(
                event["device_id"],
                event["event"],
                [
                    {
                        "field": values[0]["field"],
                        "value": 1 if current_value is False else 0,
                    }
                ],
            )
            time.sleep(1.0)
            self._save_data(
                event["device_id"],
                event["event"],
                [
                    {
                        "field": values[0]["field"],
                        "value": 1 if current_value is True else 0,
                    }
                ],
            )
        else:
            self._save_data(event["device_id"], event["event"], values)
