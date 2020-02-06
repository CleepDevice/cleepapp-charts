#!/usr/bin/env python
# -*- coding: utf-8 -*-
    
import os
import sqlite3
import logging
from raspiot.raspiot import RaspIotModule
from raspiot.utils import CommandError, MissingParameter, InvalidParameter
import time
import threading

__all__ = [u'Charts']

class Charts(RaspIotModule):
    """
    Module that provides data storage in local database for charts generation
    Also unlock sensors and system charts
    """
    MODULE_AUTHOR = u'Cleep'
    MODULE_VERSION = u'1.0.0'
    MODULE_PRICE = 0
    MODULE_DEPS = []
    MODULE_DESCRIPTION = u'Follow easily your sensors values graphically.'
    MODULE_LONGDESCRIPTION = u'Charts application automatically generates charts according to your connected sensors values.<br> \
                             It allows you to follow in time the evolution of the measurements of your sensors'
    MODULE_CATEGORY = u'APPLICATION'
    MODULE_TAGS = [u'sensors', u'graphs', u'charts', u'database']
    MODULE_COUNTRY = None
    MODULE_URLINFO = u'https://github.com/tangb/cleepmod-charts'
    MODULE_URLHELP = u'https://github.com/tangb/cleepmod-charts/wiki'
    MODULE_URLSITE = None
    MODULE_URLBUGS = u'https://github.com/tangb/cleepmod-charts/issues'

    MODULE_CONFIG_FILE = u'charts.conf'

    DATABASE_PATH = u'/etc/raspiot/charts'
    DATABASE_NAME = u'charts.db'
    #for debug only, to avoid exception during session closing
    CHECK_SAME_THREAD = True

    def __init__(self, bootstrap, debug_enabled):
        """
        Constructor

        Args:
            bootstrap (dict): bootstrap objects
            debug_enabled (bool): flag to set debug level to logger
        """
        #init
        RaspIotModule.__init__(self, bootstrap, debug_enabled)

        #member
        self._cnx = None
        self._cur = None

        #make sure database path exists
        if not os.path.exists(Charts.DATABASE_PATH):
            self.cleep_filesystem.mkdir(Charts.DATABASE_PATH, True)

    def _configure(self):
        """
        Configure module
        """
        #make sure database file exists
        if not os.path.exists(os.path.join(Charts.DATABASE_PATH, Charts.DATABASE_NAME)):
            self.logger.debug(u'Database file not found')
            self._init_database()

        path = os.path.join(Charts.DATABASE_PATH, Charts.DATABASE_NAME)
        self.logger.debug(u'Connect to database "%s"' % path)
        self._cnx = sqlite3.connect(path, check_same_thread=Charts.CHECK_SAME_THREAD)
        self._cur = self._cnx.cursor()

    def _stop(self):
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
        self.logger.debug(u'Initialize database "%s"' % path)

        #create database file
        cnx = sqlite3.connect(path)
        cur = cnx.cursor()

        #create devices table (handle number of values associated to device)
        #format:
        # - uuid: store device uuid (string) (primary key)
        # - event: event type stored for the device (string)
        # - valuescount: number of values saved for the device
        # - value1: field name for value1
        # - value2: field name for value2
        # - value3: field name for value3
        # - value4: field name for value4
        cur.execute(u'CREATE TABLE devices(uuid TEXT PRIMARY KEY UNIQUE, event TEXT, valuescount INTEGER, value1 NUMBER DEFAULT NULL, value2 TEXT DEFAULT NULL, value3 TEXT DEFAULT NULL, value4 TEXT DEFAULT NULL);')

        #create data1 table (contains 1 field to store value, typically light/humidity... sensors)
        #format:
        # - id: unique id (primary key)
        # - timestamp: timestamp when value was inserted
        # - uuid: device uuid that pushes values
        # - value, value1, value2, value3, value4: values of device
        cur.execute(u'CREATE TABLE data1(id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, timestamp INTEGER, uuid TEXT, value1 NUMBER);')
        cur.execute(u'CREATE INDEX data1_device_index ON data1(uuid);')
        cur.execute(u'CREATE INDEX data1_timestamp_index ON data1(timestamp);')

        #create data2 table (contains 2 fields to store values, typically gps positions, temperature (C° and F°))
        cur.execute(u'CREATE TABLE data2(id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, timestamp INTEGER, uuid TEXT, value1 NUMBER, value2 NUMBER);')
        cur.execute(u'CREATE INDEX data2_device_index ON data2(uuid);')
        cur.execute(u'CREATE INDEX data2_timestamp_index ON data2(timestamp);')

        #create data3 table (contains 3 fields to store values)
        cur.execute(u'CREATE TABLE data3(id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, timestamp INTEGER, uuid TEXT, value1 NUMBER, value2 NUMBER, value3 NUMBER);')
        cur.execute(u'CREATE INDEX data3_device_index ON data3(uuid);')
        cur.execute(u'CREATE INDEX data3_timestamp_index ON data3(timestamp);')

        #create data4 table (contains 4 fields to store values)
        cur.execute(u'CREATE TABLE data4(id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, timestamp INTEGER, uuid TEXT, value1 NUMBER, value2 NUMBER, value3 NUMBER, value4 NUMBER);')
        cur.execute(u'CREATE INDEX data4_device_index ON data4(uuid);')
        cur.execute(u'CREATE INDEX data4_timestamp_index ON data4(timestamp);')

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
            if current_field==u'timestamp':
                #return reduced string of timestamp
                return u'ts'
            else: 
                return fields[current_field]
        except:
            #field name not found
            return current_field

    def _save_data(self, uuid, event, values):
        """
        Save data into database

        Args:
            uuid (string): device uuid
            event (string): event name
            values (list): values to save (must be an list of dict(<field>,<value>))

        Raises:
            InvalidParameter: if invalid parameter is specified
        """
        self.logger.debug(u'Set_data uuid=%s event=%s values=%s' % (uuid, event, unicode(values)))
        if uuid is None or len(uuid)==0:
            raise MissingParameter(u'Parameter "uuid" is missing')
        if event is None or len(event)==0:
            raise MissingParameter(u'Parameter "event" is missing')
        if values is None:
            raise MissingParameter(u'Parameter "values" is missing')
        if not isinstance(values, list):
            raise InvalidParameter(u'Parameter "values" must be a list')
        if len(values)==0:
            raise InvalidParameter(u'No value to save')
        if len(values)>4:
            raise InvalidParameter(u'Too many values to save for event "%s". It is limited to 4 values for now: %s' % (event, values))

        def get_value(value):
            """
            Prevent bool value to be stored (replace it with 1 or 0)
            """
            if not isinstance(value, bool):
                return value
            return 1 if value is True else 0

        #save uuid infos at first insert
        self._cur.execute(u'SELECT * FROM devices WHERE uuid=?', (uuid,))
        row = self._cur.fetchone()
        if row is None:
            #no infos yet, insert new entry for this device
            if len(values)==1:
                self._cur.execute(u'INSERT INTO devices(uuid, event, valuescount, value1) VALUES(?,?,?,?)', (uuid, event, len(values), values[0][u'field']))
            elif len(values)==2:
                self._cur.execute(u'INSERT INTO devices(uuid, event, valuescount, value1, value2) VALUES(?,?,?,?,?)', (uuid, event, len(values), values[0][u'field'], values[1][u'field']))
            elif len(values)==3:
                self._cur.execute(u'INSERT INTO devices(uuid, event, valuescount, value1, value2, value3) VALUES(?,?,?,?,?,?)', (uuid, event, len(values), values[0][u'field'], values[1][u'field'], values[2][u'field']))
            elif len(values)==4:
                self._cur.execute(u'INSERT INTO devices(uuid, event, valuescount, value1, value2, value3, value4) VALUES(?,?,?,?,?,?,?)', (uuid, event, len(values), values[0][u'field'], values[1][u'field'], values[2][u'field'], values[3][u'field']))
        else:
            #entry exists, check it
            infos = dict((self._cur.description[i][0], value) for i, value in enumerate(row))
            if infos[u'event']!=event:
                raise CommandError(u'Device %s cannot store values from event %s' % (uuid, event))
            if infos[u'valuescount']!=len(values):
                raise CommandError(u'Event %s is supposed to store %d values not %d' % (event, infos[u'valuescount'], len(values)))

        #save values
        if len(values)==1:
            self._cur.execute(u'INSERT INTO data1(timestamp, uuid, value1) values(?,?,?)', (int(time.time()), uuid, get_value(values[0][u'value'])))
        elif len(values)==2:
            self._cur.execute(u'INSERT INTO data2(timestamp, uuid, value1, value2) values(?,?,?,?)', (int(time.time()), uuid, get_value(values[0][u'value']), get_value(values[1][u'value'])))
        elif len(values)==3:
            self._cur.execute(u'INSERT INTO data3(timestamp, uuid, value1, value2, value3) values(?,?,?,?,?)', (int(time.time()), uuid, get_value(values[0][u'value']), get_value(values[1][u'value']), get_value(values[2][u'value'])))
        elif len(values)==4:
            self._cur.execute(u'INSERT INTO data4(timestamp, uuid, value1, value2, value3, value4) values(?,?,?,?,?,?)', (int(time.time()), uuid, get_value(values[0][u'value']), get_value(values[1][u'value']), get_value(values[2][u'value']), get_value(values[3][u'value'])))

        #commit changes
        self._cnx.commit()
        
        return True

    def __get_device_infos(self, uuid):
        """
        Return device infos (read from "devices" table)

        Args:
            uuid (string): uuid

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
        self._cur.execute(u'SELECT event, valuescount, value1, value2, value3, value4 FROM devices WHERE uuid=?', (uuid,))
        row = self._cur.fetchone()
        if row is None:
            raise CommandError(u'Device %s not found!' % uuid)
        return dict((self._cur.description[i][0], value) for i, value in enumerate(row))

    def get_data(self, uuid, timestamp_start, timestamp_end, options=None):
        """
        Return data from data table

        Args:
            uuid (string): device uuid
            timestamp_start (int): start of range
            timestamp_end (int): end of range
            options (dict): command options::

                {
                    output (string): output format ('list'|'dict'[default])
                    fields (list): list of fields to return
                    sort (string): sort value ('asc'[default]|'desc')
                    limit (int): limit number
                }

        Returns:
            dict: data::

                {
                    uuid (string): device uuid
                    event (string): event name
                    names (list): list of column names
                    data (dict): dict of data. Content can be a list or a dict according to "outpu" option
                }

        Raises:
            InvalidParameter: if invalid parameter is specified
            MissingParameter: if parameter is missing
        """
        #check parameters
        if uuid is None or len(uuid)==0:
            raise MissingParameter(u'Parameter "uuid" is missing')
        if timestamp_start is None:
            raise MissingParameter(u'Parameter "timestamp_start" is missing')
        if timestamp_start<0:
            raise InvalidParameter(u'Timestamp_start value must be positive') 
        if timestamp_end is None:
            raise MissingParameter(u'Parameter "timestamp_end" is missing')
        if timestamp_end<0:
            raise InvalidParameter(u'Timestamp_end value must be positive') 

        #prepare options
        options_fields = []
        options_output = u'dict'
        options_sort = u'asc'
        options_limit = u''
        if options is not None:
            if u'fields' in options:
                options_fields = options[u'fields']
            if u'output' in options and options[u'output'] in (u'list', u'dict'):
                options_output = options[u'output']
            if u'sort' in options and options[u'sort'] in (u'asc', u'desc'):
                options_sort = options[u'sort']
            if u'limit' in options and isinstance(options[u'limit'], int):
                options_limit = 'LIMIT %d' % options['limit']
        self.logger.debug(u'options: fields=%s output=%s sort=%s limit=%s' % (options_fields, options_output, options_sort, options_limit))

        #get device infos
        infos = self.__get_device_infos(uuid)
        self.logger.debug(u'infos=%s' % infos)

        #prepare query options
        columns = []
        names = [u'timestamp']
        if len(options_fields)==0:
            #no field filtered, add all existing fields
            columns.append(u'value1')
            names.append(infos[u'value1'])
            if infos[u'value2'] is not None:
                columns.append(u'value2')
                names.append(infos[u'value2'])
            if infos[u'value3'] is not None:
                columns.append(u'value3')
                names.append(infos[u'value3'])
            if infos[u'value4'] is not None:
                columns.append(u'value4')
                names.append(infos[u'value4'])
        else:
            #get column associated to field name
            for options_field in options_fields:
                for column in infos.keys():
                    if column.startswith(u'value') and infos[column]==options_field:
                        columns.append(column)
                        names.append(options_field)

        #get device data for each request columns
        data = None
        if options_output==u'dict':
            #output as dict
            query = u'SELECT timestamp,%s FROM data%d WHERE uuid=? AND timestamp>=? AND timestamp<=? ORDER BY timestamp %s %s' % (u','.join(columns), infos[u'valuescount'], options_sort, options_limit)
            self.logger.debug(u'Select query: %s' % query)
            self._cur.execute(query, (uuid, timestamp_start, timestamp_end))
            #@see http://stackoverflow.com/a/3287775
            data = [dict((self.__restore_field_name(self._cur.description[i][0], infos), value) for i, value in enumerate(row)) for row in self._cur.fetchall()]

        else:
            #output as list
            data = {}
            for column in columns:
                query = u'SELECT timestamp,%s FROM data%d WHERE uuid=? AND timestamp>=? AND timestamp<=? ORDER BY timestamp %s %s' % (column, infos[u'valuescount'], options_sort, options_limit)
                self.logger.debug(u'Select query: %s' % query)
                self._cur.execute(query, (uuid, timestamp_start, timestamp_end))
                data[infos[column]] = {
                    u'name': infos[column],
                    u'values': self._cur.fetchall()
                }

        return {
            u'uuid': uuid,
            u'event': infos[u'event'],
            u'names': names,
            u'data': data
        }

    def purge_data(self, uuid, timestamp_until):
        """
        Purge device data until specified time

        Args:
            uuid (string): device uuid (string)
            timestamp_until (int): timestamp to delete data before (int)

        Returns:
            bool: always True

        Raises:
            MissingParameter: if parameter is missing
            InvalidParameter: if invalid parameter is specified
        """
        #check parameters
        if uuid is None or len(uuid)==0:
            raise MissingParameter(u'Parameter "uuid" is missing')
        if timestamp_until is None:
            raise MissingParameter(u'Parameter "timestamp_until" is missing')
        if timestamp_until<0:
            raise InvalidParameter(u'Timestamp_until value must be positive') 
        
        #get device infos
        infos = self.__get_device_infos(uuid)
        self.logger.debug(u'infos=%s' % infos)

        #prepare query parameters
        tablename = u''
        if infos[u'valuescount']==1:
            tablename = u'data1'
        if infos[u'valuescount']==2:
            tablename = u'data2'
        if infos[u'valuescount']==3:
            tablename = u'data3'
        if infos[u'valuescount']==4:
            tablename = u'data4'

        #prepare sql query
        query = u'DELETE FROM %s WHERE uuid=? AND timestamp<?' % tablename
        self.logger.debug(u'Purge query: %s with uuid=%s, timestamp=%s' % (query, uuid, timestamp_until))

        #execute query
        self._cur.execute(query, (uuid, timestamp_until))
        self._cnx.commit()

        return True

    def _delete_device(self, uuid):
        """
        Delete device from database

        Args:
            uuid (string): device uuid
        
        Returns:
            bool: always True

        Raises:
            MissingParameter: if parameter is missing
        """
        #check parameters
        if uuid is None or len(uuid)==0:
            raise MissingParameter(u'Parameter "uuid" is missing')
        
        #get device infos
        infos = self.__get_device_infos(uuid)
        self.logger.debug(u'infos=%s' % infos)

        #prepare query parameters
        tablename = u''
        if infos[u'valuescount']==1:
            tablename = u'data1'
        if infos[u'valuescount']==2:
            tablename = u'data2'
        if infos[u'valuescount']==3:
            tablename = u'data3'
        if infos[u'valuescount']==4:
            tablename = u'data4'

        #delete device data
        query = u'DELETE FROM %s WHERE uuid=?' % tablename
        self.logger.debug(u'Data query: %s' % query)
        self._cur.execute(query, (uuid,))
        self._cnx.commit()

        #delete device entry
        query = u'DELETE FROM devices WHERE uuid=?'
        self.logger.debug('Devices query: %s' % query)
        self._cur.execute(query, (uuid,))
        self._cnx.commit()

        return True

    def event_received(self, event):
        """
        Event received, stored sensor data if possible

        Args:
            event (MessageRequest): event
        """
        self.logger.debug(u'Event received %s' % event)
        if event[u'device_id'] is None:
            #no device associated
            return

        #split event
        (event_module, event_type, event_action) = event[u'event'].split(u'.')

        #delete device data
        if event_module=='system' and event_type==u'device' and event_action==u'delete':
            self._delete_device(event[u'device_id'])
            return

        #get event instance
        event_instance = self.events_broker.get_event_instance(event[u'event'])
        if not event_instance:
            self.logger.debug(u'No event instance found for "%s"' % event[u'event'])
            return

        #get and check chart values
        values = event_instance.get_chart_values(event[u'params'])
        if values is None:
            self.logger.trace(u'No chart values for event "%s"' % event[u'event'])
            return
        if not isinstance(values, list) or len(values)==0:
            self.logger.debug(u'Invalid chart values for event "%s": %s' % (event[u'event'], values))
            return

        if len(values)==1 and isinstance(values[0][u'value'], bool):
            #handle differently single bool value to make possible chart generation:
            #we inject opposite value just before current value
            current_value = values[0][u'value']
            self._save_data(event[u'device_id'], event_type, [
                {
                    u'field': values[0][u'field'],
                    u'value': 1 if current_value is False else 0
                }
            ])
            time.sleep(1.0)
            self._save_data(event[u'device_id'], event_type, [
                {
                    u'field': values[0][u'field'],
                    u'value': 1 if current_value is True else 0
                }
            ])
        else:
            self._save_data(event[u'device_id'], event_type, values)

