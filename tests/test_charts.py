import unittest
import logging
import sys
sys.path.append('../')
from backend.charts import Charts
from raspiot.utils import InvalidParameter, MissingParameter, CommandError, Unauthorized
from raspiot.libs.tests import session
import os
import sqlite3
import time
from mock import Mock

class FakeEvent():

    def __init__(self, values):
        self.values = values

    def get_chart_values(self, params):
        return self.values

class TestCharts(unittest.TestCase):

    def setUp(self):
        self.session = session.Session(logging.CRITICAL)
        _charts = Charts
        _charts.DATABASE_PATH = '/tmp/'
        self.db_path = os.path.join(_charts.DATABASE_PATH, _charts.DATABASE_NAME)
        _charts.CHECK_SAME_THREAD = False
        self.module = self.session.setup(_charts)
        self.cnx = sqlite3.connect(os.path.join(_charts.DATABASE_PATH, _charts.DATABASE_NAME))
        self.cur = self.cnx.cursor()

    def tearDown(self):
        self.session.clean()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def __get_table_count(self, table_name, uuid=None):
        query = 'SELECT count(*) FROM %s' % table_name
        if uuid:
            query = '%s WHERE uuid="%s"' % (query, uuid)
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res[0][0]

    def __get_table_rows(self, table_name, uuid=None):
        query = 'SELECT * FROM %s' % table_name
        if uuid:
            query = '%s WHERE uuid="%s"' % (query, uuid)
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def test_check_database(self):
        self.cur.execute('SELECT name FROM sqlite_master WHERE type="table";')
        tables = self.cur.fetchall()
        tables = [table[0] for table in tables]
        self.assertTrue('data1' in tables, 'data1 table should be created')
        self.assertTrue('data2' in tables, 'data2 table should be created')
        self.assertTrue('data3' in tables, 'data3 table should be created')
        self.assertTrue('data4' in tables, 'data4 table should be created')
        self.assertTrue('devices' in tables, 'devices table should be created')

    def test_save_data_1(self):
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test', 'value':1}]
        self.module._save_data(uuid, event, values)

        count = self.__get_table_count('devices')
        self.assertEqual(count, 1, 'Devices table should have only one record')
        row = self.__get_table_rows('devices')
        #(u'132-456-789', u'test.test.test', 1, u'test', None, None, None)
        self.assertEqual(row[0][0], uuid, 'Device uuid is not properly saved')
        self.assertEqual(row[0][1], event, 'Event is not properly saved')
        self.assertEqual(row[0][2], len(values), 'Values count is not properly saved')
        self.assertEqual(row[0][3], values[0]['field'], 'Field name is not properly saved')
        self.assertEqual(row[0][4], None, 'Field name is not properly saved')
        self.assertEqual(row[0][5], None, 'Field name is not properly saved')
        self.assertEqual(row[0][6], None, 'Field name is not properly saved')
        count = self.__get_table_count('data1')
        self.assertEqual(count, 1, 'Data1 table should have only one record')
        count = self.__get_table_count('data2')
        self.assertEqual(count, 0, 'Data2 table should have no record')
        count = self.__get_table_count('data3')
        self.assertEqual(count, 0, 'Data3 table should have no record')
        count = self.__get_table_count('data4')
        self.assertEqual(count, 0, 'Data4 table should have no record')

    def test_save_data_2(self):
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}]
        self.module._save_data(uuid, event, values)

        count = self.__get_table_count('devices')
        self.assertEqual(count, 1, 'Devices table should have only one record')
        row = self.__get_table_rows('devices')
        self.assertEqual(row[0][0], uuid, 'Device uuid is not properly saved')
        self.assertEqual(row[0][1], event, 'Event is not properly saved')
        self.assertEqual(row[0][2], len(values), 'Values count is not properly saved')
        self.assertEqual(row[0][3], values[0]['field'], 'Field name is not properly saved')
        self.assertEqual(row[0][4], values[1]['field'], 'Field name is not properly saved')
        self.assertEqual(row[0][5], None, 'Field name is not properly saved')
        self.assertEqual(row[0][6], None, 'Field name is not properly saved')
        count = self.__get_table_count('data1')
        self.assertEqual(count, 0, 'Data1 table should have no record')
        count = self.__get_table_count('data2')
        self.assertEqual(count, 1, 'Data2 table should have only one record')
        count = self.__get_table_count('data3')
        self.assertEqual(count, 0, 'Data3 table should have no record')
        count = self.__get_table_count('data4')
        self.assertEqual(count, 0, 'Data4 table should have no record')

    def test_save_data_3(self):
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}, {'field':'test3', 'value':3}]
        self.module._save_data(uuid, event, values)

        count = self.__get_table_count('devices')
        self.assertEqual(count, 1, 'Devices table should have only one record')
        row = self.__get_table_rows('devices')
        self.assertEqual(row[0][0], uuid, 'Device uuid is not properly saved')
        self.assertEqual(row[0][1], event, 'Event is not properly saved')
        self.assertEqual(row[0][2], len(values), 'Values count is not properly saved')
        self.assertEqual(row[0][3], values[0]['field'], 'Field name is not properly saved')
        self.assertEqual(row[0][4], values[1]['field'], 'Field name is not properly saved')
        self.assertEqual(row[0][5], values[2]['field'], 'Field name is not properly saved')
        self.assertEqual(row[0][6], None, 'Field name is not properly saved')
        count = self.__get_table_count('data1')
        self.assertEqual(count, 0, 'Data1 table should have no record')
        count = self.__get_table_count('data2')
        self.assertEqual(count, 0, 'Data2 table should have no record')
        count = self.__get_table_count('data3')
        self.assertEqual(count, 1, 'Data3 table should have only one record')
        count = self.__get_table_count('data4')
        self.assertEqual(count, 0, 'Data4 table should have no record')

    def test_save_data_4(self):
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}, {'field':'test3', 'value':3}, {'field':'test4', 'value':4}]
        self.module._save_data(uuid, event, values)

        count = self.__get_table_count('devices')
        self.assertEqual(count, 1, 'Devices table should have only one record')
        row = self.__get_table_rows('devices')
        self.assertEqual(row[0][0], uuid, 'Device uuid is not properly saved')
        self.assertEqual(row[0][1], event, 'Event is not properly saved')
        self.assertEqual(row[0][2], len(values), 'Values count is not properly saved')
        self.assertEqual(row[0][3], values[0]['field'], 'Field name is not properly saved')
        self.assertEqual(row[0][4], values[1]['field'], 'Field name is not properly saved')
        self.assertEqual(row[0][5], values[2]['field'], 'Field name is not properly saved')
        self.assertEqual(row[0][6], values[3]['field'], 'Field name is not properly saved')
        count = self.__get_table_count('data1')
        self.assertEqual(count, 0, 'Data1 table should have no record')
        count = self.__get_table_count('data2')
        self.assertEqual(count, 0, 'Data2 table should have no record')
        count = self.__get_table_count('data3')
        self.assertEqual(count, 0, 'Data3 table should have no record')
        count = self.__get_table_count('data4')
        self.assertEqual(count, 1, 'Data4 table should have only one record')

    def test_save_data_bool(self):
        event = 'test.test.test'

        uuid = '123-456-789-0'
        values = [{'field':'test', 'value':True}]
        self.module._save_data(uuid, event, values)
        row = self.__get_table_rows('data1')
        self.assertEqual(row[0][3], 1, 'Bool value is not properly saved')

        uuid = '123-456-789-1'
        values = [{'field':'test1', 'value':True}, {'field':'test2', 'value':True}]
        self.module._save_data(uuid, event, values)
        row = self.__get_table_rows('data2')
        self.assertEqual(row[0][3], 1, 'Bool value is not properly saved')
        self.assertEqual(row[0][4], 1, 'Bool value is not properly saved')

        uuid = '123-456-789-2'
        values = [{'field':'test1', 'value':True}, {'field':'test2', 'value':True}, {'field':'test3', 'value':True}]
        self.module._save_data(uuid, event, values)
        row = self.__get_table_rows('data3')
        self.assertEqual(row[0][3], 1, 'Bool value is not properly saved')
        self.assertEqual(row[0][4], 1, 'Bool value is not properly saved')
        self.assertEqual(row[0][5], 1, 'Bool value is not properly saved')

        uuid = '123-456-789-3'
        values = [{'field':'test1', 'value':True}, {'field':'test2', 'value':True}, {'field':'test3', 'value':True}, {'field':'test4', 'value':True}]
        self.module._save_data(uuid, event, values)
        row = self.__get_table_rows('data4')
        self.assertEqual(row[0][3], 1, 'Bool value is not properly saved')
        self.assertEqual(row[0][4], 1, 'Bool value is not properly saved')
        self.assertEqual(row[0][5], 1, 'Bool value is not properly saved')
        self.assertEqual(row[0][6], 1, 'Bool value is not properly saved')

        uuid = '123-456-789-4'
        values = [{'field':'test1', 'value':False}, {'field':'test2', 'value':False}, {'field':'test3', 'value':False}, {'field':'test4', 'value':False}]
        self.module._save_data(uuid, event, values)
        row = self.__get_table_rows('data4', uuid)
        self.assertEqual(row[0][3], 0, 'Bool value is not properly saved')
        self.assertEqual(row[0][4], 0, 'Bool value is not properly saved')
        self.assertEqual(row[0][5], 0, 'Bool value is not properly saved')
        self.assertEqual(row[0][6], 0, 'Bool value is not properly saved')

    def test_save_data_existing_device(self):
        event = 'test.test.test'
        uuid = '123-456-789'
        values = [{'field':'test', 'value':1}]
        self.module._save_data(uuid, event, values)
        self.module._save_data(uuid, event, values)
        row = self.__get_table_rows('data1')
        self.assertEqual(len(row), 2, 'It should have 2 rows')

    def test_save_data_existing_device_different_event(self):
        event1 = 'test.test.test1'
        event2 = 'test.test.test2'
        uuid = '123-456-789'
        values = [{'field':'test', 'value':1}]
        self.module._save_data(uuid, event1, values)
        with self.assertRaises(CommandError) as cm:
            self.module._save_data(uuid, event2, values)
        self.assertEqual(cm.exception.message, 'Device %s cannot store values from event %s' % (uuid, event2), 'Invalid message')

    def test_save_data_exisiting_device_with_incompatible_values(self):
        event = 'test.test.test'
        uuid = '123-456-789'
        values1 = [{'field':'test', 'value':1}]
        values2 = [{'field':'test', 'value':1}, {'field':'test', 'value':2}]
        self.module._save_data(uuid, event, values1)
        with self.assertRaises(CommandError) as cm:
            self.module._save_data(uuid, event, values2)
        self.assertEqual(cm.exception.message, 'Event %s is supposed to store %d values not %d' % (event, len(values1), len(values2)), 'Invalid message')

    def test_save_data_invalid_parameters(self):
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}, {'field':'test3', 'value':3}, {'field':'test4', 'value':4}]
        
        with self.assertRaises(MissingParameter) as cm:
            self.module._save_data(None, event, values)
        self.assertEqual(cm.exception.message, 'Parameter "uuid" is missing', 'Uuid should not be None')
        with self.assertRaises(MissingParameter) as cm:
            self.module._save_data('', event, values)
        self.assertEqual(cm.exception.message, 'Parameter "uuid" is missing', 'Uuid should not be None')
        with self.assertRaises(MissingParameter) as cm:
            self.module._save_data(uuid, None, values)
        self.assertEqual(cm.exception.message, 'Parameter "event" is missing', 'Uuid should not be None')
        with self.assertRaises(MissingParameter) as cm:
            self.module._save_data(uuid, '', values)
        self.assertEqual(cm.exception.message, 'Parameter "event" is missing', 'Uuid should not be None')
        with self.assertRaises(MissingParameter) as cm:
            self.module._save_data(uuid, event, None)
        self.assertEqual(cm.exception.message, 'Parameter "values" is missing', 'Uuid should not be None')
        with self.assertRaises(InvalidParameter) as cm:
            self.module._save_data(uuid, event, 1)
        self.assertEqual(cm.exception.message, 'Parameter "values" must be a list', 'Uuid should not be None')
        with self.assertRaises(InvalidParameter) as cm:
            self.module._save_data(uuid, event, {'field':'field'})
        self.assertEqual(cm.exception.message, 'Parameter "values" must be a list', 'Uuid should not be None')
        with self.assertRaises(InvalidParameter) as cm:
            self.module._save_data(uuid, event, [])
        self.assertEqual(cm.exception.message, 'No value to save', 'Should failed if values if empty')
        with self.assertRaises(InvalidParameter) as cm:
            self.module._save_data(uuid, event, [{}, {}, {}, {}, {}])
        self.assertEqual(cm.exception.message, 'Too many values to save for event "%s". It is limited to 4 values for now: %s' % (event, [{},{},{},{},{}]), 'Should failed if too many values are passed')

    def test_get_data_1(self):
        start = int(time.time())
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}]
        self.module._save_data(uuid, event, values)
        end = int(time.time())
        
        data = self.module.get_data(uuid, start, end)
        self.assertTrue('data' in data, 'Data field is missing in get_data response')
        self.assertTrue('uuid' in data, 'Uuid field is missing in get_data response')
        self.assertTrue('event' in data, 'Event field is missing in get_data response')
        self.assertTrue('names' in data, 'Names field is missing in get_data response')
        self.assertEqual(len(data['data']), 1, 'There should have 1 data')
        self.assertEqual(len(data['names']), len(values)+1, 'There should have %d names' % (len(values)+1))
        self.assertTrue('timestamp' in data['names'], 'ts column should be returned in names')
        self.assertTrue('test1' in data['names'], 'test1 column should be returned in names')
        for name in [value['field'] for value in values]:
            self.assertTrue(name in data['names'], 'Name "%s" should exists in names' % name)
        for value in [(value['field'], value['value']) for value in values]:
            self.assertTrue(value[0] in data['data'][0], 'Name "%s" should exists in data' % value[0])
            self.assertEqual(value[1], data['data'][0][value[0]], 'Invalid value saved for field "%s"' % value[0])

    def test_get_data_2(self):
        start = int(time.time())
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}]
        self.module._save_data(uuid, event, values)
        end = int(time.time())
        
        data = self.module.get_data(uuid, start, end)
        self.assertTrue('data' in data, 'Data field is missing in get_data response')
        self.assertTrue('uuid' in data, 'Uuid field is missing in get_data response')
        self.assertTrue('event' in data, 'Event field is missing in get_data response')
        self.assertTrue('names' in data, 'Names field is missing in get_data response')
        self.assertEqual(len(data['data']), 1, 'There should have 1 data')
        self.assertEqual(len(data['names']), len(values)+1, 'There should have %d names' % (len(values)+1))
        for name in [value['field'] for value in values]:
            self.assertTrue(name in data['names'], 'Name "%s" should exists in names' % name)
        for value in [(value['field'], value['value']) for value in values]:
            self.assertTrue(value[0] in data['data'][0], 'Name "%s" should exists in data' % value[0])
            self.assertEqual(value[1], data['data'][0][value[0]], 'Invalid value saved for field "%s"' % value[0])

    def test_get_data_3(self):
        start = int(time.time())
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}, {'field':'test3', 'value':3}]
        self.module._save_data(uuid, event, values)
        end = int(time.time())
        
        data = self.module.get_data(uuid, start, end)
        self.assertTrue('data' in data, 'Data field is missing in get_data response')
        self.assertTrue('uuid' in data, 'Uuid field is missing in get_data response')
        self.assertTrue('event' in data, 'Event field is missing in get_data response')
        self.assertTrue('names' in data, 'Names field is missing in get_data response')
        self.assertEqual(len(data['data']), 1, 'There should have 1 data')
        self.assertEqual(len(data['names']), len(values)+1, 'There should have %d names' % (len(values)+1))
        for name in [value['field'] for value in values]:
            self.assertTrue(name in data['names'], 'Name "%s" should exists in names' % name)
        for value in [(value['field'], value['value']) for value in values]:
            self.assertTrue(value[0] in data['data'][0], 'Name "%s" should exists in data' % value[0])
            self.assertEqual(value[1], data['data'][0][value[0]], 'Invalid value saved for field "%s"' % value[0])

    def test_get_data_4(self):
        start = int(time.time())
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}, {'field':'test3', 'value':3}, {'field':'test4', 'value':4}]
        self.module._save_data(uuid, event, values)
        end = int(time.time())
        
        data = self.module.get_data(uuid, start, end)
        self.assertTrue('data' in data, 'Data field is missing in get_data response')
        self.assertTrue('uuid' in data, 'Uuid field is missing in get_data response')
        self.assertTrue('event' in data, 'Event field is missing in get_data response')
        self.assertTrue('names' in data, 'Names field is missing in get_data response')
        self.assertEqual(len(data['data']), 1, 'There should have 1 data')
        self.assertEqual(len(data['names']), len(values)+1, 'There should have %d names' % (len(values)+1))
        for name in [value['field'] for value in values]:
            self.assertTrue(name in data['names'], 'Name "%s" should exists in names' % name)
        for value in [(value['field'], value['value']) for value in values]:
            self.assertTrue(value[0] in data['data'][0], 'Name "%s" should exists in data' % value[0])
            self.assertEqual(value[1], data['data'][0][value[0]], 'Invalid value saved for field "%s"' % value[0])

    def test_get_data_with_options(self):
        start = int(time.time())
        uuid = '123-456-789'
        event = 'test.test.test'
        values1 = [{'field':'test1', 'value':1}, {'field':'test2', 'value':1}]
        values2 = [{'field':'test1', 'value':2}, {'field':'test2', 'value':2}]
        values3 = [{'field':'test1', 'value':3}, {'field':'test2', 'value':3}]
        values4 = [{'field':'test1', 'value':4}, {'field':'test2', 'value':4}]
        self.module._save_data(uuid, event, values1)
        time.sleep(1.0)
        self.module._save_data(uuid, event, values2)
        time.sleep(1.0)
        self.module._save_data(uuid, event, values3)
        time.sleep(1.0)
        self.module._save_data(uuid, event, values4)
        end = int(time.time())
        
        #sort asc
        data = self.module.get_data(uuid, start, end, {'sort':'asc'})
        ts = [row['ts'] for row in data['data']]
        ts_sorted = ts[:]
        ts_sorted.sort()
        self.assertTrue(ts==ts_sorted, 'Sort by asc is invalid')

        #sort desc
        data = self.module.get_data(uuid, start, end, {'sort':'desc'})
        ts = [row['ts'] for row in data['data']]
        ts_sorted = ts[:]
        ts_sorted.sort(reverse=True)
        self.assertTrue(ts==ts_sorted, 'Sort by desc is invalid')

        #limit
        data = self.module.get_data(uuid, start, end, {'limit':2})
        self.assertEqual(len(data['data']), 2, 'Limit option is invalid')

        #fields
        data = self.module.get_data(uuid, start, end, {'fields':['test2']})
        self.assertEqual(len(data['names']), 2, 'Only two colums should be returned')
        self.assertTrue('test2' in data['names'], 'test2 column should only be returned')
        self.assertTrue('test2' in data['data'][0], 'test2 column should be returned')
        self.assertTrue('test1' not in data['data'][0], 'test1 column should not be returned')

        #output as list
        data = self.module.get_data(uuid, start, end, {'output':'list'})
        self.assertTrue('test1' in data['data'], 'test1 colum should be returned in data dict')
        self.assertTrue('test2' in data['data'], 'test1 colum should be returned in data dict')
        self.assertTrue('values' in data['data']['test1'], 'Data should contain "values" key')
        self.assertTrue('name' in data['data']['test1'], 'Data should contain "name" key')
        self.assertTrue(isinstance(data['data']['test1']['values'], list), 'Data should be returned as list')

    def test_get_data_invalid_parameters(self):
        start = int(time.time())
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}, {'field':'test3', 'value':3}, {'field':'test4', 'value':4}]
        self.module._save_data(uuid, event, values)
        end = int(time.time())
        
        with self.assertRaises(MissingParameter) as cm:
            self.module.get_data(None, start, end)
        self.assertEqual(cm.exception.message, 'Parameter "uuid" is missing', 'Uuid should not be None')
        with self.assertRaises(MissingParameter) as cm:
            self.module.get_data('', start, end)
        self.assertEqual(cm.exception.message, 'Parameter "uuid" is missing', 'Uuid should not be None')
        with self.assertRaises(MissingParameter) as cm:
            self.module.get_data(uuid, None, end)
        self.assertEqual(cm.exception.message, 'Parameter "timestamp_start" is missing', 'Timestamp_start should not be None')
        with self.assertRaises(MissingParameter) as cm:
            self.module.get_data(uuid, start, None)
        self.assertEqual(cm.exception.message, 'Parameter "timestamp_end" is missing', 'Timestamp_end should not be None')
        with self.assertRaises(InvalidParameter) as cm:
            self.module.get_data(uuid, -1, end)
        self.assertEqual(cm.exception.message, 'Timestamp_start value must be positive', 'Timestamp_start should be >0')
        with self.assertRaises(InvalidParameter) as cm:
            self.module.get_data(uuid, start, -1)
        self.assertEqual(cm.exception.message, 'Timestamp_end value must be positive', 'Timestamp_end should be >0')

    def test_purge_data_1(self):
        start = int(time.time())
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}]
        self.module._save_data(uuid, event, values)
        end = int(time.time())

        count = self.__get_table_count('data1')
        self.assertEqual(count, 1, 'Data1 should not be empty')
        self.module.purge_data(uuid, end+1)
        count = self.__get_table_count('data1')
        self.assertEqual(count, 0, 'Data1 should be empty')

    def test_purge_data_2(self):
        start = int(time.time())
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}]
        self.module._save_data(uuid, event, values)
        end = int(time.time())

        count = self.__get_table_count('data2')
        self.assertEqual(count, 1, 'Data2 should not be empty')
        self.module.purge_data(uuid, end+1)
        count = self.__get_table_count('data2')
        self.assertEqual(count, 0, 'Data2 should be empty')

    def test_purge_data_3(self):
        start = int(time.time())
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}, {'field':'test3', 'value':3}]
        self.module._save_data(uuid, event, values)
        end = int(time.time())

        count = self.__get_table_count('data3')
        self.assertEqual(count, 1, 'Data3 should not be empty')
        self.module.purge_data(uuid, end+1)
        count = self.__get_table_count('data3')
        self.assertEqual(count, 0, 'Data3 should be empty')

    def test_purge_data_4(self):
        start = int(time.time())
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}, {'field':'test3', 'value':3}, {'field':'test4', 'value':4}]
        self.module._save_data(uuid, event, values)
        end = int(time.time())

        count = self.__get_table_count('data4')
        self.assertEqual(count, 1, 'Data4 should not be empty')
        self.module.purge_data(uuid, end+1)
        count = self.__get_table_count('data4')
        self.assertEqual(count, 0, 'Data4 should be empty')

    def test_purge_data_missing_parameters(self):
        start = int(time.time())
        uuid = '123-456-789'
        event = 'test.test.test'
        values = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}, {'field':'test3', 'value':3}, {'field':'test4', 'value':4}]
        self.module._save_data(uuid, event, values)
        end = int(time.time())
        
        with self.assertRaises(MissingParameter) as cm:
            self.module.purge_data(None, end)
        self.assertEqual(cm.exception.message, 'Parameter "uuid" is missing', 'Uuid should not be None')
        with self.assertRaises(MissingParameter) as cm:
            self.module.purge_data('', end)
        self.assertEqual(cm.exception.message, 'Parameter "uuid" is missing', 'Uuid should not be empty')
        with self.assertRaises(MissingParameter) as cm:
            self.module.purge_data(uuid, None)
        self.assertEqual(cm.exception.message, 'Parameter "timestamp_until" is missing', 'Timestamp_until should not be None')
        with self.assertRaises(InvalidParameter) as cm:
            self.module.purge_data(uuid, -1)
        self.assertEqual(cm.exception.message, 'Timestamp_until value must be positive', 'Timestamp_until should be >0')

    def test_delete_device_data(self):
        uuid = '123-456-789'
        event = 'test.test.test'
        values1 = [{'field':'test1', 'value':1}]
        values2 = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}]
        values3 = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}, {'field':'test3', 'value':3}]
        values4 = [{'field':'test1', 'value':1}, {'field':'test2', 'value':2}, {'field':'test3', 'value':3}, {'field':'test4', 'value':4}]

        self.module._save_data(uuid, event, values1)
        count = self.__get_table_count('data1', uuid)
        self.assertEqual(count, 1, 'Device data should be inserted')
        count = self.__get_table_count('devices', uuid)
        self.assertEqual(count, 1, 'Device should be inserted')
        self.module._delete_device(uuid)
        count = self.__get_table_count('data1', uuid)
        self.assertEqual(count, 0, 'Device data should be deleted')
        count = self.__get_table_count('devices', uuid)
        self.assertEqual(count, 0, 'Device should be deleted')

        self.module._save_data(uuid, event, values2)
        self.module._delete_device(uuid)
        count = self.__get_table_count('data2', uuid)
        self.assertEqual(count, 0, 'Device data should be deleted')
        count = self.__get_table_count('devices', uuid)
        self.assertEqual(count, 0, 'Device should be deleted')

        self.module._save_data(uuid, event, values3)
        self.module._delete_device(uuid)
        count = self.__get_table_count('data3', uuid)
        self.assertEqual(count, 0, 'Device data should be deleted')
        count = self.__get_table_count('devices', uuid)
        self.assertEqual(count, 0, 'Device should be deleted')

        self.module._save_data(uuid, event, values4)
        self.module._delete_device(uuid)
        count = self.__get_table_count('data4', uuid)
        self.assertEqual(count, 0, 'Device data should be deleted')
        count = self.__get_table_count('devices', uuid)
        self.assertEqual(count, 0, 'Device should be deleted')

    def test_delete_device_data_invalid_parameters(self):
        uuid = '123-456-789'

        with self.assertRaises(MissingParameter) as cm:
            self.module._delete_device(None)
        self.assertEqual(cm.exception.message, 'Parameter "uuid" is missing', 'Uuid should not be None')
        with self.assertRaises(MissingParameter) as cm:
            self.module._delete_device('')
        self.assertEqual(cm.exception.message, 'Parameter "uuid" is missing', 'Uuid should not be None')

    def test_event_received(self):
        uuid = '123-456-789'
        event = {'event':'test.test.test', 'params':{}, 'startup':False, 'device_id':uuid, 'from':'test'}
        fake_event = FakeEvent([{'field':'test', 'value':666}])
        self.module.events_broker.get_event_instance = Mock(return_value=fake_event)
        
        count = self.__get_table_count('data1', uuid)
        self.assertEqual(count, 0, 'Data1 should be empty')
        self.module.event_received(event)
        self.assertEqual(self.module.events_broker.get_event_instance.call_count, 1, 'Get_event_instance should be called')
        count = self.__get_table_count('data1', uuid)
        self.assertEqual(count, 1, 'Data1 should contain single record')

    def test_event_received_delete_device(self):
        uuid = '123-456-789'
        event = {'event':'system.device.delete', 'params':{}, 'startup':False, 'device_id':uuid, 'from':'test'}
        self.module._delete_device = Mock()
        
        self.module.event_received(event)
        self.assertEqual(self.module._delete_device.call_count, 1, '_delete_device should be called')

    def test_event_received_event_not_found(self):
        uuid = '123-456-789'
        event = {'event':'test.test.test', 'params':{}, 'startup':False, 'device_id':uuid, 'from':'test'}
        self.module.events_broker.get_event_instance = Mock(return_value=None)
        
        self.module.event_received(event)
        self.assertEqual(self.module.events_broker.get_event_instance.call_count, 1, 'Get_event_instance should be called')
        count = self.__get_table_count('data1', uuid)
        self.assertEqual(count, 0, 'Data1 should be empty')

    def test_event_received_no_chart_value(self):
        uuid = '123-456-789'
        event = {'event':'test.test.test', 'params':{}, 'startup':False, 'device_id':uuid, 'from':'test'}
        fake_event = FakeEvent(None)
        self.module.events_broker.get_event_instance = Mock(return_value=fake_event)
        
        self.module.event_received(event)
        self.assertEqual(self.module.events_broker.get_event_instance.call_count, 1, 'Get_event_instance should be called')
        count = self.__get_table_count('data1', uuid)
        self.assertEqual(count, 0, 'Data1 should be empty')
        
    def test_event_received_invalid_chart_value(self):
        uuid = '123-456-789'
        event = {'event':'test.test.test', 'params':{}, 'startup':False, 'device_id':uuid, 'from':'test'}

        fake_event = FakeEvent({})
        self.module.events_broker.get_event_instance = Mock(return_value=fake_event)
        self.module.event_received(event)
        count = self.__get_table_count('data1', uuid)
        self.assertEqual(count, 0, 'Data1 should be empty')

        fake_event = FakeEvent(666)
        self.module.events_broker.get_event_instance = Mock(return_value=fake_event)
        self.module.event_received(event)
        count = self.__get_table_count('data1', uuid)
        self.assertEqual(count, 0, 'Data1 should be empty')

        fake_event = FakeEvent('evil')
        self.module.events_broker.get_event_instance = Mock(return_value=fake_event)
        self.module.event_received(event)
        count = self.__get_table_count('data1', uuid)
        self.assertEqual(count, 0, 'Data1 should be empty')

    def test_event_received_single_true_value(self):
        uuid = '123-456-789'
        event = {'event':'test.test.test', 'params':{}, 'startup':False, 'device_id':uuid, 'from':'test'}
        fake_event = FakeEvent([{'field':'test', 'value':True}])
        self.module.events_broker.get_event_instance = Mock(return_value=fake_event)
        
        self.module.event_received(event)
        self.assertEqual(self.module.events_broker.get_event_instance.call_count, 1, 'Get_event_instance should be called')
        count = self.__get_table_count('data1', uuid)
        self.assertEqual(count, 2, 'Data1 should contain 2 records')
        rows = self.__get_table_rows('data1')
        self.assertEqual(rows[0][3], 0, '0 value should be inserted before real value')
        self.assertEqual(rows[1][3], 1, '1 value should be inserted instead of real value')

    def test_event_received_single_false_value(self):
        uuid = '123-456-789'
        event = {'event':'test.test.test', 'params':{}, 'startup':False, 'device_id':uuid, 'from':'test'}
        fake_event = FakeEvent([{'field':'test', 'value':False}])
        self.module.events_broker.get_event_instance = Mock(return_value=fake_event)
        
        self.module.event_received(event)
        self.assertEqual(self.module.events_broker.get_event_instance.call_count, 1, 'Get_event_instance should be called')
        count = self.__get_table_count('data1', uuid)
        self.assertEqual(count, 2, 'Data1 should contain 2 records')
        rows = self.__get_table_rows('data1')
        self.assertEqual(rows[0][3], 1, '1 value should be inserted before real value')
        self.assertEqual(rows[1][3], 0, '0 value should be inserted instead of real value')





if __name__ == "__main__":
    unittest.main()
    
