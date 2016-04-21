#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import sys
import socket
import logging
import logging.config
from time import sleep
from ConfigParser import ConfigParser

import peewee
from openprocurement_client.client import TendersClient

logger = logging.getLogger('transfuse')


class MyConfigParser(ConfigParser):
    OPTCRE = re.compile(
        r'(?P<option>[^=\s][^=]*)'
        r'\s*(?P<vi>[=])\s*'
        r'(?P<value>.*)$'
        )
    OPTCRE_NV = re.compile(
        r'(?P<option>[^=\s][^=]*)'
        r'\s*(?:'
        r'(?P<vi>[=])\s*'
        r'(?P<value>.*))?$'
        )
    def optionxform(self, optionstr):
        return optionstr


class MyClient(TendersClient):
    def __init__(self, key, host_url, api_version, params=None, resource=None):
        super(MyClient, self).__init__(key, host_url, api_version, params)
        if resource:
            self.prefix_path = '/api/{}/{}'.format(api_version, resource)


class TendersToMySQL(object):
    client_config = {
        'key': '',
        'host_url': "https://api-sandbox.openprocurement.org",
        'api_version': '2.2',
        'params': {},
        'timeout': 30,
        'continue': False,
        'resource': None,
        'skip_until': None,
        'max_count': 0,
    }
    mysql_config = {
        'host': 'localhost',
        'user': 'tenders',
        'passwd': '',
        'db': 'tenders',
        'db_table': 'tenders',
    }
    table_schema = {
    }

    class BaseTendersModel(peewee.Model):
        class Meta:
            db_table = 'tenders'

    def __init__(self, client_config, mysql_config, table_schema):
        self.client_config.update(client_config)
        self.mysql_config.update(mysql_config)
        self.table_schema.update(table_schema)
        # tenders client
        self.timeout = float(self.client_config.pop('timeout'))
        self.c_continue = self.client_config.pop('continue')
        self.skip_until = self.client_config.pop('skip_until')
        self.max_count = int(self.client_config.pop('max_count'))
        self.client = MyClient(**self.client_config)
        # peewee mysql
        self.db_name = self.mysql_config.pop('db')
        self.db_table = self.mysql_config.pop('db_table')
        self.mysql_db = peewee.MySQLDatabase(self.db_name, **self.mysql_config)
        self.create_model_class()

    def field_name(self, key):
        return key.replace('.', '_').replace(':', '_').lower()

    def filed_value(self, key, data):
        func = None
        if key.find(':') > 0:
            func, key = key.split(':', 1)
        chain = key.split('.')
        for key in chain:
            if isinstance(data, list):
                res = list()
                for item in data:
                    if item.get(key):
                        res.extend(item[key])
                if len(res) == 1:
                    data = res[0]
                else:
                    data = res
            else:
                data = data.get(key)
            if not data:
                return
        if isinstance(data, basestring) and len(data) > 999:
            data = data[:999]
        if func == 'count':
            data = len(data)
        return data

    def create_model_class(self):
        fields = dict() # _id=peewee.PrimaryKeyField(primary_key=True)
        for key,val in self.table_schema.items():
            name = self.field_name(key)
            fieldtype = peewee.__dict__.get(val)
            if not fieldtype:
                raise ValueError("Invalid filed type: %s", val)
            fieldopts = dict(null=True)
            if val == 'CharField' and key != 'id':
                fieldopts['max_length'] = 1000
            if val == 'DecimalField':
                fieldopts['max_digits'] = 15
                fieldopts['decimal_places'] = 2
            if key == 'id':
                fieldopts['primary_key'] = True
            fields[name] = fieldtype(**fieldopts)
        self.model_class = type('TenderModel', (self.BaseTendersModel,), fields)
        self.model_class._meta.database = self.mysql_db
        self.model_class._meta.db_table = self.db_table
        # create model instance, drop and create table
        try:
            self.model_class.select().count()
        except:
            self.model_class.create_table()

    def process_tender(self, tender):
        data = self.client.get_tender(tender.id)['data']
        fields = dict()
        for key,val in self.table_schema.items():
            value = self.filed_value(key, data)
            if value is not None:
                name = self.field_name(key)
                fields[name] = value
        item = self.model_class(**fields)
        try:
            item.save(force_insert=True)
        except peewee.IntegrityError:
            if not getattr(item, 'id', None):
                raise
            logger.warning("Update %s %s", tender.id, tender.dateModified)
            self.model_class.delete().where(
                self.model_class.id==item.id).execute()
            item.save(force_insert=True)

    def run(self):
        self.should_stop = False
        while not self.should_stop:
            socket.setdefaulttimeout(self.timeout)
            tenders_list = self.client.get_tenders()

            for tender in tenders_list:
                if self.skip_until and self.skip_until > tender.dateModified:
                    logger.debug("Ignore %s %s", tender.id, tender.dateModified)
                    continue

                logger.info("Process %s %s", tender.id, tender.dateModified)
                self.process_tender(tender)

                if self.max_count:
                    self.max_count -= 1
                    if self.max_count < 1:
                        logger.info("Reached max_count, stop.")
                        return
            # endfor

            if not tenders_list:
                if self.c_continue:
                    logger.info("Wait before next loop...")
                    sleep(10)
                else:
                    break

        # endwhile


def main():
    if len(sys.argv) < 2:
        print("Usage: transfuse config.ini")
        sys.exit(1)

    logging.config.fileConfig(sys.argv[1])

    parser = MyConfigParser(allow_no_value=True)
    parser.read(sys.argv[1])

    client_config = parser.items('client')
    mysql_config = parser.items('mysql')
    table_schema = parser.items('table_schema')

    app = TendersToMySQL(client_config, mysql_config, table_schema)
    app.run()


if __name__ == '__main__':
    main()
