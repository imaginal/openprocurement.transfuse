#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import socket
import logging
import logging.config

from argparse import ArgumentParser
from ConfigParser import ConfigParser

from munch import munchify
from iso8601 import parse_date
from restkit.errors import ResourceError
from openprocurement_client.client import TendersClient

import simplejson as json
import peewee

logger = logging.getLogger('transfuse')


class MyConfigParser(ConfigParser):
    def optionxform(self, optionstr):
        return optionstr


class MyApiClient(TendersClient):
    def __init__(self, key, config):
        params = dict()
        if config['mode'] in ('test', '_all_'):
            params['mode'] = config['mode']
        TendersClient.__init__(self, key, config['host_url'], config['api_version'], params)
        if config.get('resource'):
            self.prefix_path = '/api/{}/{}'.format(config['api_version'], config['resource'])
        if config['timeout']:
            socket.setdefaulttimeout(float(config['timeout']))

    def get_tender(self, tender_id):
        for i in range(5):
            try:
                resp = TendersClient.get_tender(self, tender_id)
            except ResourceError as e:
                logger.error("get_tender on %s error %s", tender_id, e)
                time.sleep(10)
            else:
                return resp
        raise ResourceError("Maximum retry reached")


class BaseTendersModel(peewee.Model):
    class Meta:
        db_table = 'tenders'


class TendersToMySQL(object):
    client_config = {
        'key': '',
        'host_url': "https://api-sandbox.openprocurement.org",
        'api_version': '2',
        'mode': "_all_",
        'timeout': 30,
        'offset': None,
        'limit': None,
        'resume': False,
    }
    server_config = {
        'class': 'MySQLDatabase',
        'host': 'localhost',
        'user': 'tenders',
        'passwd': '',
        'db': 'tenders',
        'db_table': 'tenders',
    }
    table_schema = {
    }
    field_types = {
        'char': (peewee.CharField, {'null': True, 'max_length': 250}),
        'longchar': (peewee.CharField, {'null': True, 'max_length': 2000}),
        'text': (peewee.TextField, {'null': True}),
        'date': (peewee.DateTimeField, {'null': True}),
        'int': (peewee.IntegerField, {'null': True}),
        'bigint': (peewee.BigIntegerField, {'null': True}),
        'float': (peewee.FloatField, {'null': True}),
        'decimal': (peewee.DecimalField, {'null': True, 'max_digits': 16, 'decimal_places': 2}),
        'bool': (peewee.BooleanField, {'null': True})
    }

    def __init__(self, config, args):
        self.client_config.update(config.items('client'))
        self.server_config.update(config.items('server'))
        # update config from args
        self.update_config(args)
        # create client
        api_key = self.client_config.pop('key')
        logger.info("Create client %s", self.client_config)
        self.client = MyApiClient(api_key, self.client_config)
        # create database connection
        passwd = self.server_config.pop('passwd')
        logger.info("Connect server %s", self.server_config)
        if passwd: self.server_config['passwd'] = passwd
        db_class = peewee.__dict__.get(self.server_config.pop('class'))
        self.db_name = self.server_config.pop('db')
        self.db_table = self.server_config.pop('db_table')
        self.database = db_class(self.db_name, **self.server_config)
        # create model class
        self.create_models(config)

    def update_config(self, args):
        for key in ('offset', 'limit', 'resume'):
            if getattr(args, key, None):
                self.client_config[key] = getattr(args, key)

    @staticmethod
    def field_name(name):
        return name.replace('.', '_').replace('(', '_').replace(')', '').strip()

    def create_table(self, model_class):
        logger.debug("Drop & Create table `%s`", model_class._meta.db_table)
        try:
            model_class.select().count()
            model_class.drop_table()
        except:
            pass
        model_class.create_table()

    def init_model(self, table_name, table_schema):
        if table_name in self.models:
            raise IndexError('Model %s already exists', table_name)
        logger.info("Create model %s", table_name)
        fields = dict()
        parsed_schema = list()
        table_options = dict()
        has_primary_key = False

        for key,val in table_schema:
            if key.startswith('__'):
                if key == '__iter__':
                    val = val.split('.')
                table_options[key] = val
                continue
            name = self.field_name(key)
            logger.debug("+ %s %s", name, val)
            opts = [s.strip() for s in val.split(',')]
            # field = type,flags,max_length
            fieldtype, fieldopts = self.field_types.get(opts[0])
            if len(opts) > 1:
                fieldopts = dict(fieldopts)
                fieldopts[ opts[1] ] = True
                if opts[1] == 'primary_key':
                    has_primary_key = True
            if len(opts) > 2:
                fieldopts['max_length'] = int(opts[2])
            fields[name] = fieldtype(**fieldopts)
            # parse field path
            funcs = key.replace(')','').split('(')
            chain = funcs.pop().split('.')
            parsed_schema.append((name, chain, funcs, opts[0]))

        if not has_primary_key:
            fields['pk_id'] = peewee.PrimaryKeyField(primary_key=True)
        model_class = type(name+'Model', (BaseTendersModel,), fields)
        model_class._meta.table_options = table_options
        model_class._meta.table_schema = parsed_schema
        model_class._meta.database = self.database
        model_class._meta.db_table = table_name
        self.models[table_name] = model_class
        if not self.client_config.get('resume', False):
            self.create_table(model_class)

    def create_models(self, config):
        self.models = dict()
        for section in config.sections():
            if section.startswith('table:'):
                table_schema = config.items(section)
                table, name = section.split(':', 2)
                self.init_model(name, table_schema)

    def apply_func(self, fn, data):
        if fn == 'count':
            return len(data)
        if fn == 'sum':
            return sum(data)
        if fn == 'min':
            return min(data)
        if fn == 'max':
            return max(data)
        if fn == 'avg':
            return sum(data)/len(data)
        raise ValueError('unknown function '+fn)

    def field_value(self, chain, funcs, data):
        for key in chain:
            if isinstance(data, list):
                res = list()
                for item in data:
                    val = item.get(key)
                    if isinstance(val, list):
                        res.extend(val)
                    elif val is not None:
                        res.append(val)
                for item in res:
                    if isinstance(item, dict):
                        item['parent'] = data
                data = res
            elif data:
                data = data.get(key)
            if data is None:
                return
        for fn in funcs:
            data = self.apply_func(fn, data)
        return data

    @staticmethod
    def parse_iso_datetime(value):
        return parse_date(value).replace(tzinfo=None)

    def process_model_item(self, model_class, data):
        table_schema = model_class._meta.table_schema
        fields = dict()
        for field_info in table_schema:
            name, chain, funcs, ftype = field_info
            value = self.field_value(chain, funcs, data)
            if value is None:
                continue
            if isinstance(value, list):
                value = value[0] if len(value) else None
            if ftype == 'date':
                value = self.parse_iso_datetime(value)
            fields[name] = value
        item = model_class(**fields)
        try:
            item.save(force_insert=True)
        except peewee.IntegrityError:
            logger.warning("Delete before insert %s", data.id)
            item.delete_instance()
            item.save(force_insert=True)

    def process_model_data(self, model_class, data):
        table_options = model_class._meta.table_options
        if table_options.get('__iter__'):
            iter_name = table_options['__iter__']
            iter_list = self.field_value(iter_name, [], data)
            root_name = table_options.get('__root__', 'root')
            if iter_list:
                for item in iter_list:
                    logger.info("+ Child %s %s", item.get('id'), iter_name)
                    item[root_name] = data
                    self.process_model_item(model_class, item)
        else:
            return self.process_model_item(model_class, data)

    def process_tender(self, tender):
        data = self.client.get_tender(tender.id)['data']
        for model_name,model_class in self.models.items():
            self.process_model_data(model_class, data)

    def run(self):
        offset = self.client_config.get('offset', '')
        limit = int(self.client_config.get('limit') or 0)
        self.total_processed = 0

        if offset:
            self.client.params['offset'] = offset

        while True:
            tenders_list = self.client.get_tenders(feed='')

            for tender in tenders_list:
                if offset and offset > tender.dateModified:
                    logger.debug("Ignore %s %s", tender.id, tender.dateModified)
                    continue

                dateModified = tender.dateModified.replace('T', ' ')[:19]
                logger.info("Process %s %s", tender.id, dateModified)
                self.process_tender(tender)
                self.total_processed += 1

                if limit and self.total_processed >= limit:
                    logger.info("Reached limit, stop.")
                    return

            if not tenders_list:
                logger.info("No more records.")
                break

    def run_debug(self):
        with open('debug/tender.json') as f:
            tender = json.load(f)
        data = munchify(tender['data'])
        for model_name,model_class in self.models.items():
            self.process_model_data(model_class, data)


def main():
    parser = ArgumentParser(description='OpenProcurement API to SQL bridge')
    parser.add_argument('config', nargs='+', help='ini file(s)')
    parser.add_argument('--offset', type=int, help='client offset')
    parser.add_argument('--limit', type=int, help='client limit')
    parser.add_argument('--resume', action='store_true', help='dont drop table')
    args = parser.parse_args()

    for inifile in args.config:
        logging.config.fileConfig(inifile)
        break

    config = MyConfigParser(allow_no_value=True)
    for inifile in args.config:
        config.read(inifile)

    app = TendersToMySQL(config, args)
    return app.run()


if __name__ == '__main__':
    main()
