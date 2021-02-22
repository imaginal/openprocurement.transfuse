#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import time
import fcntl
import atexit
import argparse
import codecs
import collections
import datetime
import iso8601
import munch
import peewee
import requests
import warnings
import signal
import socket
import six
import zlib
import logging
import logging.config
try:
    import configparser
except ImportError:
    import ConfigParser as configparser
try:
    import simplejson as json
except ImportError:
    import json

assert peewee.__version__ >= '3.1'


__version__ = '3.1'

logger = logging.getLogger('transfuse')

transfuse_app = None

if sys.version_info[0] < 3:
    input = raw_input
else:
    unicode = str


class MyConfigParser(configparser.RawConfigParser):
    def optionxform(self, optionstr):
        return optionstr

    def test(self, filename, prefix='table:'):
        """Check config file for model duplicates"""
        parser = MyConfigParser()
        if not parser.read(filename):
            raise ValueError("Can't read config from file %s" % filename)
        for section in parser.sections():
            if section.startswith(prefix) and section in self.sections():
                raise ValueError("Model %s duplicate in file %s" % (section, filename))
        return True


class NotFoundError(Exception):
    pass


class RetryError(Exception):
    pass


class MyApiClient(object):
    def __init__(self, config):
        self.session = requests.Session()
        if 'url' in config:
            self.prefix_path = config['url']
        else:
            self.prefix_path = '{}/api/{}/{}'.format(
                config['host_url'],
                config['api_version'],
                config['resource'])
        self.params = {'limit': 1000, 'mode': ''}
        if config.get('mode', '') in ('test', '_all_'):
            self.params['mode'] = config['mode']
        if 'list_limit' in config:
            self.params['limit'] = int(config['list_limit'])
        self.timeout = float(config.get('timeout', 30))
        if self.timeout and self.timeout > 0:
            self.session.timeout = self.timeout
        if 'auth' in config:
            self.session.auth = tuple(config['auth'].split(':', 1))
        elif 'key' in config:
            self.session.auth = (config['key'], '')
        accept_encoding = config.get('accept_encoding', 'gzip,deflate')
        user_agent = "Transfuse/%s %s" % (__version__, config['user_agent'])
        self.session.headers.update({
            'Accept-Encoding': accept_encoding,
            'User-Agent': user_agent})
        self.preload_limit = int(config.get('preload', 0))
        self.limit_rps = float(config.get('limit_rps', 9.9))
        self.max_retry = int(config.get('max_retry', 5))
        self.use_cookies = int(config.get('use_cookies', 1))
        self.next_request_time = 0
        if self.use_cookies:
            self.request_cookie()

    def close(self):
        self.session.close()

    def set_limit(self, limit):
        if self.params['limit'] > limit:
            self.params['limit'] = limit
        if self.preload_limit > limit:
            self.preload_limit = limit

    def set_offset(self, offset):
        self.params['offset'] = offset

    def wait_before_request(self):
        if self.limit_rps:
            now = time.time()
            if self.next_request_time > now:
                time.sleep(self.next_request_time - now)
            self.next_request_time = 1.0 / self.limit_rps + now

    def request_cookie(self):
        self.wait_before_request()
        self.session.get(self.prefix_path, timeout=self.timeout)
        logger.debug("Cookie: %s", self.session.cookies.items())

    def preload(self, feed='', limit=0, callback=None):
        preload_items = []
        items = True
        if self.use_cookies and not self.session.cookies:
            self.request_cookie()
        while items:
            items = self.get_list(feed=feed)
            if items:
                preload_items.extend(items)
            if items and len(items) < self.params['limit']:
                break
            if self.preload_limit and self.preload_limit < len(preload_items):
                break
            if items and callback:
                callback(len(preload_items), items[-1])
        return preload_items

    def log_request_error(self, url, exc, method='GET'):
        logger.error("{} {} {}".format(method, url, repr(exc)))
        if hasattr(exc, 'request') and getattr(exc, 'request', None):
            request = exc.request
            headers = "\n".join(["  {}: {}".format(*i) for i in request.headers.items()])
            logger.debug("Request {} {}\n{}".format(request.method, request.url, headers))
        if hasattr(exc, 'response') and getattr(exc, 'response', None):
            response = exc.response
            headers = "\n".join(["  {}: {}".format(*i) for i in response.headers.items()])
            logger.debug("Response {}\n{}".format(response.status_code, headers))

    def get_list(self, params={}, feed='changes'):
        params['feed'] = feed
        for i in range(self.max_retry):
            try:
                self.wait_before_request()
                self.params.update(params)
                response = self.session.get(self.prefix_path,
                    params=self.params, timeout=self.timeout)

                if response.status_code == 404:
                    raise NotFoundError("404 Not found {}".format(self.prefix_path))
                else:
                    response.raise_for_status()

                resp_list = munch.munchify(response.json())
                if 'next_page' in resp_list and 'offset' in resp_list['next_page']:
                    self.params['offset'] = resp_list.next_page.offset
                return resp_list['data']

            except (socket.error, requests.RequestException) as e:
                self.log_request_error(self.prefix_path, e)
                if i < self.max_retry - 1:
                    time.sleep(10 * i + 10)

        raise RetryError("Maximum retry reached for {}".format(self.prefix_path))

    def get(self, item_id):
        url = "{}/{}".format(self.prefix_path, item_id)
        for i in range(self.max_retry):
            try:
                if self.use_cookies and not self.session.cookies:
                    self.request_cookie()
                self.wait_before_request()
                response = self.session.get(url, timeout=self.timeout)

                if response.status_code == 404:
                    raise NotFoundError("404 Not found {}".format(url))
                else:
                    response.raise_for_status()

                resp_data = munch.munchify(response.json())
                return resp_data['data']

            except (socket.error, requests.RequestException) as e:
                self.log_request_error(url, e)
                if i > 1:
                    self.session.cookies.clear()
                if i < self.max_retry - 1:
                    time.sleep(10 * i + 10)

        raise RetryError("Maximum retry reached for {}".format(url))


class MyModelIndex(peewee.Index):
    def __init__(self, model, fields, unique=False, safe=True, where=None,
                 using=None, name=None):
        self._model = model
        if name is None:
            f_names = [getattr(f, 'column_name', str(f)) for f in fields]
            name = "{}_{}".format(model._meta.table_name, "_".join(f_names))
        if getattr(model._meta, 'index_max_length', None):
            index_max_length = model._meta.index_max_length
            new_fields = list()
            for field in fields:
                if isinstance(field, peewee.CharField):
                    column_name = "%s(%d)" % (field.column_name, index_max_length)
                    new_fields.append(column_name)
                else:
                    new_fields.append(field)
        else:
            new_fields = fields
        super(MyModelIndex, self).__init__(
            name=name,
            table=model._meta.table,
            expressions=new_fields,
            unique=unique,
            safe=safe,
            where=where,
            using=using)


peewee.ModelIndex = MyModelIndex


class BaseTendersModel(peewee.Model):
    class Meta:
        pass

    @classmethod
    def model_name(klass):
        return klass._meta.table_name


class CacheTendersModel(BaseTendersModel):
    tender_id = peewee.CharField(primary_key=True)
    dateModified = peewee.CharField()
    gzip_data = peewee.BlobField()


def force_encoding(value, encoding='cp1251'):
    try:
        value.encode(encoding)
    except AttributeError:
        pass
    except UnicodeEncodeError:
        value = value.encode(encoding, 'replace').decode(encoding)
    return value


def relative_offset(offset):
    if offset and offset[0] in ('+', '-'):
        muls = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
        if offset[-1] in muls:
            seconds = muls[offset[-1]] * int(offset[1:-1])
        else:
            seconds = int(offset[1:])
        toffs = datetime.datetime.now()
        delta = datetime.timedelta(seconds=seconds)
        if offset[0] == '+':
            toffs += delta
        else:
            toffs -= delta
        offset = toffs.isoformat()
    return offset


def avg(data):
    if data:
        return sum(data) / len(data)


def safe_dumps(data):
    return json.dumps(data, default=str, ensure_ascii=False, sort_keys=True)


def join(data, sep=','):
    if isinstance(data, (list, dict, tuple)):
        return sep.join(map(str, data))
    return str(data)


def remove_pidfile(lockfile, filename, mypid):
    if mypid != os.getpid():
        return
    logger.debug("Remove pidfile %s", filename)
    lockfile.seek(0)
    filepid = int(lockfile.readline(10) or 0)
    fcntl.lockf(lockfile, fcntl.LOCK_UN)
    lockfile.close()
    if mypid == filepid:
        os.remove(filename)


def write_pidfile(filename, wait=0):
    if not filename:
        return
    # try get exclusive lock to prevent second start
    mypid = os.getpid()
    logger.info("Save process id %d to %s", mypid, filename)
    lockfile = None
    for i in range(wait + 1):
        try:
            lockfile = open(filename, "w+")
            fcntl.lockf(lockfile, fcntl.LOCK_EX + fcntl.LOCK_NB)
            lockfile.write(str(mypid) + "\n")
            lockfile.flush()
            atexit.register(remove_pidfile, lockfile, filename, mypid)
        except IOError as e:
            logger.error("Could not open lock file %s (%s)", filename, e)
            if lockfile:
                lockfile.close()
            if i == wait:
                raise
            time.sleep(1)
            continue
        break


class TendersToSQL(object):
    CHAR_MAX_LENGTH = 250
    general_config = {
        'timeout': 0,
        'lockfile': None,
        'lockwait': 0,
        'warnings': "error",
    }
    client_config = {
        'key': "",
        'host_url': "https://public.api.openprocurement.org",
        'api_version': "0",
        'resource': "tenders",
        'user_agent': '',
        'mode': "",
        'feed': "",
        'offset': None,
        'limit': None,
        'alive': False,
        'resume': False,
        'ignore': False,
        'preload': 0,
        'timeout': 30,
    }
    server_config = {
        'class': 'MySQLDatabase',
    }
    server_defaults = {
        'MySQLDatabase': {
            'host': 'localhost',
            'user': 'prozorro',
            'passwd': 'prozorro',
            'db': 'prozorro',
            'index_max_length': 40,
        },
        'PostgresqlDatabase': {
            'host': 'localhost',
            'user': 'prozorro',
            'password': 'prozorro',
            'database': 'prozorro',
        },
        'SqliteDatabase': {
            'db': 'prozorro.db',
        }
    }
    table_schema = {
    }
    field_types = {
        'char': (peewee.CharField, {'null': True, 'max_length': CHAR_MAX_LENGTH}),
        'text': (peewee.TextField, {'null': True}),
        'date': (peewee.DateTimeField, {'null': True}),
        'now': (peewee.DateTimeField, {'default': datetime.datetime.now}),
        'int': (peewee.IntegerField, {'null': True}),
        'bigint': (peewee.BigIntegerField, {'null': True}),
        'float': (peewee.FloatField, {'null': True}),
        'decimal': (peewee.DecimalField, {'null': True, 'max_digits': 20, 'decimal_places': 2}),
        'bool': (peewee.BooleanField, {'null': True})
    }
    type_aliases = {
        'longchar': 'char,max_length:2500'
    }
    field_overrides = {
        'auto': 'int',
        'bool': 'tinyint',
    }
    known_funcs = {
        'json': safe_dumps,
        'join': join,
        'count': len,
        'sum': sum,
        'min': min,
        'max': max,
        'avg': avg,
    }
    allowed_fieldargs = ['null', 'index', 'unique', 'primary_key',
        'max_length', 'max_digits', 'decimal_places', 'auto_round']
    allowed_fieldopts = ['column_name', 'default', 'collation']
    fieldopt_synonims = {'pk': 'primary_key', 'len': 'max_length',
        'digits': 'max_digits', 'decimal': 'decimal_places',
        'enc': 'encoding', 'charset': 'encoding', 'trunc': 'truncate',
        'name': 'column_name'}
    last_tender_id = None

    def __init__(self, config, args):
        db_class = config.get('server', 'class')
        if db_class in self.server_defaults:
            self.server_config.update(self.server_defaults[db_class])
        self.server_config.update(config.items('server'))
        self.client_config.update(config.items('client'))
        self.type_aliases.update(config.items('aliases'))
        self.general_config.update(config.items('general'))
        # update config from args
        self.update_config(args)
        # create lockfile
        if self.general_config['lockfile']:
            filename = self.general_config['lockfile']
            wait = int(self.general_config['lockwait'] or 0)
            write_pidfile(filename, wait)
        # setup warnings
        if self.general_config['warnings'] and not args.ignore:
            warnings.simplefilter(self.general_config['warnings'])
        # all sockets default timeout
        timeout = float(self.general_config['timeout'] or 0)
        if timeout > 0.001:
            socket.setdefaulttimeout(timeout)
        # create client
        safe_config = dict(self.client_config)
        safe_config.pop('key', None)
        safe_config.pop('auth', None)
        uri = safe_config.get('url', safe_config['host_url'])
        logger.info("Create API client %s", uri)
        logger.debug("Client config %s", safe_config)
        self.client = MyApiClient(self.client_config)
        # log connection config w/o password
        safe_config = dict(self.server_config)
        safe_config.pop('passwd', None)
        safe_config.pop('password', None)
        uri = "{}/{}".format(safe_config.get('host', 'file://'),
            safe_config.get('db', safe_config.get('database')))
        logger.info("Connect to database %s", uri)
        logger.debug("Database config %s", safe_config)
        # create database connection
        db_class = peewee.__dict__.get(self.server_config.pop('class'))
        self.db_init = self.server_config.pop('init', '').strip(' \'"')
        self.db_name = self.server_config.pop('db', None)
        self.db_ping = self.server_config.pop('ping', 0)
        self.force_encoding = self.server_config.pop('force_encoding', 0)
        if self.force_encoding:
            codecs.lookup(self.force_encoding)
        index_max_length = self.server_config.pop('index_max_length', 0)
        self.index_max_length = int(index_max_length)
        for param in ('port',):
            if param in self.server_config:
                self.server_config[param] = int(self.server_config[param])
        for param in ('connect_timeout', 'read_timeout', 'write_timeout'):
            if param in self.server_config:
                self.server_config[param] = float(self.server_config[param])
        if not self.db_name and self.server_config.get('database'):
            self.db_name = self.server_config.pop('database')
        self.database = db_class(self.db_name, **self.server_config)
        if self.db_init:
            self.database.execute_sql(self.db_init)
        # create model class
        self.create_models(config)
        # create cache model
        self.init_cache(config)

    def update_config(self, args):
        for key in ('lockfile', 'lockwait'):
            if getattr(args, key, None):
                self.general_config[key] = getattr(args, key)
        for key in ('offset', 'limit', 'resume', 'alive'):
            if getattr(args, key, None):
                self.client_config[key] = getattr(args, key)
        for key in ('no_cache', 'drop_cache', 'fill_cache', 'verbose', 'debug'):
            setattr(self, key, getattr(args, key, False))
        self.loglevel = logging.INFO if self.verbose else logging.DEBUG
        self.ignore_errors = args.ignore

    def init_cache(self, config):
        self.cache_model = None
        if self.no_cache or self.client_config['resume']:
            return
        if not config.has_section('cache'):
            return
        cache_config = dict(config.items('cache'))
        cache_table = cache_config.get('table')
        if not cache_table:
            return
        logger.info("Init cache table `%s`", cache_table)
        blob_type = cache_config.get('blob_type', 'BLOB')
        max_size = int(cache_config.get('max_size', 65530))
        CacheTendersModel.gzip_data.field_type = blob_type
        self.cache_model = CacheTendersModel
        self.cache_model._meta.database = self.database
        self.cache_model._meta.table_name = cache_table
        self.cache_max_size = max_size
        self.cache_hit_count = 0
        self.cache_miss_count = 0
        try:
            self.cache_model.select().get()
            cache_table_exists = True
        except CacheTendersModel.DoesNotExist:
            cache_table_exists = True
        except peewee.DatabaseError:
            cache_table_exists = False
            self.database.rollback()
        if self.drop_cache:
            logger.warning("Drop cache table `%s`", cache_table)
            self.cache_model.drop_table(safe=True)
            cache_table_exists = False
        if not cache_table_exists:
            logger.info("Create cache table `%s`", cache_table)
            self.cache_model.create_table(safe=True)

    @staticmethod
    def field_name(name):
        return name.replace('.', '_').replace('(', '_').replace(')', '').strip()

    def compare_table(self, table_name, fields):
        columns = self.database.get_columns(table_name)
        col_map = {c.name: c for c in columns}
        for col in columns:
            if col.name not in fields:
                raise KeyError("Table %s filed %s not found in config" % (table_name, col.name))
        for name in fields.keys():
            if name not in col_map:
                raise KeyError("Table %s filed %s not found in database" % (table_name, name))
            data_type = col_map[name].data_type.lower()
            field_type = fields[name].field_type.lower()
            if data_type == field_type:
                continue
            if data_type == self.field_overrides.get(field_type, ''):
                continue
            raise TypeError("Table %s filed %s type not euqal, re-create tables" % (table_name, name))

        logger.debug("Use existing table %s", table_name)

    def model_row_size(self, model_class):
        row_size = 0
        for field in model_class._meta.fields.values():
            if isinstance(field, peewee.CharField):
                row_size += 2 * field.max_length + 4
            elif isinstance(field, (peewee.IntegerField, peewee.BooleanField)):
                row_size += 4
            else:
                row_size += 8
        return row_size

    def create_table(self, model_class):
        with self.database.transaction():
            try:
                model_class.drop_table(safe=True)
                logger.warning("Drop table `%s`", model_class._meta.table_name)
            except peewee.DatabaseError:
                self.database.rollback()
        with self.database.transaction():
            logger.info("Create table `%s` (row size %d)", model_class._meta.table_name,
                self.model_row_size(model_class))
            model_class.create_table(safe=True)

    def init_model(self, table_name, table_schema, index_schema, filters):
        logger.log(self.loglevel, "Create model %s", table_name)
        if table_name in self.models:
            raise KeyError('Model %s already exists' % table_name)

        fields = collections.OrderedDict()
        parsed_schema = collections.OrderedDict()
        table_options = {'__name__': table_name}
        index_options = []
        filter_options = []
        has_primary_key = False

        for key, val in table_schema:
            if key.startswith('__'):
                if key == '__iter__':
                    table_options['__path__'] = val
                    val = val.split('.')
                table_options[key] = val
                continue
            name = self.field_name(key)
            if name in fields:
                raise KeyError('Model %s field %s already exists' % (table_name, name))
            logger.debug("+ %s %s", name, val)
            opts = [s.strip() for s in val.split(',')]
            # [table:model_name]
            # field = type,option,option:value
            ftype = opts[0]
            if ftype in self.type_aliases:
                opts[0] = self.type_aliases[ftype]
                opts = [s.strip() for s in ','.join(opts).split(',')]
                ftype = opts[0]
            if ftype not in self.field_types:
                raise TypeError("Unknown type '%s' for field '%s'" % (ftype, key))
            field_class, fieldopts = self.field_types.get(ftype)
            extraopts = None
            if len(opts) > 1:
                fieldopts = dict(fieldopts)
                extraopts = dict()
                for opt in opts[1:]:
                    val = 1
                    if ':' in opt:
                        opt, val = opt.split(':', 1)
                    if opt == 'not_null':
                        opt = 'null'
                        val = 0
                    if opt in self.fieldopt_synonims:
                        opt = self.fieldopt_synonims[opt]
                    if opt == 'primary_key':
                        has_primary_key = True
                    if opt == 'encoding':
                        codecs.lookup(val)
                        extraopts[opt] = val
                    elif opt in ('truncate', 'ignore'):
                        extraopts[opt] = int(val)
                    elif opt in self.allowed_fieldargs:
                        fieldopts[opt] = int(val)
                    elif opt in self.allowed_fieldopts:
                        fieldopts[opt] = val
                    else:
                        raise ValueError("Unknown option '%s' for field '%s' table [%s]"
                            % (opt, key, table_name))
            fields[name] = field_class(**fieldopts)
            # parse field path
            funcs = key.replace(')', '').split('(')
            chain = funcs.pop().split('.')
            for f in funcs:
                if f not in self.known_funcs:
                    raise KeyError("Unknown function '%s' in field '%s' table [%s]"
                        % (f, key, table_name))
            parsed_schema[name] = (chain, funcs, ftype, fieldopts, extraopts)

        # parse indexes
        for key, val in index_schema:
            index_fields = []
            unique = False
            if val.startswith('unique:'):
                val = val.replace('unique:', '')
                unique = True
            for name in val.split(','):
                # check for filed(length)
                if '(' in name:
                    name, index_max_length = name.replace(')', '').split('(', 1)
                    index_max_length = int(index_max_length or self.index_max_length)
                else:
                    index_max_length = self.index_max_length
                name = self.field_name(name)
                if name not in fields:
                    raise KeyError('Model %s index %s field %s not found' % (table_name, key, name))
                ftype = parsed_schema[name][2]
                if ftype in ('char', 'text') and index_max_length:
                    name = "%s(%d)" % (name, index_max_length)
                index_fields.append(name)
            logger.debug("+ INDEX %s ON %s %s", key, index_fields, unique)
            index_options.append((key, index_fields, unique))

        # parse filters
        for key, val in filters:
            if "__" in key:
                key, op = key.split("__", 1)
            else:
                op = "eq"
            if op not in ('eq', 'ne', 'lt', 'lte', 'gt', 'gte', 'in', 'notin',
                          'between', 'empty', 'notempty', 'isnull', 'notnull'):
                raise ValueError("Unknown condition '%s' for %s in %s" % (op, key, table_name))
            if op in ('in', 'notin', 'between'):
                val = val.split(',')
            if op in ('between', 'notbetween') and len(val) != 2:
                raise ValueError("%s filter require exact 2 values for '%s' in [%s]" % (op, key,
                                 table_name))
            # convert value by field type
            name = self.field_name(key)
            conv = None
            if name in parsed_schema:
                # (chain, funcs, ftype, fieldopts, extraopts)
                ftype = parsed_schema[name][2]
                if ftype in ('int', 'bigint'):
                    conv = int
                elif ftype in ('float', 'decimal'):
                    conv = float
                elif ftype in ('char', 'longchar', 'text'):
                    conv = unicode
            if conv:
                try:
                    if isinstance(val, list):
                        val = map(conv, val)
                    else:
                        val = conv(val)
                except Exception as e:
                    raise ValueError("Could not convert filter %s to %s: %s", name, ftype, repr(e))
            # parse field path
            funcs = key.replace(')', '').split('(')
            chain = funcs.pop().split('.')
            logger.debug("* FILTER %s %s %s", key, op, str(val))
            filter_options.append((key, chain, funcs, op, val))

        if not has_primary_key:
            fields['pk_id'] = peewee.AutoField(primary_key=True)
        class_name = str("%s__Model" % table_name.title())
        model_class = type(class_name, (BaseTendersModel,), fields)
        model_class._meta.index_max_length = self.index_max_length
        model_class._meta.filter_options = filter_options
        model_class._meta.index_options = index_options
        model_class._meta.table_options = table_options
        model_class._meta.table_schema = parsed_schema
        model_class._meta.database = self.database
        model_class._meta.table_name = table_name
        self.models[table_name] = model_class
        if self.client_config['resume']:
            self.compare_table(table_name, fields)
        else:
            self.create_table(model_class)
        # check for main model
        if 'dateModified' in fields and '__iter__' not in table_options:
            if not self.main_model:
                self.main_model = model_class
        elif '__main__' in table_options:
            if self.main_model:
                raise ValueError('Main model defined twice')
            self.main_model = model_class

    def create_models(self, config):
        self.models = collections.OrderedDict()
        self.main_model = None
        for section in config.sections():
            if section.startswith('table:'):
                table_schema = config.items(section)
                table, name = section.split(':', 2)
                filter_section = 'filter:' + name
                filters = []
                if config.has_section(filter_section):
                    filters = config.items(filter_section)
                index_section = 'index:' + name
                index_schema = []
                if config.has_section(index_section):
                    index_schema = config.items(index_section)
                self.init_model(name, table_schema, index_schema, filters)

        self.sorted_models = sorted(self.models.values(),
            key=lambda m: len(m._meta.table_options.get('__iter__', [])))

        if self.client_config['resume'] and not self.main_model:
            raise ValueError('Main model is required for resume mode')
        if not self.main_model:
            logger.warning('Main model is not defined in config')

    def create_indexes(self):
        for model_class in self.sorted_models:
            index_options = model_class._meta.index_options
            for name, fields, unique in index_options:
                unique_str = 'UNIQUE ' if unique else ''
                table_name = model_class.model_name()
                name = "%s_%s" % (table_name, name)
                logger.info("CREATE %sINDEX %s ON %s (%s);", unique_str, name,
                    table_name, ', '.join(fields))
                with self.database.transaction():
                    try:
                        table = model_class._meta.table
                        index = peewee.Index(name, table, fields, unique, safe=True)
                        self.database.execute(index)
                    except Exception as e:
                        message = "%s on create_index %s %s %s" % (str(e),
                            name, table_name, str(fields))
                        if self.ignore_errors:
                            logger.error("%s: %s (ignored)", type(e).__name__, message)
                            self.database.rollback()
                            return
                        logger.debug("%s: %s", repr(e), message)
                        six.reraise(type(e), type(e)(message), sys.exc_info()[2])
                        raise

    def apply_func(self, fn, data):
        return self.known_funcs[fn](data)

    def field_value(self, chain, funcs, data):
        for key in chain:
            if isinstance(data, list):
                res = list()
                for item in data:
                    val = item.get(key)
                    # make val to be always a list
                    if not isinstance(val, list):
                        val = [val]
                    # and solve the problem only for list
                    for vi in val:
                        if isinstance(vi, dict):
                            vi['parent'] = item
                        if vi is not None:
                            res.append(vi)
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
        return iso8601.parse_date(value).replace(tzinfo=None)

    def process_model_item(self, model_class, data):
        fields = dict()
        table_schema = model_class._meta.table_schema
        for name in table_schema:
            # (chain, funcs, ftype, fieldopts, extraopts)
            chain, funcs, ftype, fieldopts, extraopts = table_schema[name]
            try:
                value = self.field_value(chain, funcs, data)
                if isinstance(value, list):
                    value = value[0] if len(value) else None
                if value is None:
                    continue
                if ftype in ('char', 'longchar', 'text'):
                    value = unicode(value)
                    if extraopts and 'encoding' in extraopts:
                        value = force_encoding(value, extraopts['encoding'])
                    elif self.force_encoding:
                        value = force_encoding(value, self.force_encoding)
                    if extraopts and 'truncate' in extraopts:
                        truncate = extraopts['truncate']
                        if len(value) > truncate:
                            value = value[:truncate]
                if ftype == 'char':
                    max_length = fieldopts.get('max_length', self.CHAR_MAX_LENGTH)
                    ignore = extraopts and 'ignore' in extraopts
                    if len(value) > max_length and not ignore:
                        raise ValueError("Value is too long, use text or truncate" % name)
                if ftype == 'date':
                    value = self.parse_iso_datetime(value)
                fields[name] = value
            except Exception as e:
                message = "%s on field '%s' table [%s] id=%s" % (str(e),
                    name, model_class.model_name(), data.get('id'))
                if extraopts and 'ignore' in extraopts:
                    logger.error("%s: %s (ignored)", type(e).__name__, message)
                else:
                    logger.debug("%s: %s", repr(e), message)
                    six.reraise(type(e), type(e)(message), sys.exc_info()[2])
                    raise

        item = model_class(**fields)
        item.save(force_insert=True)

    def process_signle_filter(self, filter_opts, data):
        key, chain, funcs, op, opval = filter_opts
        value = self.field_value(chain, funcs, data)
        # 'eq', 'ne', 'lt', 'lte', 'gt', 'gte', 'in', 'notin',
        # 'between', 'empty', 'notempty', 'isnull', 'notnull'
        if op == 'eq':
            return value == opval
        if op == 'ne':
            return value != opval
        if op == 'lt':
            return value < opval
        if op == 'lte':
            return value <= opval
        if op == 'gt':
            return value > opval
        if op == 'gte':
            return value >= opval
        if op == 'in':
            return value in opval
        if op == 'notin':
            return value not in opval
        if op == 'between':
            return opval[0] <= value <= opval[1]
        if op == 'empty':
            return not value
        if op == 'notempty':
            return value or True
        if op == 'isnull':
            return value is None
        if op == 'notnull':
            return value is not None
        raise ValueError("Unknwon filter condition %s" % op)

    def process_filters(self, model_class, data):
        if not model_class:
            return True
        for opts in model_class._meta.filter_options:
            try:
                if not self.process_signle_filter(opts, data):
                    return False
            except Exception as e:
                message = "%s on table [%s] filter %s id=%s" % (str(e),
                    str(model_class.model_name()), str(opts), data.get('id'))
                logger.debug("%s: %s", repr(e), message)
                six.reraise(type(e), type(e)(message), sys.exc_info()[2])
                raise
        return True

    def process_model_data(self, model_class, data):
        if not self.process_filters(model_class, data):
            logger.log(self.loglevel, "Filter %s %s", model_class._meta.table_name,
                data.get('id', '-'))
            return
        table_options = model_class._meta.table_options
        if table_options.get('__iter__'):
            iter_name = table_options['__iter__']
            iter_path = table_options['__path__']
            iter_list = self.field_value(iter_name, [], data)
            root_name = table_options.get('__root__', 'root')
            if iter_list:
                for item in iter_list:
                    logger.debug("+ Child %s %s", item.get('id', '-'), iter_path)
                    item[root_name] = data
                    self.process_model_item(model_class, item)
        else:
            return self.process_model_item(model_class, data)

    def delete_model_data(self, model_class, tender):
        table_options = model_class._meta.table_options
        if table_options.get('__iter__'):
            root_name = table_options.get('__root__', 'root')
            root_id = getattr(model_class, '%s_id' % root_name)
            deleted = model_class.delete().where(root_id == tender.id).execute()
            logger.debug("Delete child %s %d rows", table_options['__path__'], deleted)
        else:
            deleted = model_class.delete().where(model_class.id == tender.id).execute()
            logger.debug("Delete root %s %d row", model_class.model_name(), deleted)

    def tender_exists(self, tender, delete=False):
        try:
            found = (self.main_model
                .select(self.main_model.id, self.main_model.dateModified)
                .where(self.main_model.id == tender.id).get())
            if found.dateModified == tender.dateModified:
                return True
        except self.main_model.DoesNotExist:
            return None

        if found and delete:
            logger.debug("Delete %s %s", found.id, found.dateModified)
            self.total_deleted += 1
            with self.database.transaction():
                for model_class in reversed(self.sorted_models):
                    self.delete_model_data(model_class, tender)
            found = False
        return found

    def process_tender(self, tender):
        self.last_tender_id = tender.id
        if self.client_config['resume']:
            if self.tender_exists(tender, delete=True):
                logger.log(self.loglevel, "Exists %s %s", tender.id, tender.dateModified)
                self.total_exists += 1
                return
        elif self.client_config['ignore']:
            if self.tender_exists(tender, delete=False):
                logger.log(self.loglevel, "Exists %s %s", tender.id, tender.dateModified)
                self.total_exists += 1
                return
        data = self.get_from_cache(tender)
        if not data:
            data = self.client.get(tender.id)
            self.save_to_cache(tender, data)
        if self.fill_cache:
            logger.log(self.loglevel, "Store %s %s", tender.id, tender.dateModified)
            return
        if not self.process_filters(self.main_model, data):
            logger.log(self.loglevel, "Filter %s %s", tender.id, tender.dateModified)
            return
        logger.log(self.loglevel, "Process %s %s", tender.id, tender.dateModified)
        with self.database.transaction():
            try:
                for model_class in self.sorted_models:
                    self.process_model_data(model_class, data)
                self.total_inserted += 1
            except Exception as e:
                message = "%s table [%s] id=%s" % (str(e),
                    model_class.model_name(), data.get('id'))
                if self.ignore_errors:
                    logger.error("%s: %s (ignored)", type(e).__name__, message)
                    self.database.rollback()
                    return
                logger.debug("%s: %s", repr(e), message)
                six.reraise(type(e), type(e)(message), sys.exc_info()[2])
                raise
        self.last_tender_id = None
        return True

    def ping_db_connection(self):
        conn = self.database.connection()
        if hasattr(conn, 'ping'):
            conn.ping(True)

    def get_from_cache(self, tender):
        if not self.cache_model:
            return None
        total_count = self.cache_hit_count + self.cache_miss_count
        if total_count > 0 and total_count % 100000 == 0:
            usage = 100.0 * self.cache_hit_count / total_count
            logger.info("Cache hit %d miss %d usage %1.0f%%",
                self.cache_hit_count, self.cache_miss_count, usage)
        try:
            item = self.cache_model.get(
                self.cache_model.tender_id == tender.id,
                self.cache_model.dateModified == tender.dateModified)
        except self.cache_model.DoesNotExist:
            self.cache_miss_count += 1
            return None
        self.cache_hit_count += 1
        json_data = zlib.decompress(item.gzip_data)
        return munch.munchify(json.loads(json_data))

    def save_to_cache(self, tender, data):
        if not self.cache_model:
            return False
        json_data = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
        if isinstance(json_data, six.text_type):
            json_data = json_data.encode('utf-8')
        gzip_data = zlib.compress(json_data)
        if len(gzip_data) > self.cache_max_size:
            logger.warning("Too big for cache %s size %d gzip %d",
                           tender.id, len(json_data), len(gzip_data))
            return False
        cache_item = self.cache_model(tender_id=tender.id,
                dateModified=tender.dateModified,
                gzip_data=gzip_data)
        try:
            field_dict = cache_item.__data__.copy()
            cache_item.insert(**field_dict).on_conflict(
                preserve=('dateModified', 'gzip_data')).execute()
        except peewee.IntegrityError:
            cache_item.save()
        return True

    def onpreload(self, count, last):
        logger.log(self.loglevel, "Preload %d last %s", count, last.get('dateModified', ''))
        if self.db_ping:
            self.ping_db_connection()

    def log_total(self, last_date):
        insert_new = self.total_inserted - self.total_deleted
        logger.info("Total %d new %d upd %d last %s", self.total_listed,
                    insert_new, self.total_deleted, last_date)

    def run(self):
        feed = self.client_config.get('feed', '')
        offset = self.client_config.get('offset', '')
        limit = int(self.client_config.get('limit') or 0)
        self.total_listed = 0
        self.total_exists = 0
        self.total_processed = 0
        self.total_inserted = 0
        self.total_deleted = 0
        last_total = 0
        tender = None
        stop = False

        if feed:
            logger.info("Set client feed to %s", feed)

        if offset:
            offset = relative_offset(offset)
            logger.info("Set client offset to %s", offset)
            self.client.set_offset(offset)

        if limit:
            logger.info("Set client limit to %d", limit)
            self.client.set_limit(limit)

        while not stop:
            tenders_list = self.client.preload(feed=feed, callback=self.onpreload)

            if self.db_ping:
                self.ping_db_connection()

            if not tenders_list:
                if self.client_config['alive']:
                    time.sleep(10)
                    continue
                logger.info("No more records.")
                break

            for tender in tenders_list:
                if self.total_listed - last_total >= 10000:
                    last_total = self.total_listed
                    self.log_total(tender.dateModified)

                self.total_listed += 1

                if offset and offset > tender.dateModified:
                    logger.log(self.loglevel, "Ignore %s %s", tender.id, tender.dateModified)
                    continue

                if self.process_tender(tender):
                    self.total_processed += 1

                if limit and self.total_processed >= limit:
                    logger.info("Reached limit %d records, stop.", limit)
                    stop = True
                    break

        self.log_total(tender.dateModified if tender else '-')

        if not self.client_config['resume']:
            self.create_indexes()

    def run_debug(self):
        with open('debug/tender.json') as f:
            tender = json.load(f)
        data = munch.munchify(tender['data'])
        for model_class in self.sorted_models:
            self.process_model_data(model_class, data)

    def close(self):
        self.database.close()
        self.client.close()


def formatwarning(message, category, filename, lineno, line=None):
    if len(filename) > 30:
        filename = "..." + filename[-27:]
    if transfuse_app and transfuse_app.last_tender_id:
        message = "%s id=%s" % (message, transfuse_app.last_tender_id)
    return "%s:%s: %s: %s" % (filename, lineno, category.__name__, message)


def run_app(args):
    global transfuse_app
    config = MyConfigParser(allow_no_value=True)
    for inifile in args.config:
        config.test(inifile)
        config.read(inifile)

    app = TendersToSQL(config, args)
    transfuse_app = app
    try:
        app.run()
    finally:
        app.close()


def term_handler(signum, frame):
    logger.warning("Signal %d received", signum)
    sys.exit(1)


def main():
    description = "Prozorro API to SQL database converter v%s" % __version__
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('config', nargs='+', help='ini file(s)')
    parser.add_argument('-o', '--offset', type=str, help='client api offset')
    parser.add_argument('-l', '--limit', type=int, help='client api limit')
    parser.add_argument('-a', '--alive', action='store_true', help='run infinitely')
    parser.add_argument('-r', '--resume', action='store_true', help='dont drop table')
    parser.add_argument('-i', '--ignore', action='store_true', help='ignore errors')
    parser.add_argument('-x', '--drop-cache', action='store_true', help="clear cache")
    parser.add_argument('-f', '--fill-cache', action='store_true', help="only save to cache")
    parser.add_argument('-n', '--no-cache', action='store_true', help="don't use cache")
    parser.add_argument('-v', '--verbose', action='store_true', help='verbose logging')
    parser.add_argument('-d', '--debug', action='store_true', help='print traceback')
    parser.add_argument('-p', '--pause', action='store_true', help='pause before exit')
    parser.add_argument('--lockfile', type=str, help='prevent second start')
    parser.add_argument('--lockwait', type=int, default=3600, help='wait for lockfile (seconds)')
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, term_handler)

    for inifile in args.config:
        logging.config.fileConfig(inifile)
        logging.captureWarnings(True)
        warnings.formatwarning = formatwarning
        break

    if args.debug:
        logger.setLevel(logging.DEBUG)

    logger.info(description)

    exit_code = 0

    try:
        run_app(args)
    except KeyboardInterrupt:
        logger.error("Keyboard Interrupt")
        pass
    except Exception as e:
        if args.debug:
            logger.exception("Got Exception")
        else:
            logger.error(repr(e))
        exit_code = 1

    if args.pause:
        sys.stdout.write("Press Enter to continue...")
        sys.stdout.flush()
        input()

    logger.debug("Exit with code %d", exit_code)

    return exit_code


if __name__ == '__main__':
    sys.exit(main())
