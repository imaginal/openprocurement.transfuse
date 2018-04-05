#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import time
import zlib
import socket
import logging
import logging.config
import simplejson as json
import peewee

from argparse import ArgumentParser
from ConfigParser import RawConfigParser

from munch import munchify
from iso8601 import parse_date
from datetime import datetime
from restkit.client import get_session
from restkit.errors import ResourceError, ResourceNotFound
from openprocurement_client.client import APIBaseClient

# fix for py2exe
from socketpool import backend_thread
unused_Queue = backend_thread.PriorityQueue


__version__ = '2.1.2'

logger = logging.getLogger('transfuse')

CHAR_MAX_LENGTH = 250
LONGCHAR_MAX_LENGTH = 1000


class MyConfigParser(RawConfigParser):
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


class MyApiClient(APIBaseClient):
    def __init__(self, key, config):
        params = {'limit': 1000, 'mode': ''}
        if config['mode'] in ('test', '_all_'):
            params['mode'] = config['mode']
        timeout = float(config.get('timeout', 0))
        if timeout > 0.01:
            socket.setdefaulttimeout(timeout)
        APIBaseClient.__init__(
            self, key, config['host_url'], config['api_version'], config['resource'], params,
            timeout=timeout)
        self.headers['User-Agent'] = "Transfuse/%s %s" % (__version__, config['user_agent'])
        self.limit_preload = int(config.get('preload', 0))
        self.api_version = config['api_version']
        self.log_cookie()

    def log_cookie(self):
        logger.info("Cookie: %s", self.headers.get('Cookie'))

    def request_cookie(self):
        self.head('/api/{}/spore'.format(self.api_version))
        self.log_cookie()

    def preload_tenders(self, feed='', limit=0, callback=None):
        preload_items = []
        items = True
        if not self.headers.get('Cookie', None):
            self.request_cookie()
        while items:
            items = self.get_tenders(feed=feed)
            if items:
                preload_items.extend(items)
            if self.limit_preload >= 0 and self.limit_preload < len(preload_items):
                break
            if items and callback:
                callback(len(preload_items), items[-1])
        return preload_items

    def log_request_error(self, msg, e=''):
        logger.error("%s error %s", msg, str(e))
        logger.error("Request params %s headers %s", self.params, self.headers)
        if e and getattr(e, 'response', None):
            logger.error("Response status %s headers %s", e.response.status_int, e.response.headers)

    def get_tenders(self, params={}, feed='changes'):
        params['feed'] = feed
        for i in range(5):
            try:
                self._update_params(params)
                response = self.get(
                    self.prefix_path,
                    params_dict=self.params)
                if response.status_int == 200:
                    tender_list = munchify(json.loads(response.body_string()))
                    self._update_params(tender_list.next_page)
                    return tender_list.data

                logger.warning("get_tenders response %d headers %s",
                    response.status_int, str(response.headers))

            except ResourceNotFound:
                self.params.pop('offset', '')
                raise
            except (socket.error, ResourceError) as e:
                self.log_request_error('get_tenders', e)
                time.sleep(10 * i + 10)

        raise ResourceError("Maximum retry reached")

    def get_tender(self, tender_id):
        for i in range(5):
            try:
                if not self.headers.get('Cookie', None):
                    self.request_cookie()
                return self._get_resource_item('{}/{}'.format(self.prefix_path, tender_id))

            except (socket.error, ResourceError) as e:
                self.log_request_error('get_tender/%s' % tender_id, e)
                if i > 1:
                    self.headers.pop('Cookie', None)
                time.sleep(10 * i + 10)

        raise ResourceError("Maximum retry reached")


class BaseTendersModel(peewee.Model):
    class Meta:
        pass

    @classmethod
    def model_name(klass):
        return klass._meta.db_table


class CacheTendersModel(BaseTendersModel):
    tender_id = peewee.CharField(primary_key=True)
    dateModified = peewee.CharField()
    gzip_data = peewee.BlobField()


def avg(data):
    if data:
        return sum(data) / len(data)


class TendersToSQL(object):
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
        'resume': False,
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
        'longchar': (peewee.CharField, {'null': True, 'max_length': LONGCHAR_MAX_LENGTH}),
        'text': (peewee.TextField, {'null': True}),
        'date': (peewee.DateTimeField, {'null': True}),
        'now': (peewee.DateTimeField, {'default': datetime.now}),
        'int': (peewee.IntegerField, {'null': True}),
        'bigint': (peewee.BigIntegerField, {'null': True}),
        'float': (peewee.FloatField, {'null': True}),
        'decimal': (peewee.DecimalField, {'null': True, 'max_digits': 20, 'decimal_places': 2}),
        'bool': (peewee.BooleanField, {'null': True})
    }
    field_overrides = {
        'primary_key': 'int',
        'bool': 'tinyint',
    }
    known_funcs = {
        'count': len,
        'sum': sum,
        'min': min,
        'max': max,
        'avg': avg,
    }
    allowed_fieldopts = ['null', 'index', 'unique', 'primary_key']

    def __init__(self, config, args):
        db_class = config.get('server', 'class')
        if db_class in self.server_defaults:
            self.server_config.update(self.server_defaults[db_class])
        self.server_config.update(config.items('server'))
        self.client_config.update(config.items('client'))
        # update config from args
        self.update_config(args)
        # create client
        api_key = self.client_config.pop('key')
        logger.info("Create client %s", self.client_config)
        self.client = MyApiClient(api_key, self.client_config)
        # log connection config w/o password
        safe_config = dict(self.server_config)
        safe_config.pop('passwd', None)
        safe_config.pop('password', None)
        logger.info("Connect server %s", safe_config)
        # create database connection
        db_class = peewee.__dict__.get(self.server_config.pop('class'))
        self.db_init = self.server_config.pop('init', '').strip(' \'"')
        self.db_name = self.server_config.pop('db', None)
        self.db_ping = self.server_config.pop('ping', 0)
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
        for key in ('offset', 'limit', 'resume'):
            if getattr(args, key, None):
                self.client_config[key] = getattr(args, key)
        for key in ('no_cache', 'drop_cache', 'fill_cache'):
            setattr(self, key, getattr(args, key, False))
        self.ignore_errors = args.ignore

    def init_cache(self, config):
        self.cache_model = None
        if self.no_cache or self.client_config['resume']:
            return
        if not config.has_option('cache', 'table'):
            return
        cache_table = config.get('cache', 'table')
        if not cache_table:
            return
        logger.info("Init cache table `%s`", cache_table)
        self.cache_model = CacheTendersModel
        self.cache_model._meta.database = self.database
        self.cache_model._meta.db_table = cache_table
        self.cache_max_size = 0xfff0
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
        if self.drop_cache and cache_table_exists:
            logger.warning("Drop cache table `%s`", cache_table)
            self.cache_model.drop_table()
            cache_table_exists = False
        if not cache_table_exists:
            logger.info("Create cache table `%s`", cache_table)
            self.cache_model.create_table()

    @staticmethod
    def field_name(name):
        return name.replace('.', '_').replace('(', '_').replace(')', '').strip()

    def compare_table(self, table_name, fields):
        compiler = self.database.compiler()
        columns = self.database.get_columns(table_name)
        col_map = {c.name: c for c in columns}
        for col in columns:
            if col.name not in fields:
                raise KeyError("Table %s filed %s not found in config" % (table_name, col.name))
        for name in fields.keys():
            if name not in col_map:
                raise KeyError("Table %s filed %s not found in database" % (table_name, name))
            data_type = col_map[name].data_type
            db_field = fields[name].db_field
            if data_type == db_field:
                continue
            if data_type == compiler.get_column_type(db_field).lower():
                continue
            if data_type == self.field_overrides.get(db_field, ''):
                continue
            raise TypeError("Table %s filed %s type not euqal, re-create tables" % (table_name, name))

        logger.info("Use existing table `%s`", table_name)

    def create_table(self, model_class):
        logger.warning("Drop & Create table `%s`", model_class._meta.db_table)
        with self.database.transaction():
            try:
                model_class.select().count()
                model_class.drop_table(fail_silently=True)
            except peewee.DatabaseError:
                self.database.rollback()
        with self.database.transaction():
            model_class.create_table()

    def init_model(self, table_name, table_schema, index_schema):
        logger.info("Create model %s", table_name)
        if table_name in self.models:
            raise IndexError('Model %s already exists' % table_name)

        fields = dict()
        parsed_schema = list()
        table_options = {'__name__': table_name}
        index_options = []
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
                raise IndexError('Model %s field %s already exists' % (table_name, name))
            logger.debug("+ %s %s", name, val)
            opts = [s.strip() for s in val.split(',')]
            # [table:model_name]
            # field = type,flags,max_length
            if opts[0] not in self.field_types:
                raise TypeError("Unknown type '%s' for field '%s'" % (opts[0], key))
            fieldtype, fieldopts = self.field_types.get(opts[0])
            if len(opts) > 1:
                if opts[1] not in self.allowed_fieldopts:
                    raise ValueError("Unknown option '%s' for field '%s'" % (opts[1], key))
                fieldopts = dict(fieldopts)
                fieldopts[ opts[1] ] = True
                if opts[1] == 'primary_key':
                    has_primary_key = True
            if len(opts) > 2:
                fieldopts['max_length'] = int(opts[2])
            fields[name] = fieldtype(**fieldopts)
            # parse field path
            funcs = key.replace(')', '').split('(')
            chain = funcs.pop().split('.')
            for f in funcs:
                if f not in self.known_funcs:
                    raise IndexError("Unknown function '%s' in field '%s'" % (f, key))
            parsed_schema.append((name, chain, funcs, opts[0]))

        for key, val in index_schema:
            index_fields = []
            unique = False
            if val.startswith('unique:'):
                val = val.replace('unique:', '')
                unique = True
            for name in val.split(','):
                name = self.field_name(name)
                if name not in fields:
                    raise IndexError('Model %s index %s field %s not found' % (table_name, key, name))
                index_fields.append(name)
            logger.debug("+ INDEX %s ON %s %s", key, index_fields, unique)
            index_options.append((key, index_fields, unique))

        if not has_primary_key:
            fields['pk_id'] = peewee.PrimaryKeyField(primary_key=True)
        class_name = "%sModel" % table_name.title()
        model_class = type(class_name, (BaseTendersModel,), fields)
        model_class._meta.index_options = index_options
        model_class._meta.table_options = table_options
        model_class._meta.table_schema = parsed_schema
        model_class._meta.database = self.database
        model_class._meta.db_table = table_name
        self.models[table_name] = model_class
        if self.client_config['resume']:
            self.compare_table(table_name, fields)
        else:
            self.create_table(model_class)
        # check for main model
        if 'dateModified' in fields and '__iter__' not in table_options:
            self.main_model = model_class

    def create_models(self, config):
        self.models = dict()
        self.main_model = None
        for section in config.sections():
            if section.startswith('table:'):
                table_schema = config.items(section)
                table, name = section.split(':', 2)
                index_section = 'index:' + name
                index_schema = []
                if config.has_section(index_section):
                    index_schema = config.items(index_section)
                self.init_model(name, table_schema, index_schema)

        self.sorted_models = sorted(self.models.values(),
            key=lambda m: len(m._meta.table_options.get('__iter__', [])))

        if self.client_config['resume'] and not self.main_model:
            raise ValueError('Main model is required for resume mode')

    def create_indexes(self):
        for model_class in self.sorted_models:
            index_options = model_class._meta.index_options
            for name, fields, unique in index_options:
                unique_str = 'UNIQUE ' if unique else ''
                logger.info("CREATE %sINDEX %s ON %s (%s);", unique_str, name,
                    model_class.model_name(), ', '.join(fields))
                with self.database.transaction():
                    self.database.create_index(model_class, fields, unique)

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
        return parse_date(value).replace(tzinfo=None)

    def process_model_item(self, model_class, data):
        table_schema = model_class._meta.table_schema
        fields = dict()
        for field_info in table_schema:
            name, chain, funcs, ftype = field_info
            try:
                value = self.field_value(chain, funcs, data)
                if isinstance(value, list):
                    value = value[0] if len(value) else None
                if value is None:
                    continue
                if ftype in ['char', 'longchar', 'text']:
                    value = unicode(value)
                if ftype == 'char' and len(value) > CHAR_MAX_LENGTH:
                    raise ValueError("Value too long, use longchar or text")
                if ftype == 'longchar' and len(value) > LONGCHAR_MAX_LENGTH:
                    value = value[:LONGCHAR_MAX_LENGTH]
                if ftype == 'date':
                    value = self.parse_iso_datetime(value)
                fields[name] = value
            except Exception as e:
                message = "%s on model [%s] field %s itemID=%s" % (str(e),
                    str(model_class.model_name()), name, data.get('id'))
                raise type(e), type(e)(message), sys.exc_info()[2]
        item = model_class(**fields)
        item.save(force_insert=True)

    def process_model_data(self, model_class, data):
        table_options = model_class._meta.table_options
        if table_options.get('__iter__'):
            iter_name = table_options['__iter__']
            iter_path = table_options['__path__']
            iter_list = self.field_value(iter_name, [], data)
            root_name = table_options.get('__root__', 'root')
            if iter_list:
                for item in iter_list:
                    logger.info("+ Child %s %s", item.get('id', '-'), iter_path)
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

    def process_tender(self, tender):
        if self.db_ping:
            self.ping_db_connection()
        if self.client_config['resume'] and self.tender_exists(tender, delete=True):
            logger.debug("Exists %s %s", tender.id, tender.dateModified)
            return
        logger.info("Process %s %s", tender.id, tender.dateModified)
        data = self.get_from_cache(tender)
        if not data:
            data = self.client.get_tender(tender.id)['data']
            self.save_to_cache(tender, data)
        if self.fill_cache:
            return
        with self.database.transaction():
            try:
                for model_class in self.sorted_models:
                    self.process_model_data(model_class, data)
                self.total_inserted += 1
            except Exception as e:
                message = str(e) + " rootID=%s" % data.get('id')
                if self.ignore_errors:
                    self.database.rollback()
                    logger.error(message)
                    return
                raise type(e), type(e)(message), sys.exc_info()[2]

    def ping_db_connection(self):
        conn = self.database.get_conn()
        if hasattr(conn, 'ping'):
            conn.ping(True)

    def get_from_cache(self, tender):
        if not self.cache_model:
            return None
        total_count = self.cache_hit_count + self.cache_miss_count
        if total_count > 0 and total_count % 1000 == 0:
            usage = 100.0 * self.cache_hit_count / total_count
            logger.info("Cache hit %d miss %d usage %1.0f %%",
                self.cache_hit_count, self.cache_miss_count, usage)
        try:
            item = self.cache_model.get(
                self.cache_model.tender_id == tender.id,
                self.cache_model.dateModified == tender.dateModified)
        except self.cache_model.DoesNotExist:
            self.cache_miss_count += 1
            return None
        self.cache_hit_count += 1
        return munchify(json.loads(zlib.decompress(item.gzip_data)))

    def save_to_cache(self, tender, data):
        if not self.cache_model:
            return False
        gzip_data = zlib.compress(json.dumps(data))
        if len(gzip_data) > self.cache_max_size:
            logger.warning("Too big for cache %s", tender.id)
            return False
        cache_item = self.cache_model(tender_id=tender.id,
                dateModified=tender.dateModified,
                gzip_data=gzip_data)
        try:
            cache_item.save(force_insert=True)
        except peewee.IntegrityError:
            cache_item.save()
        return True

    def onpreload(self, count, last):
        logger.info("Preload %d last %s", count, last.get('dateModified', ''))

    def log_total(self, last_date):
        insert_new = self.total_inserted - self.total_deleted
        ignored = self.total_listed - self.total_processed
        logger.info("Total %d ignore %d new %d upd %d last %s", self.total_listed,
                    ignored, insert_new, self.total_deleted, last_date)

    def run(self):
        feed = self.client_config.get('feed', '')
        offset = self.client_config.get('offset', '')
        limit = int(self.client_config.get('limit') or 0)
        self.total_listed = 0
        self.total_processed = 0
        self.total_inserted = 0
        self.total_deleted = 0
        last_date = ''
        last_total = 0

        if offset:
            self.client.params['offset'] = offset

        if limit and self.client.limit_preload > limit:
            self.client.limit_preload = limit

        tenders_list = True

        while tenders_list:
            tenders_list = self.client.preload_tenders(feed=feed, callback=self.onpreload)

            if not tenders_list:
                logger.info("No more records.")
                break

            for tender in tenders_list:
                if last_date < tender.dateModified[:10] or self.total_listed - last_total >= 10000:
                    last_date = tender.dateModified[:10]
                    last_total = self.total_listed
                    self.log_total(last_date)

                self.total_listed += 1

                if offset and offset > tender.dateModified:
                    logger.debug("Ignore %s %s", tender.id, tender.dateModified)
                    continue

                self.process_tender(tender)
                self.total_processed += 1

                if limit and self.total_processed >= limit:
                    logger.info("Reached limit %d records, stop.", limit)
                    tenders_list = False
                    break

        self.log_total(last_date)

        if not self.client_config['resume']:
            self.create_indexes()

    def run_debug(self):
        with open('debug/tender.json') as f:
            tender = json.load(f)
        data = munchify(tender['data'])
        for model_class in self.sorted_models:
            self.process_model_data(model_class, data)

    def close_client(self):
        self.client.client._pool.release_all()
        del self.client


def create_socket_pool(backend="thread"):
    get_session(backend, reap_connections=False)


def shutdown_sockets(backend="thread"):
    pool = get_session(backend)
    if not pool:
        return
    try:
        while pool._reaper and pool._reaper.running:
            logger.info('Stop pool thread')
            pool.stop_reaper()
            time.sleep(0.5)
    except AttributeError:
        pass


def run_app(args):
    config = MyConfigParser(allow_no_value=True)
    for inifile in args.config:
        config.test(inifile)
        config.read(inifile)

    create_socket_pool()
    app = TendersToSQL(config, args)
    app.run()
    app.close_client()
    shutdown_sockets()


def main():
    description = "Prozorro API to SQL server bridge v%s" % __version__
    parser = ArgumentParser(description=description)
    parser.add_argument('config', nargs='+', help='ini file(s)')
    parser.add_argument('-o', '--offset', type=str, help='client api offset')
    parser.add_argument('-l', '--limit', type=int, help='client api limit')
    parser.add_argument('-r', '--resume', action='store_true', help='dont drop table')
    parser.add_argument('-i', '--ignore', action='store_true', help='ignore errors')
    parser.add_argument('-x', '--drop-cache', action='store_true', help="clear cache")
    parser.add_argument('-f', '--fill-cache', action='store_true', help="only save to cache")
    parser.add_argument('-n', '--no-cache', action='store_true', help="don't use cache")
    parser.add_argument('-d', '--debug', action='store_true', help='print traceback')
    parser.add_argument('-p', '--pause', action='store_true', help='pause before exit')
    args = parser.parse_args()

    for inifile in args.config:
        logging.config.fileConfig(inifile)
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
            logger.error(e)
        exit_code = 1

    if args.pause:
        print "Press Enter to continue..."
        raw_input()

    logger.info("Exit with code %d", exit_code)

    return exit_code


if __name__ == '__main__':
    sys.exit(main())
