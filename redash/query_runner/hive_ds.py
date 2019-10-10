import logging
import sys
import base64

from redash.query_runner import *
from redash.utils import json_dumps

logger = logging.getLogger(__name__)

try:
    from pyhive import hive
    from pyhive.exc import DatabaseError
    from thrift.transport import THttpClient
    enabled = True
except ImportError:
    enabled = False

COLUMN_NAME = 0
COLUMN_TYPE = 1

types_map = {
    'BIGINT_TYPE': TYPE_INTEGER,
    'TINYINT_TYPE': TYPE_INTEGER,
    'SMALLINT_TYPE': TYPE_INTEGER,
    'INT_TYPE': TYPE_INTEGER,
    'DOUBLE_TYPE': TYPE_FLOAT,
    'DECIMAL_TYPE': TYPE_FLOAT,
    'FLOAT_TYPE': TYPE_FLOAT,
    'REAL_TYPE': TYPE_FLOAT,
    'BOOLEAN_TYPE': TYPE_BOOLEAN,
    'TIMESTAMP_TYPE': TYPE_DATETIME,
    'DATE_TYPE': TYPE_DATETIME,
    'CHAR_TYPE': TYPE_STRING,
    'STRING_TYPE': TYPE_STRING,
    'VARCHAR_TYPE': TYPE_STRING
}


class Hive(BaseSQLQueryRunner):
    should_annotate_query = False
    noop_query = "SELECT 1"

    @classmethod
    def configuration_schema(cls):
        # return {
        #     "type": "object",
        #     "properties": {
        #         "host": {
        #             "type": "string"
        #         },
        #         "port": {
        #             "type": "number"
        #         },
        #         "database": {
        #             "type": "string"
        #         },
        #         "username": {
        #             "type": "string"
        #         },
        #     },
        #     "order": ["host", "port", "database", "username"],
        #     "required": ["host"]
        # }
        return {
        "type": "object",
        "properties": {
                "host": {
                    "type": "string"
                },
                "port": {
                    "type": "number"
                },
                "database": {
                    "type": "string"
                },
                "username": {
                    "type": "string"
                },
                "mysql_host": {
                    "type": "string",
                },
                "mysql_port": {
                    "type": "number",
                    "default": 3306
                },
                "mysql_database": {
                    "type": "string",
                    "default": "hive_test"
                },
                "mysql_username": {
                    "type": "string",
                    "default": "root"
                },
                "passwd": {
                    "type": "string",
                    "title": "Mysql Password"
                }
            },
        "order": ["host", "port", "database", "username", "mysql_host", "mysql_port", "mysql_database", "mysql_username", "passwd"],
        "required": ["host", "mysql_host", "mysql_port", "mysql_database", "mysql_username", "passwd"],
        'secret': ['passwd']
        }

    @classmethod
    def type(cls):
        return "hive"

    @classmethod
    def enabled(cls):
        return enabled

    # def _get_tables(self, schema):
        # schemas_query = "show schemas"

        # tables_query = "show tables in %s"

        # columns_query = "show columns in %s.%s"

        # for schema_name in filter(lambda a: len(a) > 0, map(lambda a: str(a['database_name']), self._run_query_internal(schemas_query))):
        #     for table_name in filter(lambda a: len(a) > 0, map(lambda a: str(a['tab_name']), self._run_query_internal(tables_query % schema_name))):
        #         columns = filter(lambda a: len(a) > 0, map(lambda a: str(a['field']), self._run_query_internal(columns_query % (schema_name, table_name))))

        #         if schema_name != 'default':
        #             table_name = '{}.{}'.format(schema_name, table_name)

        #         schema[table_name] = {'name': table_name, 'columns': columns}
        # return schema.values()
        
    def _get_tables(self, schema):
        table_list = []
        tabel_sql = "select tbl_name as table_name from DBS as db inner join TBLS as tbl on db.db_id = tbl.db_id where db.name = '"+self.configuration["database"]+"'"
        results, error = self._mysql_run(tabel_sql, None)
        if error is not None:
            raise Exception("Failed getting schema.")
        results = json_loads(results)
        for row in results['rows']:
            table_list.append(row['table_name'])
        for table_name in table_list:
            tabel_sql = """
                SELECT *
                FROM
                (
                  SELECT col.column_name as column_name,
                          col.type_name AS column_type,
                          col.COMMENT AS column_comment
                   FROM DBS AS db
                   INNER JOIN TBLS AS tbl ON db.db_id = tbl.db_id
                   INNER JOIN SDS AS sds ON tbl.sd_id = sds.sd_id
                   INNER JOIN COLUMNS_V2 AS col ON sds.cd_id = col.cd_id
                   WHERE db.name = '"""+self.configuration["database"]+"""'
                     AND tbl_name = '"""+table_name+"""'
                   UNION ALL 
                   SELECT pt.PKEY_NAME AS column_name,
                          pt.PKEY_TYPE AS column_type,
                          pt.PKEY_COMMENT as column_comment
                   FROM DBS AS db
                   INNER JOIN TBLS AS tbl ON db.db_id = tbl.db_id
                   LEFT JOIN PARTITION_KEYS AS pt ON tbl.tbl_id = pt.tbl_id
                   WHERE db.name = '"""+self.configuration["database"]+"""'
                     AND tbl_name = '"""+table_name+"""'
                ) AS t
                WHERE t.COLUMN_NAME IS NOT NULL
            """
            results, error = self._mysql_run(tabel_sql, None)
            if error is not None:
                raise Exception("Failed getting schema.")
            results = json_loads(results)
            for row in results['rows']:
                if table_name not in schema:
                    schema[table_name] = {'name': table_name, 'columns': []}
                schema[table_name]['columns'].append(row['column_name'])
        return schema.values()

    def _get_connection(self):
        host = self.configuration['host']

        connection = hive.connect(
            host=host,
            port=self.configuration.get('port', None),
            database=self.configuration.get('database', 'default'),
            username=self.configuration.get('username', None),
        )

        return connection

    def run_query(self, query, user):
        connection = None
        try:
            connection = self._get_connection()
            cursor = connection.cursor()

            cursor.execute(query)

            column_names = []
            columns = []

            for column in cursor.description:
                column_name = column[COLUMN_NAME]
                column_names.append(column_name)

                columns.append({
                    'name': column_name,
                    'friendly_name': column_name,
                    'type': types_map.get(column[COLUMN_TYPE], None)
                })

            rows = [dict(zip(column_names, row)) for row in cursor]

            data = {'columns': columns, 'rows': rows}
            json_data = json_dumps(data)
            error = None
        except KeyboardInterrupt:
            if connection:
                connection.cancel()
            error = "Query cancelled by user."
            json_data = None
        except DatabaseError as e:
            try:
                error = e.args[0].status.errorMessage
            except AttributeError:
                error = str(e)
            json_data = None
        finally:
            if connection:
                connection.close()

        return json_data, error


class HiveHttp(Hive):
    @classmethod
    def name(cls):
        return "Hive (HTTP)"

    @classmethod
    def type(cls):
        return 'hive_http'

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "host": {
                    "type": "string"
                },
                "port": {
                    "type": "number"
                },
                "database": {
                    "type": "string"
                },
                "username": {
                    "type": "string"
                },
                "http_scheme": {
                    "type": "string",
                    "title": "HTTP Scheme (http or https)",
                    "default": "https"
                },
                "http_path": {
                    "type": "string",
                    "title": "HTTP Path"
                },
                "http_password": {
                    "type": "string",
                    "title": "Password"
                },
            },
            "order": ["host", "port", "http_path", "username", "http_password", "database", "http_scheme"],
            "secret": ["http_password"],
            "required": ["host", "http_path"]
        }

    def _get_connection(self):
        host = self.configuration['host']

        scheme = self.configuration.get('http_scheme', 'https')

        # if path is set but is missing initial slash, append it
        path = self.configuration.get('http_path', '')
        if path and path[0] != '/':
            path = '/' + path

        # if port is set prepend colon
        port = self.configuration.get('port', '')
        if port:
            port = ':' + str(port)

        http_uri = "{}://{}{}{}".format(scheme, host, port, path)

        # create transport
        transport = THttpClient.THttpClient(http_uri)

        # if username or password is set, add Authorization header
        username = self.configuration.get('username', '')
        password = self.configuration.get('http_password', '')
        if username or password:
            auth = base64.b64encode(username + ':' + password)
            transport.setCustomHeaders({'Authorization': 'Basic ' + auth})

        # create connection
        connection = hive.connect(thrift_transport=transport)

        return connection


register(Hive)
register(HiveHttp)
