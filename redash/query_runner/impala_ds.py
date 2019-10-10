import logging

from redash.query_runner import *
from redash.utils import json_dumps

logger = logging.getLogger(__name__)

try:
    from impala.dbapi import connect
    from impala.error import DatabaseError, RPCError
    enabled = True
except ImportError as e:
    enabled = False

COLUMN_NAME = 0
COLUMN_TYPE = 1

types_map = {
    'BIGINT': TYPE_INTEGER,
    'TINYINT': TYPE_INTEGER,
    'SMALLINT': TYPE_INTEGER,
    'INT': TYPE_INTEGER,
    'DOUBLE': TYPE_FLOAT,
    'DECIMAL': TYPE_FLOAT,
    'FLOAT': TYPE_FLOAT,
    'REAL': TYPE_FLOAT,
    'BOOLEAN': TYPE_BOOLEAN,
    'TIMESTAMP': TYPE_DATETIME,
    'CHAR': TYPE_STRING,
    'STRING': TYPE_STRING,
    'VARCHAR': TYPE_STRING
}


class Impala(BaseSQLQueryRunner):
    noop_query = "show schemas"

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
        #         "protocol": {
        #             "type": "string",
        #             "title": "Please specify beeswax or hiveserver2"
        #         },
        #         "database": {
        #             "type": "string"
        #         },
        #         "use_ldap": {
        #             "type": "boolean"
        #         },
        #         "ldap_user": {
        #             "type": "string"
        #         },
        #         "ldap_password": {
        #             "type": "string"
        #         },
        #         "timeout": {
        #             "type": "number"
        #         }
        #     },
        #     "required": ["host"],
        #     "secret": ["ldap_password"]
        # }
        return {
            "type": "object",
            "properties": {
                "host": {
                    "type": "string",
                    "title": "Host"
                },
                "port": {
                    "type": "number",
                    "title": "Port"
                },
                "database": {
                    "type": "string",
                    "title": "Database"
                },
                "user": {
                    "type": "string",
                    "title": "User"
                },
                "password": {
                    "type": "string",
                    "title": "Password"
                },
                "timeout": {
                    "type": "number",
                    "title": "timeout"
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
                "mysql_passwd": {
                    "type": "string",
                    "title": "Mysql Password"
                }
            },
            "required": ["host","port","database","user","password","timeout","mysql_host","mysql_port","mysql_database","mysql_username","mysql_passwd"],
            "order": ["host","port","database","user","password","timeout","mysql_host","mysql_port","mysql_database","mysql_username","mysql_passwd"],
            "secret": ["password","mysql_passwd"]
        }

    @classmethod
    def type(cls):
        return "impala"

    def _get_tables(self, schema_dict):
        # schemas_query = "show schemas;"
        # tables_query = "show tables in %s;"
        # columns_query = "show column stats %s.%s;"

        # for schema_name in map(lambda a: unicode(a['name']), self._run_query_internal(schemas_query)):
        #     for table_name in map(lambda a: unicode(a['name']), self._run_query_internal(tables_query % schema_name)):
        #         columns = map(lambda a: unicode(a['Column']), self._run_query_internal(columns_query % (schema_name, table_name)))

        #         if schema_name != 'default':
        #             table_name = '{}.{}'.format(schema_name, table_name)

        #         schema_dict[table_name] = {'name': table_name, 'columns': columns}

        # return schema_dict.values()
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
                if table_name not in schema_dict:
                    schema_dict[table_name] = {'name': table_name, 'columns': []}
                schema_dict[table_name]['columns'].append(row['column_name'])
        
        return schema_dict.values()



    #for hive meta 
    def _mysql_run(self, query, user):
        import MySQLdb
        connection = None
        try:
            connection = MySQLdb.connect(host=self.configuration.get('mysql_host', ''),
                                         user=self.configuration.get('mysql_username', ''),
                                         passwd=self.configuration.get('mysql_passwd', ''),
                                         db=self.configuration['mysql_database'],
                                         port=self.configuration.get('mysql_port', 3306),
                                         charset='utf8', use_unicode=True,
                                         connect_timeout=60)
            cursor = connection.cursor()
            logger.debug("MySQL running query: %s", query)
            cursor.execute(query)

            data = cursor.fetchall()

            while cursor.nextset():
                data = cursor.fetchall()

            # TODO - very similar to pg.py
            if cursor.description is not None:
                columns = self.fetch_columns([(i[0], types_map.get(i[1], None)) for i in cursor.description])
                rows = [dict(zip((c['name'] for c in columns), row)) for row in data]

                data = {'columns': columns, 'rows': rows}
                json_data = json_dumps(data)
                error = None
            else:
                json_data = None
                error = "No data was returned."

            cursor.close()
        except MySQLdb.Error as e:
            json_data = None
            error = e.args[1]
        except KeyboardInterrupt:
            cursor.close()
            error = "Query cancelled by user."
            json_data = None
        finally:
            if connection:
                connection.close()

        return json_data, error
    def run_query(self, query, user):

        connection = None
        try:
            connection = connect(**self.configuration.to_dict())

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
            cursor.close()
        except DatabaseError as e:
            json_data = None
            error = e.message
        except RPCError as e:
            json_data = None
            error = "Metastore Error [%s]" % e.message
        except KeyboardInterrupt:
            connection.cancel()
            error = "Query cancelled by user."
            json_data = None
        finally:
            if connection:
                connection.close()

        return json_data, error


register(Impala)
