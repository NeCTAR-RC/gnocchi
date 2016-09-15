
from __future__ import absolute_import

try:
    import influxdb
except ImportError:
    influxdb = None


def get_connection(conf):
    if influxdb is None:
        raise RuntimeError("python-influxdb unavailable")
    print influxdb.__file__
    return influxdb.InfluxDBClient(conf.influxdb_host,
                                 conf.influxdb_port,
                                 conf.influxdb_username,
                                 conf.influxdb_password,
                                 conf.influxdb_database)
