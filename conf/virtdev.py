FS = True
LO = True
BT = False
HA = False
USB = False
RSYNC = False
SHADOW = False
EXPOSE = False
MASTER = False
EXTEND = False

USR_FINDER = False
USR_MAPPER = False
DEV_FINDER = False
DEV_MAPPER = False
DISTRIBUTOR = True
DATA_SERVER = False

LOG_ERR = True
LOG_DEBUG = True
LOG_WARNNING = False

BT_PORT = 1
LO_PORT = 15101
ROOT_PORT = 16101
DAEMON_PORT = 17101
FILTER_PORT = 18101
BROKER_PORT = 19101
MASTER_PORT = 20101
BRIDGE_PORT = 21101
ADAPTER_PORT = 22101
HANDLER_PORT = 23101
NOTIFIER_PORT = 27001
REQUESTER_PORT = 24101
CONDUCTOR_PORT = 25101
FILE_HTTP_PORT = 50070
DISPATCHER_PORT = 26101
USR_FINDER_PORT = 27101
USR_MAPPER_PORT = 28101
DEV_FINDER_PORT = 29101
DEV_MAPPER_PORT = 30101
META_SERVER_PORT = 27017
FILE_SERVER_PORT = 34310
EVENT_MONITOR_PORT = 31101
EVENT_COLLECTOR_PORT = 32101
CACHE_PORTS = {}

LO_ADDR = '127.0.0.1'
PROC_ADDR = '127.0.0.1'
MASTER_ADDR = '0.0.0.0'
ADAPTER_ADDR = '127.0.0.1'
NOTIFIER_ADDR = '127.0.0.1'

USR_SERVERS = ['0.0.0.0']
DEV_SERVERS = ['0.0.0.0']
ROOT_SERVERS = ['0.0.0.0']
META_SERVERS = ['0.0.0.0']
DATA_SERVERS = ['0.0.0.0']
CACHE_SERVERS = ['0.0.0.0']
MAPPER_SERVERS = ['0.0.0.0']
FINDER_SERVERS = ['0.0.0.0']
BROKER_SERVERS = ['0.0.0.0']
BRIDGE_SERVERS = ['0.0.0.0']
WORKER_SERVERS = ['0.0.0.0']

PATH_LIB = '/var/lib/vdev'
PATH_MNT = '/mnt/vdev'
PATH_RUN = '/var/run/vdev'
PATH_VAR = '/vdev'
PATH_MQTT = '/etc/mosquitto/mosquitto.conf'

AREA_CODE = 0
RECORD_MAX = 30

IFNAME = 'eth0'
IFBACK = 'eth0'
PROTOCOL = 'n2n'
