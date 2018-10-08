from datetime import datetime


def parse_log_entry(log):
    server_name, timestamp, remote_ip, uri = log.split(',')
    return dict(server_name=server_name,
                timestamp=datetime.fromtimestamp(int(timestamp)),
                remote_ip=remote_ip,
                uri=uri)
