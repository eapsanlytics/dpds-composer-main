"""
Customized version of airflow.providers.sftp.sensors.sftp
that will handle a wildcarded filename at the end of the path
"""

import fnmatch
import ntpath

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from paramiko import SFTP_NO_SUCH_FILE


class SFTPSensor(BaseSensorOperator):
    """
    Waits for a file or directory to be present on SFTP.

    :param path: Remote file or directory path
    :type path: str
    :param sftp_conn_id: The connection to run the sensor against
    :type sftp_conn_id: str
    """
    template_fields = ('path',)

    @apply_defaults
    def __init__(self, path, sftp_conn_id='sftp_default', *args, **kwargs):
        super(SFTPSensor, self).__init__(*args, **kwargs)
        self.path = path
        self.hook = None
        self.sftp_conn_id = sftp_conn_id

    def poke(self, context):
        self.hook = SFTPHook(self.sftp_conn_id)
        self.log.info('Poking for %s', self.path)
        try:
            search_dir, search_file = ntpath.split(self.path)
            search_dir = search_dir + "/"
            file_list = self.hook.list_directory(search_dir)
            filtered = fnmatch.filter(file_list, search_file)
            if len(filtered) > 0:
                search_path = search_dir + filtered[0]
            else:
                search_path = self.path
            self.hook.get_mod_time(search_path)
            self.log.info('Found %s', self.path)
        except IOError as io_error:
            if io_error.errno != SFTP_NO_SUCH_FILE:
                raise io_error
            return False
        self.hook.close_conn()
        return True
