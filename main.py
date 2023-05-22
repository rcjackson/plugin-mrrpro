import paramiko
import datetime
import os
import logging
import sage_data_client
from waggle.plugin import Plugin

mrr_ip_address = '10.31.81.113'
mrr_user_name = 'mrruser'
mrr_password = 'metek'

def recursive_list(sftp):
    """
    Returns a recursive directory listing from the current directory in the SFTP Connection.
    This assumes the data are stored as /%y%m/%y%m%d/

    Parameters
    ----------
    sftp: SFTPClient
        The current SFTP Client.

    Returns
    -------
    list: str list
        The list of .nc files available
    """
    file_list = []
    year_months = sftp.listdir()
    for ym in year_months:
        ymds = sftp.listdir('/u/data/%s' % ym)
        for ymd in ymds:
            files = sftp.listdir('/u/data/%s/%s' % (ym, ymd))
            for fi in files:
                file_list.append('/u/data/%s/%s/%s' % (ym, ymd, fi))

    return file_list


def main():
    with Plugin() as plugin:
        t = paramiko.Transport((mrr_ip_address, 22))
        channel = t.connect(username=mrr_user_name, password=mrr_password)
        sftp = t.open_sftp_client()
        sftp.chdir('/u/data')
        file_list = recursive_list(sftp)
        df = sage_data_client.query(
            start="-7d",
            filter={"name": "upload"},
            vsn=vsn).set_index("timestamp")
        for fi in file_list:
            base, name = os.path.split(fi)
            if not name in df.value:
                if name[-3] == 'log':
                    dt = datetime.datetime.strptime(name, '%Y%m%d.log')
                else:
                    dt = datetime.datetime.strptime(name, '%Y%m%d_%H%M%S.nc')
                timestamp = int(datetime.datetime.timestamp(dt) * 1e9)
                logging.debug("Downloading %s" % fi)
                sftp.get(fi, '/app/')
                logging.debug("Uploading %s to beehive" % fi)
                plugin.upload_file(os.path.join('/app/', name), timestamp=timestamp)
                os.remove(os.path.join('/app/', name))
                

if __name__ == "__main__":
    main()
