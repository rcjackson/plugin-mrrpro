import paramiko
import datetime
import os
import logging
import sage_data_client
import argparse

from waggle.plugin import Plugin

mrr_ip_address = '10.31.81.113'
mrr_user_name = 'mrruser'
mrr_password = 'metek'
vsn = "W08D"

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
        ymds = sftp.listdir('/u/data/%s/' % ym)
        for ymd in ymds:
            files = sftp.listdir('/u/data/%s/%s/' % (ym, ymd))
            for fi in files:
                if '.nc' in fi:
                    file_list.append('/u/data/%s/%s/%s' % (ym, ymd, fi))

    return file_list


def main(args):
    num_files = int(args.num_files)
    
    with Plugin() as plugin:
        t = paramiko.Transport((mrr_ip_address, 22))
        channel = t.connect(username=mrr_user_name, password=mrr_password)
        sftp = t.open_sftp_client()
        sftp.chdir('/u/data')
        file_list = recursive_list(sftp)
        for fi in file_list[-num_files:]:
            print(fi)
            base, name = os.path.split(fi)
            dt = datetime.datetime.strptime(name, '%Y%m%d_%H%M%S.nc')
            timestamp = int(datetime.datetime.timestamp(dt) * 1e9)
            logging.debug("Downloading %s" % fi)
            sftp.get(fi, localpath=name)
            logging.debug("Uploading %s to beehive" % fi)
            plugin.upload_file(name)
                

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            prog='mrrpro-plugin',
            description='Plugin to transfer MRR-PRO data')
    parser.add_argument('-n', '--num-files', default=1,
            help='number of files to transfer (0 to transfer all data)')
    args = parser.parse_args()
    main(args)
