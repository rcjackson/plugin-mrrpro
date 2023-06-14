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
        # Make sure we have a complete file from the MRR and continue when we do when in
        # pull one file mode
        if num_files == 1:
            while file_list[-1][0] == ".":
                file_list = recursive_list(sftp)

        # If we're pulling every file, then load database to make sure we are not uploading duplicates
        file_names = []
        if num_files > 1:
            print("Accessing beehive...")
            df = sage_data_client.query(
                start="-%dh" % num_files,
                filter={"name": "upload", "vsn": "W08D",
                     "plugin": "registry.sagecontinuum.org/rjackson/mrrpro:0.1.1"}).set_index("timestamp")
            file_names = df['meta.filename'].values
        for fi in file_list[-num_files:]:
            print(fi)
            base, name = os.path.split(fi)
            try:
                dt = datetime.datetime.strptime(name, '%Y%m%d_%H%M%S.nc')
            except ValueError:
                # File incomplete, skip. Should not happen in one-file mode.
                continue
                
            if not int(args.hour) == -1:
                if not int(dt.hour) == int(args.hour):
                    continue
            if name in file_names:
                print('%s already on beehive, skipping!' % name)
                continue
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
    parser.add_argument('-hr', '--hour', default=-1,
            help='Hour of the day to transfer (-1 for all hours)')
    args = parser.parse_args()
    main(args)
