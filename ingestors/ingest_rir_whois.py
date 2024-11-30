#/usr/bin/env python
# opengeoip - developed by acidvegas in python (https://git.acid.vegas/opengeoip)
# whois.py

'''
Source: https://ftp.[afrinic|apnic|arin|lacnic|ripe].net/ + others
Notes : The databases are updated daily.
		Check out the whois2json.py script for converting the whois data into JSON objects for elasticsearch.
		Each database has a CURRENTSERIAL file that contains the serial number of the latest version of the database.
		Need to find a smart way to unify the approach for each RIR to account for their differences.
		Do we need ripe-nonauth.db.gz?      | https://ftp.ripe.net/ripe/dbase/
		Do we need twnic, krnic, jpnic?     | https://ftp.apnic.net/apnic/dbase/data/
		Do we need the IRR databases?       | https://www.irr.net/docs/list.html
		Do we need the LACNIC IRR database? | https://ftp.lacnic.net/lacnic/irr/
'''

import os
import sys

try:
    import utils
except ImportError:
    raise SystemError('utils.py module not found!')

REGISTRY_DATA = {
	'AFRINIC'  : 'ftp.afrinic.net/dbase/afrinic.db.gz',
	'APNIC'    : ['ftp.apnic.net/apnic/whois/apnic.db.as-block.gz', # APNIC really has to be "special" snowflake...
				 'ftp.apnic.net/apnic/whois/apnic.db.as-set.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.aut-num.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.domain.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.filter-set.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.inet-rtr.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.inet6num.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.inetnum.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.irt.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.key-cert.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.limerick.gz', # Little easteregg, not updated since 2017...
				 'ftp.apnic.net/apnic/whois/apnic.db.mntner.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.organisation.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.peering-set.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.role.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.route-set.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.route.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.route6.gz',
				 'ftp.apnic.net/apnic/whois/apnic.db.rtr-set.gz'],
	'JPNIC'    : 'ftp.apnic.net/apnic/dbase/data/jpnic.db.gz', # JPNIC  is APNIC but is a NIR
	'KRNIC'    : 'ftp.apnic.net/apnic/dbase/data/krnic.db.gz', # KRNIC  is APNIC but is a NIR
	'TWINIC'   : 'ftp.apnic.net/apnic/dbase/data/twnic.db.gz', # TWINIC is APNIC but is a NIR
	'ARIN'     : 'ftp.arin.net/pub/rr/arin.db.gz',
	'LACNIC'   : ['ftp.lacnic.net/lacnic/dbase/lacnic.db.gz', # LACNIC has a dbase and an IRR
				 'ftp.lacnic.net/lacnic/irr/lacnic.db.gz'],
	'RIPE'     : ['ftp.ripe.net/ripe/dbase/ripe.db.gz', # RIPE has a dbase and a non-auth dbase
				 'ftp.ripe.net/ripe/dbase/ripe-nonauth.db.gz']
}

IRR_DATA = {
	'ALTDB'    : 'ftp.altdb.net/pub/altdb/altdb.db.gz',
	'BELL'     : 'whois.in.bell.ca/bell.db.gz',
	'CANARIE'  : 'ftp.radb.net/radb/dbase/canarie.db.gz',
	'HOST'     : 'ftp.radb.net/radb/dbase/host.db.gz',
	'IDNIC'    : 'irr-mirror.idnic.net/idnic.db.gz',
	'JPIRR'    : 'ftp.nic.ad.jp/jpirr/jpirr.db.gz',
	'LACNIC'   : 'ftp.lacnic.net/lacnic/irr/lacnic.db.gz', # Name conflict...
	'LEVEL3'   : 'rr.Level3.net/level3.db.gz',
	'NESTEGG'  : 'ftp.nestegg.net/irr/nestegg.db.gz',
	'NTTCOM'   : 'rr1.ntt.net/nttcomRR/nttcom.db.gz',
	'OPENFACE' : 'ftp.radb.net/radb/dbase/openface.db.gz',
	'PANIX'    : 'ftp.panix.com/pub/rrdb/panix.db.gz',
	'RADB'     : 'ftp.radb.net/radb/dbase/radb.db.gz',
	'REACH'    : 'ftp.radb.net/radb/dbase/reach.db.gz',
	'RGNET'    : 'ftp.radb.net/radb/dbase/rgnet.db.gz',
	'TC'       : 'ftp.bgp.net.br/tc.db.gz'
}


def check_serial(local_serial_path: str, ftp_serial_url: str) -> bool:
	'''
	Check the CURRENTSERIAL file for an update.

	:param local_path: The path to the local CURRENTSERIAL file
	:param ftp_path: The path to the remote CURRENTSERIAL file
	'''

	if not os.path.exists(local_serial_path):
		return True

	else:
		with open(local_serial_path) as serial_file:
			old_serial = serial_file.read().strip()
		new_serial = utils.read_ftp(ftp_serial_url)
		if old_serial != new_serial:
			return True
		else:
			return False


def download_db(output_path: str, url: str):
	'''
	Dowload a WHOIS database.

	:param output_path: The path to the output directory
	:param url: The FTP URL of the database to download
	'''

	DB_PATH = os.path.join(output_path, url.split('/')[-1])
	utils.safe_remove(DB_PATH)
	utils.safe_remove(DB_PATH[:-3])
	utils.download_ftp(url, DB_PATH)
	utils.gunzip_extract(DB_PATH, DB_PATH[:-3])
	utils.safe_remove(DB_PATH)


def update_db(output_path: str):
	'''
	Update the whois IRR databases.

	:param output_path: The path to the output directory
	'''

	os.makedirs(output_path, exist_ok=True)

	for RIR, DB_URL in REGISTRY_DATA.items():
		if RIR == 'APNIC':
			for url in DB_URL:
				download_db(output_path, url)
		else:
			download_db(output_path, DB_URL)
	for IRR, DB_URL in REGISTRY_DATA.items():
		download_db(output_path+'/irr', DB_URL)

		# For now, we'll just download the files and not check the serials for updates APNIC will need to be handled differently...seperate function?
		'''
		SERIAL_PATH = os.path.join(output_path, REGISTRY+'.CURRENTSERIAL')
		FTP_SERIAL  = os.path.join('/'.join(DB_URL.split('/')[:-1]), REGISTRY + '.CURRENTSERIAL')

		if check_serial(SERIAL_PATH, FTP_SERIAL):
			logging.debug(f'A new version of the {REGISTRY} whois database is available! Downloading...')			
			for item in (DB_PATH, DB_PATH[:-3], SERIAL_PATH):
				utils.safe_remove(item) # Remove old files
			utils.download_ftp(DB_URL, DB_PATH)
			utils.download_ftp(FTP_SERIAL, SERIAL_PATH)
			utils.gunzip_extract(DB_PATH, DB_PATH[:-3])
			logging.debug(f'The {REGISTRY} whois database has been updated!')
		else:
			logging.info(f'The {REGISTRY} whois database is up-to-date')
		'''


if __name__ == '__main__':
	utils.print_header('WHOIS Module')
	utils.setup_logger('whois')
	utils.setup_user_agent()
	update_db('database/whois')