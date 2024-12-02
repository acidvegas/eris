#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_fcc.py

'''
This plugin downloads and processes FCC license data from:
https://data.fcc.gov/download/license-view/fcc-license-view-data-csv-format.zip
'''

import asyncio
import csv
import io
import logging
import zipfile
from datetime import datetime

try:
    import aiohttp
except ImportError:
    raise ImportError('Missing required \'aiohttp\' library. (pip install aiohttp)')

# Set a default elasticsearch index if one is not provided
default_index = 'eris-fcc'

# FCC data URL
FCC_URL = 'https://data.fcc.gov/download/license-view/fcc-license-view-data-csv-format.zip'


def construct_map() -> dict:
    '''Construct the Elasticsearch index mapping for FCC license records.'''
    
    # Match on exact value or full text search
    keyword_mapping = { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

    # Numeric fields with potential decimal values
    float_mapping = { 'type': 'float' }
    
    return {
        'mappings': {
            'properties': {
                'license_id'              : {'type': 'long'},
                'source_system'           : keyword_mapping,
                'callsign'                : keyword_mapping,
                'facility_id'             : {'type': 'long'},
                'frn'                     : keyword_mapping,
                'lic_name'                : keyword_mapping,
                'common_name'             : keyword_mapping,
                'radio_service_code'      : keyword_mapping,
                'radio_service_desc'      : keyword_mapping,
                'rollup_category_code'    : keyword_mapping,
                'rollup_category_desc'    : keyword_mapping,
                'grant_date'              : {'type': 'date'},
                'expired_date'            : {'type': 'date'},
                'cancellation_date'       : {'type': 'date'},
                'last_action_date'        : {'type': 'date'},
                'lic_status_code'         : keyword_mapping,
                'lic_status_desc'         : keyword_mapping,
                'rollup_status_code'      : keyword_mapping,
                'rollup_status_desc'      : keyword_mapping,
                'entity_type_code'        : keyword_mapping,
                'entity_type_desc'        : keyword_mapping,
                'rollup_entity_code'      : keyword_mapping,
                'rollup_entity_desc'      : keyword_mapping,
                'lic_address'             : keyword_mapping,
                'lic_city'                : keyword_mapping,
                'lic_state'               : keyword_mapping,
                'lic_zip_code'            : keyword_mapping,
                'lic_attention_line'      : keyword_mapping,
                'contact_company'         : keyword_mapping,
                'contact_name'            : keyword_mapping,
                'contact_title'           : keyword_mapping,
                'contact_address1'        : keyword_mapping,
                'contact_address2'        : keyword_mapping,
                'contact_city'            : keyword_mapping,
                'contact_state'           : keyword_mapping,
                'contact_zip'             : keyword_mapping,
                'contact_country'         : keyword_mapping,
                'contact_phone'           : keyword_mapping,
                'contact_fax'             : keyword_mapping,
                'contact_email'           : keyword_mapping,
                'market_code'             : keyword_mapping,
                'market_desc'             : keyword_mapping,
                'channel_block'           : keyword_mapping,
                'loc_type_code'           : keyword_mapping,
                'loc_type_desc'           : keyword_mapping,
                'loc_city'                : keyword_mapping,
                'loc_county_code'         : keyword_mapping,
                'loc_county_name'         : keyword_mapping,
                'loc_state'               : keyword_mapping,
                'loc_radius_op'           : float_mapping,
                'loc_seq_id'              : {'type': 'long'},
                'loc_lat_deg'             : {'type': 'integer'},
                'loc_lat_min'             : {'type': 'integer'},
                'loc_lat_sec'             : float_mapping,
                'loc_lat_dir'             : keyword_mapping,
                'loc_long_deg'            : {'type': 'integer'},
                'loc_long_min'            : {'type': 'integer'},
                'loc_long_sec'            : float_mapping,
                'loc_long_dir'            : keyword_mapping,
                'hgt_structure'           : float_mapping,
                'asr_num'                 : keyword_mapping,
                'antenna_id'              : {'type': 'long'},
                'ant_seq_id'              : {'type': 'long'},
                'ant_make'                : keyword_mapping,
                'ant_model'               : keyword_mapping,
                'ant_type_code'           : keyword_mapping,
                'ant_type_desc'           : keyword_mapping,
                'azimuth'                 : float_mapping,
                'beamwidth'               : float_mapping,
                'polarization_code'       : keyword_mapping,
                'frequency_id'            : {'type': 'long'},
                'freq_seq_id'             : {'type': 'long'},
                'freq_class_station_code' : keyword_mapping,
                'freq_class_station_desc' : keyword_mapping,
                'power_erp'               : float_mapping,
                'power_output'            : float_mapping,
                'frequency_assigned'      : float_mapping,
                'frequency_upper_band'    : float_mapping,
                'unit_of_measure'         : keyword_mapping,
                'tolerance'               : float_mapping,
                'emission_id'             : {'type': 'long'},
                'emission_seq_id'         : {'type': 'long'},
                'emission_code'           : keyword_mapping,
                'ground_elevation'        : float_mapping
            }
        }
    }

async def download_and_extract_csv():
    '''Download and extract the FCC license data ZIP file.'''

    async with aiohttp.ClientSession() as session:
        async with session.get(FCC_URL) as response:
            if response.status != 200:
                raise Exception(f'Failed to download FCC data: HTTP {response.status}')
            
            zip_data = await response.read()
            
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_file:
        # Get the first CSV file in the ZIP
        csv_filename = next(name for name in zip_file.namelist() if name.endswith('.csv'))
        return zip_file.read(csv_filename).decode('utf-8')


def parse_date(date_str):
    '''Parse date string to ISO format or return None if invalid.'''
    if not date_str or date_str == '0000-00-00':
        return None
    try:
        # Try the new format first
        return datetime.strptime(date_str.strip(), '%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        try:
            # Try the old format
            return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            return None


async def process_data(input_path: str = None):
    '''
    Process the FCC license data.
    
    :param input_path: Optional path to local CSV file (if not downloading)
    '''

    if input_path:
        with open(input_path, 'r') as f:
            csv_data = f.read()
    else:
        csv_data = await download_and_extract_csv()
    
    # Process CSV data
    csv_reader = csv.DictReader(io.StringIO(csv_data))
    
    for row in csv_reader:
        # Convert date fields
        date_fields = ['grant_date', 'expired_date', 'cancellation_date', 'last_action_date']
        
        for field in date_fields:
            if field in row:
                row[field] = parse_date(row[field])
        
        # Convert numeric fields
        numeric_fields = {
            'int'   : ['facility_id', 'loc_lat_deg', 'loc_long_deg', 'loc_lat_min', 'loc_long_min', 'loc_seq_id', 'ant_seq_id', 'freq_seq_id', 'emission_seq_id'],
            'float' : ['loc_lat_sec', 'loc_long_sec', 'loc_radius_op', 'hgt_structure', 'azimuth', 'beamwidth', 'power_erp', 'power_output', 'frequency_assigned', 'frequency_upper_band', 'tolerance', 'ground_elevation'],
            'long'  : ['license_id', 'antenna_id', 'frequency_id', 'emission_id']
        }
        
        for field_type, fields in numeric_fields.items():
            for field in fields:
                if field in row and row[field]:
                    try:
                        if field_type == 'int':
                            row[field] = int(float(row[field]))
                        elif field_type == 'float':
                            row[field] = float(row[field])
                        elif field_type == 'long':
                            row[field] = int(float(row[field]))
                    except (ValueError, TypeError):
                        row[field] = None
        
        # Remove empty fields
        record = {k.lower(): v for k, v in row.items() if v not in (None, '', 'NULL')}
        
        yield {'_index': default_index, '_source': record}

async def test():
    '''Test the ingestion process.'''
    async for document in process_data():
        print(document)

if __name__ == '__main__':
    asyncio.run(test()) 