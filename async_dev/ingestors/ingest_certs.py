#!/usr/bin/env python
# Elasticsearch Recon Ingestion Scripts (ERIS) - Developed by Acidvegas (https://git.acid.vegas/eris)
# ingest_certstream.py

import asyncio
import json
import logging

try:
    import websockets
except ImportError:
    raise ImportError('Missing required \'websockets\' library. (pip install websockets)')

default_index = 'cert-stream'

def construct_map() -> dict:
    '''Construct the Elasticsearch index mapping for Certstream records.'''

    keyword_mapping = { 'type': 'text',  'fields': { 'keyword': { 'type': 'keyword', 'ignore_above': 256 } } }

    mapping = {
        'mappings': {
            'properties': {
                'data': {
                    'properties': {
                        'cert_index': { 'type': 'integer' },
                        'cert_link': { 'type': 'keyword' },
                        'leaf_cert': {
                            'properties': {
                                'all_domains': { 'type': 'keyword' },
                                'extensions': {
                                    'properties': {
                                        'authorityInfoAccess'    : { 'type': 'text'    },
                                        'authorityKeyIdentifier' : { 'type': 'text'    },
                                        'basicConstraints'       : { 'type': 'text'    },
                                        'certificatePolicies'    : { 'type': 'text'    },
                                        'crlDistributionPoints'  : { 'type': 'text'    },
                                        'ctlPoisonByte'          : { 'type': 'boolean' },
                                        'extendedKeyUsage'       : { 'type': 'text'    },
                                        'keyUsage'               : { 'type': 'text'    },
                                        'subjectAltName'         : { 'type': 'text'    },
                                        'subjectKeyIdentifier'   : { 'type': 'text'    }
                                    }
                                },
                                'fingerprint': { 'type': 'keyword' },
                                'issuer': {
                                    'properties': {
                                        'C'            : { 'type': 'keyword' },
                                        'CN'           : { 'type': 'text'    },
                                        'L'            : { 'type': 'text'    },
                                        'O'            : { 'type': 'text'    },
                                        'OU'           : { 'type': 'text'    },
                                        'ST'           : { 'type': 'text'    },
                                        'aggregated'   : { 'type': 'text'    },
                                        'emailAddress' : { 'type': 'text'    }
                                    }
                                },
                                'not_after': { 'type': 'integer' },
                                'not_before': { 'type': 'integer' },
                                'serial_number': { 'type': 'keyword' },
                                'signature_algorithm': { 'type': 'text' },
                                'subject': {
                                    'properties': {
                                        'C'            : { 'type': 'keyword' },
                                        'CN'           : { 'type': 'text'    },
                                        'L'            : { 'type': 'text'    },
                                        'O'            : { 'type': 'text'    },
                                        'OU'           : { 'type': 'text'    },
                                        'ST'           : { 'type': 'text'    },
                                        'aggregated'   : { 'type': 'text'    },
                                        'emailAddress' : { 'type': 'text'    }
                                    }
                                }
                            }
                        },
                        'seen': { 'type': 'date', 'format': 'epoch_second' },
                        'source': {
                            'properties': {
                                'name' : { 'type': 'keyword' },
                                'url'  : { 'type': 'keyword' }
                            }
                        },
                        'update_type': { 'type': 'keyword' }
                    }
                },
                'message_type': { 'type': 'keyword' }
            }
        }
    }

    return mapping


async def process_data(file_path: str = None):
    '''
    Read and process Certsream records live from the Websocket stream.
    
    :param file_path: Path to the Certstream log file (unused, placeholder for consistency with other ingestors)
    '''

    while True:
        try:
            async with websockets.connect('wss://certstream.calidog.io/') as websocket:
                
                while True:
                    line = await websocket.recv()

                    try:
                        record = json.loads(line)
                    except json.decoder.JSONDecodeError:
                        logging.error(f'Failed to parse JSON record from Certstream! ({line})')
                        input('Press Enter to continue...') # Pause the script to allow the user to read the error message
                        continue

                    yield record

        except websockets.ConnectionClosed:
            logging.error('Connection to Certstream was closed. Attempting to reconnect...')
            await asyncio.sleep(10)

        except Exception as e:
            logging.error(f'An error occurred while processing Certstream records! ({e})')
            await asyncio.sleep(10)



'''
Example record:
{
  "data": {
    "cert_index": 43061646,
    "cert_link": "https://yeti2025.ct.digicert.com/log/ct/v1/get-entries?start=43061646&end=43061646",
    "leaf_cert": {
      "all_domains": [
        "*.d7zdnegbre53n.amplifyapp.com",
        "d7zdnegbre53n.amplifyapp.com"
      ],
      "extensions": {
        "authorityInfoAccess": "CA Issuers - URI:http://crt.r2m02.amazontrust.com/r2m02.cer\nOCSP - URI:http://ocsp.r2m02.amazontrust.com\n",
        "authorityKeyIdentifier": "keyid:C0:31:52:CD:5A:50:C3:82:7C:74:71:CE:CB:E9:9C:F9:7A:EB:82:E2\n",
        "basicConstraints": "CA:FALSE",
        "certificatePolicies": "Policy: 2.23.140.1.2.1",
        "crlDistributionPoints": "Full Name:\n URI:http://crl.r2m02.amazontrust.com/r2m02.crl",
        "ctlPoisonByte": true,
        "extendedKeyUsage": "TLS Web server authentication, TLS Web client authentication",
        "keyUsage": "Digital Signature, Key Encipherment",
        "subjectAltName": "DNS:d7zdnegbre53n.amplifyapp.com, DNS:*.d7zdnegbre53n.amplifyapp.com",
        "subjectKeyIdentifier": "59:32:78:2A:11:03:62:55:BB:3B:B9:80:24:76:28:90:2E:D1:A4:56"
      },
      "fingerprint": "D9:05:A3:D5:AA:F9:68:BC:0C:0A:15:69:C9:5E:11:92:32:67:4F:FA",
      "issuer": {
        "C": "US",
        "CN": "Amazon RSA 2048 M02",
        "L": null,
        "O": "Amazon",
        "OU": null,
        "ST": null,
        "aggregated": "/C=US/CN=Amazon RSA 2048 M02/O=Amazon",
        "emailAddress": null
      },
      "not_after": 1743811199,
      "not_before": 1709596800,
      "serial_number": "FDB450C1942E3D30A18737063449E62",
      "signature_algorithm": "sha256, rsa",
      "subject": {
        "C": null,
        "CN": "*.d7zdnegbre53n.amplifyapp.com",
        "L": null,
        "O": null,
        "OU": null,
        "ST": null,
        "aggregated": "/CN=*.d7zdnegbre53n.amplifyapp.com",
        "emailAddress": null
      }
    },
    "seen": 1709651773.594684,
    "source": {
      "name": "DigiCert Yeti2025 Log",
      "url": "https://yeti2025.ct.digicert.com/log/"
    },
    "update_type": "PrecertLogEntry"
  },
  "message_type": "certificate_update"
}
'''