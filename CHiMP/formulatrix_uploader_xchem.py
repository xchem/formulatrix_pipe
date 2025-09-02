#!/usr/bin/env python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "graypy",
#     "numpy<2",
#     "pandas",
#     "requests"
# ]
# ///

import numpy as np
import pandas as pd
import requests
import subprocess
import configparser
import argparse
import logging
import json
import shutil
import os.path
import datetime, time
import ast
import sys

from pathlib import Path
from graypy import GELFUDPHandler

parser = argparse.ArgumentParser(
    description="Registers containers to ISPyB taking a list of plate barcodes as input"
)
parser.add_argument("--barcode_list", nargs="+", required=True)
parser.add_argument("--proposal_list", nargs="+", required=True)
parser.add_argument("--config", required=True)

args = parser.parse_args()
barcodes = args.barcode_list
proposal_refs = args.proposal_list
config = open(args.config)
config = json.load(config)

url = config["url"]  #
token = config["token"]
luigidir = config["luigi_dir"]
log_cfg = config["logging"]

# Setup logging
logger = logging.getLogger()
logger.setLevel(level=logging.INFO)
# handler = GELFUDPHandler("graylog-log-target.diamond.ac.uk", 12213)
handler = GELFUDPHandler(log_cfg["gelf_udp"]["host"], log_cfg["gelf_udp"]["port"])
handler.facility = log_cfg["gelf_udp"].get("facility")
logger.addHandler(handler)

ZOCALO_RECIPE = Path(__file__).parent / "run_xchem_processing.json"
if not ZOCALO_RECIPE.is_file():
    logging.error(f"Error: Could not find recipe file {ZOCALO_RECIPE}")

# Make sure that we can find zocalo.go
if not shutil.which("zocalo.go"):
    logging.error(
        "Error: Could not find 'zocalo.go' in PATH, required for cluster submission",
    )

headers = {"Authorization": f"Bearer {token}"}  # Expeye headers

for i in range(len(barcodes)):

    try:
        barcode = barcodes[i]
        proposal_ref = proposal_refs[i]

        # logging.basicConfig(
        #     filename=f"/dls/tmp/xchem_formulatrix/{barcode}.log",
        #     format="%(asctime)s: %(levelname)s: %(message)s",
        #     level=logging.INFO,
        # )

        logger.info(
            f"Processing XChem plate barcode {barcode}, proposal {proposal_ref}"
        )

        matchdir = [
            p
            for p in Path(luigidir).iterdir()
            if p.is_dir() and p.name.startswith(f"{barcode}")
        ]

        if len(matchdir) == 0:
            logger.info(f"No image directory found for barcode {barcode}, skipping")
            continue
        elif len(matchdir) > 1:
            logger.info(
                f"More than 1 image directory found for barcode {barcode}, skipping"
            )
            continue
        else:
            logger.info(f"Image directory found for barcode {barcode}: {matchdir}")

        imagefolder = matchdir[0]
        imagepaths = sorted(list(imagefolder.iterdir()))
        drop = int(len(imagepaths) / 96)

        if drop == 2 or drop == 3:
            logger.info(
                f"Number of subwell images ({len(imagepaths)}) corresponds to a {drop} drop plate"
            )
        else:
            logger.error(
                f"Number of subwell images ({len(imagepaths)}) in {imagefolder} not does not correspond to a 2 or 3 drop plate"
            )
            continue

        list_filename = f"{imagefolder.parts[-1]}"
        with open(f"/dls/tmp/xchem_formulatrix/{list_filename}.txt", "w") as f:
            for line in imagepaths:
                f.write(f"{line}\n")

        t = os.path.getmtime(imagepaths[0])
        timestamp = datetime.datetime.fromtimestamp(t).isoformat()

        # Query some basic information using the proposal reference
        resp = requests.get(
            url + f"/api/proposals/{proposal_ref}/data", headers=headers
        ).json()
        protein_id = resp["proteins"][0]["proteinId"]  # not exact

        # crystal_id = resp["items"][0]["crystalId"]
        r = requests.post(
            url + f"/api/proteins/{protein_id}/crystals", headers=headers, json={}
        )
        crystal_id = r.json()["crystalId"]

        resp = requests.get(
            url + f"/api/proposals/{proposal_ref}/shipments", headers=headers
        ).json()
        shipment_id = resp["items"][0]["shippingId"]  # not exact

        resp = requests.get(
            url + f"/api/shipments/{shipment_id}/dewars", headers=headers
        ).json()
        dewar_id = resp["items"][0]["dewarId"]

        if drop == 3:
            container_type_id = 23  # SWISSCI 3 Drop
            container_type = f"SWISSCI {drop} Drop"
        elif drop == 2:
            logger.info(f"2 drop plate view not yet supported on Synchweb, coming soon")
            container_type_id = 14  # 20 - MRC 2 drop, 14 - CrystalQuickX
            container_type = f"CrystalQuickX"
            continue

        # Register the container
        body = {
            "dewarId": dewar_id,
            "barcode": barcode,
            "code": list_filename,
            "containerType": container_type,
            "capacity": 96 * drop,
            "bltimeStamp": timestamp,
            # "containerTypeId": container_type_id,
        }
        r = requests.post(
            url + f"/api/dewars/{dewar_id}/containers", headers=headers, json=body
        )
        container_id = r.json()["containerId"]
        logger.info(f"Container id {container_id} registered for barcode {barcode}")

        # Register a container inspection
        body = {
            "containerId": container_id,
            "inspectionTypeId": 1,
            "blTimeStamp": timestamp,
            "state": "Completed",
        }
        r = requests.post(
            url + f"/api/containers/{container_id}/inspections",
            headers=headers,
            json=body,
        )

        inspection_id = r.json()["containerInspectionId"]

        # Add sample and image info
        for j in range(len(imagepaths)):

            body = {
                "containerId": container_id,
                "location": j + 1,
                "crystalId": crystal_id,
                "recordTimestamp": barcode,
            }  # name, recordTimeStamp
            r = requests.post(
                url + f"/api/containers/{container_id}/samples",
                headers=headers,
                json=body,
            )
            sample_id = r.json()["blSampleId"]

            body = {
                "blSampleId": sample_id,
                "containerInspectionId": inspection_id,
                "imageFullPath": str(imagepaths[j]),
            }  # micronsPerPixelX, micronsPerPixelY

            r = requests.post(
                url + f"/api/samples/{sample_id}/images", headers=headers, json=body
            )

            time.sleep(25 / 1000)  #

        logger.info(
            f"Sample image info registered to container id {container_id}, barcode {barcode}"
        )

        command = f"zocalo.go -f {ZOCALO_RECIPE} -s list_filename={list_filename} -n"
        logger.info(f"Running zocalo recipe command: {command}")
        subprocess.Popen(command, shell=True)

    except Exception as e:
        logging.error("Error processing barcode {barcode}", exc_info=True)
