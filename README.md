# formulatrix_pipe

This is the luigi pipeline used to transfer images from the formulatrix imagers at the Research Complex and rank them so that they can be viewed in TeXRank.


## Setup
Almost everything you need to set up and run the pipeline is here. You can set up the conda environment from within the cloned repo with `conda create --name imager_pipe --file spec-file.txt`.  You also need to install pytds `pip install sqlalchemy-pytds` after activating the new environment.

At Diamond, you can load anaconda before you create an env for the pipeline with `module load python/3.7`

You will also need the runtime for MATLAB: MCR (we've used R2012a (v7.17)), available from http://www.mathworks.co.uk/products/compiler/mcr/

At Diamond, this already exists at `/dls/science/groups/i04-1/software/MCR/r2012a/v717`, and this path is hardcoded into `run_ranker.RunRanker`. If the location of the runtime changes, then you will need to change this path.

If for some reason you need to add a new plate type or imager, you will need to generate a plate type and generate the new background images. There are instructions for this below, and the XChem staff should know how to generate the `.mat` files that are needed. 

## Running Luigi

Luigi is the task manager that runs the pipeline. It's documentation is here: https://luigi.readthedocs.io/en/stable/. For luigi to run, a deamon is required. At diamond, this is currently running on cs04r-sc-vserv-137.diamond.ac.uk.

To run luigi on a machine (needs to run constantly, so should be a dedicated server):

```nohup luigid >/dev/null 2>&1 &```

To kill the daeomon and all child processes:

```pkill -9 luigi```

To kick off the pipeline to run every minute, add the following line to a cronjob:

```* * * * * /bin/bash -l -c "module load python/3.7; source activate imager_pipe; cd /dls/science/groups/i04-1/software/luigi_pipeline/imager_pipe; PYTHONPATH='.' luigi --module start_pipe StartPipe --production=True --workers 10 >> luigi.log 2>&1"```.

You must ensure that you have created the env `imager_pipe` as described in **Setup**


## Configuration

In order to look up plates in RockMaker and transfer images from the RCaH drive (temporary), a configuration file is needed. This should be called luigi.cfg, and should look like:

```
[ImageTransferConfig]
password = <password>
username = <username-with-domain> e.g. uzw12877@FED.CCLRC.AC.UK
machine = <shared-drive-name> e.g. \\\\RI1000-0081\\RockMakerStorage
options = <options-for-smb-client> e.g. -m SMB2 (required protocol for diamond)

[RockMakerDBConfig]
server = <server-name> e.g. DIAMRISQL01\ROCKIMAGER
database = <database-name> e.g. RockMaker
username = <username>
password = <password>
```

This file is not in this repository, for obvious reasons

## Directory Structure

For luigi to keep track of what tasks have been done, some files are created. There are also some files required for ranker. These live in particular locations in this directory. The structure looks like this:

```
formulatrix_pipe
 | -barcodes_2drop - where info about 2drop (mrc2d) plates is stored
 | |-<barcode>.csv - a csv file containing info about <barcode> scraped from the RockMaker DB
 |
 |-barcodes_3drop - where info about 3drop (SwissSci3D) plates is stored
 | |-<barcode>.csv - a csv file containing info about <barcode> scraped from the RockMaker DB
 |
 |-messages
 | |-<barcode_date-imaged_imager-platetype.txt> - text that is emailed when a plate is ranked
 |
 |-ranker_jobs
 | |-<barcode_date-imaged_imager-platetype.sh> - a bash script submitted to the cluster to run ranker
 |
 |-SubwellImages
 | |-<barcode_date-imaged_imager-platetype> - directory for subwell images
 | | |-<barcode_well-number_well-letter_drop-number.jpg> - drop image
 |
 |-transfers
 | |-<barcode_date-imaged_imager_plate-type.done> - an empty file signifying that all images have been transferred
 |
 |-<imager-platetype-subwell.mat> - matlab files for ranker (from calibration)
 |
 |-Data - the files output from ranker ----------------
 |-LogFiles - ranker log files                         |
 |-TeXRankE.exe - the executable for texrank           |  ------> ranker / TexRank files
 |-RankerE - ranker executable ------------------------
 ```

## Adding a plate type

The plate types used are translated from the directory structure used in RockMaker, which looks something like:
```
XChem
|- Plate Type (e.g. SwissSci3D)
| |- Barcode
| | |- Data
```
The plate types are translated in ```get_barcodes.py``` to make them shorter and more understandable:

class GetPlateTypes(luigi.Task):
```
    .....
    
    # this translates the directory names in RockMaker to names for the pipeline
    translate = {'SWISSci_2drop':'2drop', 'SWISSci_3Drop':'3drop'}
    
    .....
    
     def requires(self):
        
        .....

        # find the directory names corresponding to plate types for all entries in the XChem folder
        # XChem
        # |- Plate Type (e.g. SwissSci3D)
        #    |- Barcode
        #       |- Data
        c.execute("SELECT TN3.Name as 'Name' From Plate " \
                  "INNER JOIN TreeNode TN1 ON Plate.TreeNodeID = TN1.ID " \
                  "INNER JOIN TreeNode TN2 ON TN1.ParentID = TN2.ID " \
                  "INNER JOIN TreeNode TN3 ON TN2.ParentID = TN3.ID " \
                  "INNER JOIN TreeNode TN4 ON TN3.ParentID = TN4.ID " \
                  "where TN4.Name='Xchem'")

        .....

        for plate in plate_types:
            # connect to the RockMaker DB
            conn = pytds.connect(self.server, self.database, self.username, self.password)
            c = conn.cursor()

            # For each plate type, find all of the relevant barcodes
            c.execute("SELECT Barcode FROM Plate " \
                      "INNER JOIN TreeNode as TN1 ON Plate.TreeNodeID = TN1.ID " \
                      "INNER JOIN TreeNode as TN2 ON TN1.ParentID = TN2.ID " \
                      "INNER JOIN TreeNode as TN3 ON TN2.ParentID = TN3.ID " \
                      "INNER JOIN TreeNode as TN4 ON TN3.ParentID = TN4.ID " \
                      "where TN4.Name='Xchem' AND TN3.Name like %s", (str('%' + plate + '%'),))

            rows = c.fetchall()
            for row in rows:
                barcodes.append(str(row[0]))
                # translate the name from RockMaker (UI) strange folders to 2drop or 3drop (in transfer parameter)
                if plate in self.translate.keys():
                    plates.append(self.translate[plate])
                    
```
So to add a new plate type, add it to the translate dictionary, where the key is the name you have given it in the RockMaker directory structure, and the value is the name you wish it to have in formulatrix_pipe

## Adding a mat file for ranker

In the directory structure above, the ranker file is given as ```<imager-platetype-subwell.mat>```. This name does not really matter, as far as the pipeline is concerned, as there is a translation made between it's own naming conventions (from RockMaker) and the mat files. This is done in ```run_ranker.py```:
```
class RunRanker(luigi.Task):
   imager = luigi.Parameter()
   plate = luigi.Parameter()
   plate_type = luigi.Parameter()
   
.....

def run(self):
       current_directory = os.getcwd()
       # dictionary to translate imager code and pipelines plate types to the right matlab file for ranker
       lookup = {
            'RI1000-0080_2drop': glob.glob('RI1000-0080-2drop*.mat'),
            'RI1000-0080_3drop': glob.glob('RI1000-0080-3drop*.mat'),
            'RI1000-0081_2drop': glob.glob('RI1000-0081-2drop*.mat'),
            'RI1000-0081_3drop': glob.glob('RI1000-0081-3drop*.mat'),
            'RI1000-0276_2drop': glob.glob('RI1000-0276-2drop*.mat'),
            'RI1000-0276_3drop': glob.glob('RI1000-0276-3drop*.mat'),
            'RI1000-0082_2drop': glob.glob('RI1000-0082-2drop*.mat'),
            'RI1000-0082_3drop': glob.glob('RI1000-0082-3drop*.mat')
        }
       # define what we want to lookup in the above dict
       lookup_string = str(self.imager + '_' + self.plate_type)
 ```      
 
So to add a definition, just add it to the lookup dictionary, where the key is the imager name (from RockMakerDB) an underscore, and then the plate type, as defined in the section "Adding a plate type"

## Fixing Issues

### `Empty response`

The SLURM API cannot be reached at the provided URL. If there has been a SLURM upgrade, then it could be that the URL's and/or authentication token are out of date.

- In `run_ranker.py`, update the URL's to the correct API version number
- Generate a new authentication token and place it in `slurm_token.json`:

```bash
scontrol token username=$(whoami) lifespan=300000000
```
