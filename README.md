# gator
GAlaxy Tools for Obsids and RTS


## Motivation
Downloading obsids for the MWA can be a tedious, time-consuming task. The root of this problem lies with having the majority of data stored on tapes for long-term storage. `gator` aims to lessen the cognitive load required to download and process large amounts of data.

These scripts probably mean little to anyone not using the galaxy supercomputer, but they could be modified. I am, however, more interested in keeping things simple, and supporting only galaxy for the moment.


## Requirements
### Software
+ `Ruby` - the version on galaxy (`/usr/bin/ruby`) should work fine.

### Environment variables
+ `MWA_DIR` - the directory in which data is download. For EoR, this should be `/scratch2/mwaeor/MWA`. Obsids are actually contained in `$MWA_DIR/data/`.
+ `MYGROUP` - your personal group directory. For EoR, this should be `/group/mwaeor/$USER`.


## Usage
### Downloading obsids
`gator_add_to_download_table.rb` is used to create and add to an SQLite database which keeps track of all the obsids you want to download. It takes obsids from the command line, and files containing obsids; e.g.:

`gator_add_to_download_table.rb /path/to/obsids.txt 1065880008 1065880128`

Optionally, you can specify the path to the database with `-d /path/to/database`. The database will be created if it does not already exist. By default, the database used is located at `$MYGROUP/obsids.sqlite`.

Once the database has been created and is populated with obsids to be downloaded, run `gator_download_daemon.rb`. You should run this from a clean directory to keep all the slurm outputs from cluttering your workspace. This script is used to continuously submit download jobs, until all obsids in the database have been downloaded. It effectively runs every minute, and will print an update on newly submitted jobs and the output of completed jobs. The script automatically exits when everything is done. If a non-default database is to be used, again, use the `-d` flag.

To have a look at the contents of a database, use `gator_read_table.rb`.


### Processing with the RTS
Still heavily under development.


## Bugs? Inconsistencies?
Probably! If you find something odd, let me know and I'll attempt to fix it ASAP. Pull requests are also welcome.


## gator?
I'm bad at naming things. Sorry.
