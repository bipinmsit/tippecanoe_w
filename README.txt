ABOUT SCRIPT
*************************************************************
process_25_data.py

 - Script to rearrange files, convert into mbtiles & its merging, delete old tilesets from mapbox and latest upload

 - Run the above script by below steps:

----------------------------------------------------
1. Go to windows search option & type Ubuntu

2. Open Ubuntu 20.04 terminal on window

3. Navigate to python script folder e.g (cd /mnt/d/ - if it is D:\ Drive) & execute the below command

 >>python3 process_25_data.py --dir <sigwx_dir>

   Where,
	<sigwx_dir> - Path of all 0.25 degree geojson files e.g (cd /mnt/d)

*********************************************************************
DEPENDENCIES NEED TO INSTALL
---------------------------------------------------------------------
Ubuntu Terminal Dependencies:
 - Install from here https://ubuntu.com/wsl

Python Dependencies: (To install simply type 'pip install <package_name>' in ubuntu terminal)
 - mapbox 0.16.1
 - mapbox-tilesets 1.8.1