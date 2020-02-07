from collections import OrderedDict
from collections import Counter
import csv
import datetime
import errno
import math
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import uuid
import xml
import lxml
from lxml import etree
import tempfile
import fnmatch
from tkinter import *
import tkinter.filedialog
from tkinter import ttk
import glob
import pickle
import time
import openpyxl
import glob
import hashlib
import psutil

# from dfxml project
import Objects

from bdpl_ingest import *
from bdpl_ripstation_ingest import *

def main():
    
    #make sure antivirus and siegfried definitions are updated
    update_software()

    while True:
        unit_name = input("\nEnter unit abbreviation: ")
        
        shipmentDate = input("\nEnter shipment ID: ")
        
        ship_dir = os.path.join('Z:\\', unit_name, 'ingest', shipmentDate)
        print(ship_dir)
        if os.path.exists(ship_dir):
            break
        else:
            continue

    #Need to get legacy media_logs to pull out info...
    spreadsheet = os.path.join(ship_dir, '%s_%s.xlsx' % (unit_name, shipmentDate))

    if not os.path.exists(spreadsheet):
        print('\nWARNING: %s does not exist.  Check file name and ensure it is saved to %s.' % (spreadsheet, ship_dir))
        sys.exit(1)

    #track status in these files
    failed_ingest = os.path.join(ship_dir, 'failed_legacy_ingest.txt')
    untarred = os.path.join(ship_dir, 'untarred_legacy.txt')
    metadata_loaded = os.path.join(ship_dir, 'metadata_legacy.txt')
    replicated = os.path.join(ship_dir, 'replicated_legacy.txt')
    analyzed = os.path.join(ship_dir, 'analyzed_legacy.txt')
    found_folders = os.path.join(ship_dir, 'found_folders.txt')
    tar_manifest = = os.path.join(ship_dir, 'tarred_files.txt')

    #change to ship_dir to run tar command
    os.chdir(ship_dir)

    #make a manifest of all tar.gz files
    if not os.path.exists(tar_manifest):
        with open(tar_manifest, 'a') as outfile:
            for t in [x for x in os.listdir() if 'tar.gz' in x]:
                outfile.write('%s\n' % t)
    
    with open(tar_manifest, 'r') as infile:
        tar_files = infile.read().splitlines()
    
    for tar_file in tar_files:
        
        item_barcode = os.path.basename(tar_file).split('.', 1)[0]
        
        print('\n\nWORKING ON ITEM %s' % item_barcode)
        
        #skip item if on failed list
        if check_list(failed_ingest, item_barcode):
            print('\nItem has previously failed; check report and address issues.  Moving on to next item...')
            continue
        
        folders = bdpl_folders(unit_name, shipmentDate, item_barcode)
        
        destination = folders['destination']
        files_dir = folders['files_dir']
        log_dir = folders['log_dir']
        imagefile = folders['imagefile']
        temp_dir = folders['temp_dir']
        reports_dir = folders['reports_dir']
        files_dir = folders['files_dir']
        image_dir = folders['image_dir']
        jobType_file = os.path.join(temp_dir, 'jobtype.txt')
        
        #files in legacy bdpl packages
        orig_imagefile = os.path.join(destination, '%s.dd' % item_barcode)
        orig_imagefile_md5 = os.path.join(destination, '%s.dd.md5' % item_barcode)
        guy_log = os.path.join(destination, '%s.info' % item_barcode)
        info_txt = os.path.join(destination, '%s-info.txt' % item_barcode)
        info_csv = os.path.join(destination, '%s-info.csv' % item_barcode)
        orig_dfxml = os.path.join(destination, '%s.xml' % item_barcode)
        
        #get jobType if already recorded
        if os.path.exists(jobType_file):
            with open(jobType_file, 'rb') as f:
                jobType = pickle.load(f)        
        
        if not check_list(untarred, item_barcode):
        
            print('\nEXTRACTING CONTENTS FROM TAR.GZ FILE...')
            try:
                output = subprocess.call('tar xvzf %s' % tar_file)
                write_list(untarred, item_barcode)
            except subprocess.CalledProcessError as exc:
                print('\n\tWARNING: failed to extract contents! Moving on to next item...')
                write_list(failed_ingest, '%s\t%s' % (item_barcode, exc.output))
                continue
                
            #get a list of tar contents; check to see if there are any folders.  If so, make note so we can inspect...
            tar_contents = os.listdir(destination)
            found_dir = [f for f in tar_contents if os.path.isdir(os.path.join(destination, f))]
            
            if len(found_dir) == 0:
                pass
            elif len(found_dir) > 1:
                print('\n\tWARNING: multiple folders found in tar.gz file! Moving on to next item...')
                write_list(failed_ingest, '%s\tMultiple folders in legacy tar.gz' % item_barcode)
                continue
            #if we only have 1 folder and it contains files, make note so we can explore later, if need be...
            elif len(found_dir) == 1:
                found_dir = found_dir[0]
                write_list(found_folders, '%s\tUnexpected folder in legacy tar.gz' % item_barcode)
                os.rename(os.path.join(destination, found_dir), os.path.join(destination, '%s_LEGACY' % found_dir))
        
        if not check_list(metadata_loaded, item_barcode):
            
            #retrieve metadata from spreadsheet to populate metadata_dict and create folders
            print('\nLOADING METADATA AND CREATING FOLDERS...')
            
            if not load_metadata(folders, item_barcode, spreadsheet):
                print('\n\tWARNING: issue with spreadsheet metadata!  Moving on to next item...')
                write_list(failed_ingest, "%s\tspreadsheet metadata doesn't exist" % item_barcode)
                continue
            
            #create working folders
            create_folders(folders)
            
            check_item_status(folders, item_barcode)
            
            #get label transcript information from info.txt/info.csv file
            transcription = []
            
            if os.path.exists(info_txt):
                with open(info_txt, 'r') as f:
                    txt = f.read().splitlines()

                for i in range(0, len(txt)):
                    if 'label transcription' in txt[i].lower():
                        transcription.append(txt[i].split(':', 1)[1].strip())
                        tmp = i
                        while True:
                            tmp+=1
                            if 'earliest creation' in txt[tmp].lower():
                                break
                            else:
                                transcription.append(txt[tmp].strip())
                
                #move to our metadata folder
                shutil.move(info_txt, reports_dir)
            
            elif os.path.exists(info_csv):
                with open(info_csv, 'r') as f:
                    txt = csv.reader(f)
                    
                    for row in txt:
                        if 'label transcription' in row[0].lower():
                            transcription.append(row[1].replace('\n', ' '))
                
                #move to our metadata folder
                shutil.move(info_csv, reports_dir)
            
            #add this transcription info to our info
            if len(transcription) > 0:
                metadata_dict = pickleLoad('metadata_dict', folders, item_barcode)
                if metadata_dict['label_transcription'] == '-':
                    metadata_dict['label_transcription'] = ' '.join(transcription)
                else: 
                    #make sure that transcription isn't identical; if there are any differences, just append info from .txt/.csv file
                    if metadata_dict['label_transcription'].lower().split() != ' '.join(transcription).lower().split():
                        metadata_dict['label_transcription'] = '%s | %s' % (metadata_dict['label_transcription'], ' '.join(transcription))
                pickleDump('metadata_dict', metadata_dict, folders)
                
            #update status
            write_list(metadata_loaded, item_barcode)
        
        if not check_list(replicated, item_barcode):
            if os.path.exists(orig_imagefile):
            
                #parse guymager log file; create premis.  Get guymager version; timestamp; linux device, output.
                if os.path.exists(guy_log):
                    with open(guy_log, 'r') as f:
                        lines = f.read().splitlines()
                        if lines[1] != 'GUYMAGER ACQUISITION INFO FILE':
                            print('\n\tWARNING: disk image created with tool other than Guymager!  Moving on to next item...')
                            write_list(failed_ingest, '%s\tdisk image created with tool other than Guymager' % item_barcode)
                            continue
                        for line in lines:
                            if line.startswith('Version'):
                                guy_ver = 'Guymager %s' % line.split(':')[1].strip()
                            if line.startswith('Ended'):
                                timestamp = line.split(':', 1)[1].split('(')[0].strip()
                            if line.startswith('Linux device'):
                                device_name = line.split(':')[1].strip()
                            if line.startswith('Image path and file name'):
                                output_file = line.split(':')[1].strip()
                            if line.startswith('State'):
                                if 'Finished successfully' in line:
                                    exitcode = 0
                                else:
                                    exitcode = 1
                    
                    guy_cmd = 'Guymager (GUI) %s %s' % (device_name, output_file)
                    
                    premis_list = pickleLoad('premis_list', folders, item_barcode)
                    premis_list.append(premis_dict(timestamp, 'disk image creation', exitcode, guy_cmd, 'Extracted a disk image from the physical information carrier.', guy_ver))
                    pickleDump('premis_list', premis_list, folders)
                    
                    #move guymager log
                    shutil.move(guy_log, os.path.join(log_dir, 'guymager.log'))
                
                #move image file and guymager log
                shutil.move(orig_imagefile, imagefile)
                
                #delete original dfxml and dd.md5 files
                if os.path.exists(orig_dfxml):
                    os.remove(orig_dfxml)
                if os.path.exists(orig_imagefile_md5):
                    os.remove(orig_imagefile_md5)
                
                #get info on the disk image (fsstat, ils, mmls, and disktype)
                disk_image_info(folders, item_barcode)
                
                #need to check if the imagefile has a filesystem associated with optical media (UDF or ISO9660)
                fs_list = pickleLoad('fs_list', folders, item_barcode)
                secureCopy_list = ['udf', 'iso9660']
                if any(fs in ' '.join(fs_list) for fs in secureCopy_list):
                    print('\nADDITIONAL STEPS FOR ISO9660/UDF FILE SYSTEM...')
                    
                    #mount imagefile using PowerShell; requires changing file extention to .ISO
                    iso_imagefile = os.path.join(image_dir, '%s.iso' % item_barcode)
                    os.rename(imagefile, iso_imagefile)
                    exitcode = mount_iso(orig_imagefile)
                    if exitcode != 0:
                        os.rename(iso_imagefile, imagefile)
                        print('\nWARNING: failed to mount disk image!  Moving on to next item...')
                        write_list(failed_ingest, '%s\tFailed to mount disk image' % item_barcode)
                        continue
                  
                    #run lsdvd to determine if jobType is DVD-Video or Disk_Image.  First we'll need to get the drive letter
                    print('\nCHECKING IF DISC IS DATA OR DVD-VIDEO...')
                    drive_letter = get_iso_drive_letter(iso_imagefile)
                    titlecount = lsdvd_check(folders, item_barcode, drive_letter)
                    
                    #set jobType based on titlecount; if no DVD titles, then it's just a disk image...
                    if titlecount == 0:
                        jobType = 'Disk_image'
                        
                        with open(jobType_file, 'wb') as f:
                            pickle.dump(jobType, f)
                        
                        #replicate content using TeraCopy; then dismount disk image and change file extension back to .dd
                        secureCopy(drive_letter, folders, item_barcode)
                        dismount_iso(iso_imagefile)
                        os.rename(iso_imagefile, imagefile)
                    else:
                        jobType = 'DVD'
                        
                        with open(jobType_file, 'wb') as f:
                            pickle.dump(jobType, f)
                        
                        #create .MPG videos for all titles on disk
                        normalize_dvd_content(folders, item_barcode, titlecount, drive_letter)
                        
                        #dismount disk image
                        exitcode = dismount_iso(orig_imagefile)
                        if exitcode != 0:
                            print('\nWARNING: failed to dismount disk image!  Moving on to next item...')
                            write_list(failed_ingest, '%s\tFailed to dismount disk image' % item_barcode)
                            continue
                        
                        #rename to '.dd' file extension
                        os.rename(iso_imagefile, imagefile)
                
                else:
                    jobType = 'Disk_image'
                        
                    with open(jobType_file, 'wb') as f:
                        pickle.dump(jobType, f)
                        
                    disk_image_replication(folders, item_barcode)
                
            else:
                #if we've previously found a file with folders, assume that this job was 'copy only'.  Rename folder according to our convention.
                if check_list(found_folders, item_barcode) and checkFiles(os.path.join(destination, '%s_LEGACY' % found_dir)):
                    print('\nWARNING: Could be a copy_only job; need to determine if we can record PREMIS event!  Moving on to next item...')
                    write_list(failed_ingest, '%s\tCopy only; verify PREMIS information' % item_barcode)
                    continue
                    # os.rename(os.path.join(destination, '%s_LEGACY' % found_dir), files_dir)
                    
                    # jobType = 'Copy_only'
                    
                    # with open(jobType_file, 'wb') as f:
                            # pickle.dump(jobType, f)
            
                #otherwise, fail barcode: job type needs to be investigated
                else:
                    print('\nWARNING: TAR.GZ does not appear to contain data!  Moving on to next item...')
                    write_list(failed_ingest, '%s\tUnable to ID content' % item_barcode)
                    continue
            
            #update status        
            write_list(replicated, item_barcode)
            
        
        if not check_list(analyzed, item_barcode):
            #now set variables for analysis procedures
            analysis_vars = {'platform' : 'bdpl_legacy', 'jobType' : jobType, 're_analyze' : False, 'gui_vars' : {}}
            
            #send content through analysis
            analyzeContent(unit_name, shipmentDate, item_barcode, analysis_vars)
            
            #TO DO: need to verify if procedures actually completed...
            write_list(analyzed, item_barcode)
                
        if os.path.exists(tar_file):
            print('\nREMOVING TAR FILE...')
            os.remove(tar_file)
            
        print('\n%s COMPLETED\n-------------------------------------------------------' % item_barcode)   

    #once we've gone through all TAR files, check for any failures or 'found folders'
    with open(analyzed, 'r') as f:
        analyzed_stats = f.readlines()
    if os.path.exists(failed_ingest):
        with open(failed_ingest, 'r') as f:
            failed_stats = f.readlines()
    else:
        failed_stats = []
    with open(found_folders, 'r') as f:
        found_stats = f.read().splitlines()
        
    print('\n\nAll TAR.GZ files have been processed.\n\tTotal files: %s\n\tCompleted: %s\n\tFailed: %s' % (len(tar_files), len(analyzed_stats), len(failed_stats)))
    
    if len(found_stats) > 0:
        print('\nNOTE: folders found in the following files; delete if not needed or re-run analysis if necessary:\n\t%s' % '\n\t'.join(found_stats))
    


if __name__ == '__main__':
    main()