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

# from dfxml project
import Objects

from bdpl_ingest import *

def mount_iso(iso_imagefile):
    print('\nMOUNTING .ISO DISK IMAGE FILE...')
    cmd = "Mount-DiskImage -ImagePath '%s'" % iso_imagefile
    exitcode = subprocess.call('powershell "%s" > null 2>&1' % cmd)
    
    return exitcode
    
def dismount_iso(iso_imagefile):
    print('\nDISMOUNTING DISK IMAGE FILE...')
    cmd = "Dismount-DiskImage -ImagePath '%s'" % iso_imagefile
    exitcode = subprocess.call('powershell "%s" > null 2>&1' % cmd)
    
    return exitcode
    
def get_iso_drive_letter(iso_imagefile):
    cmd = "(Get-DiskImage '%s' | Get-Volume).DriveLetter" % iso_imagefile
    drive_letter = '%s:\\' % subprocess.check_output('powershell "%s"' % cmd, text=True).rstrip()
    
    return drive_letter

def check_list(list_name, item_barcode):
    if not os.path.exists(list_name):
        return False
    with open(list_name, 'r') as f:
        for item in f:
            if item_barcode in item.strip():
                return True
            else:
                continue
        return False

def main():

    while True:
        unit_name = input('\nEnter unit abbreviation: ')
        
        shipmentDate = input('\nEnter shipment date: ')
        
        ship_dir = os.path.join('Z:\\', unit_name, 'ingest', shipmentDate)
        
        if os.path.exists(ship_dir):
            break
        else:
            continue
            
    userdata = os.path.join(ship_dir, 'userdata.txt')
    if not os.path.exists(userdata):
        print('\nWARNING: Could not locate userdata.txt file with item barcodes.  Be sure file is located in %s and run script again.' % ship_dir)
        sys.exit(1)
        
    spreadsheet = os.path.join(ship_dir, '%s_%s.xlsx' % (unit_name, shipmentDate))
    if not os.path.exists(spreadsheet):
        print('\nWARNING: Could not locate %s in shipment folder.  Be sure spreadsheet is present and correctly named: %s_%s.xlsx and then run script again.' % (spreadsheet, unit_name, shipmentDate))
        sys.exit(1)
    
    #track any failures in this file
    failed_ingest = os.path.join(ship_dir, 'failed_ingest.txt')
    replicated = os.path.join(ship_dir, 'replicated.txt')
    analyzed = os.path.join(ship_dir, 'analyzed.txt')

    #Once we verify that we have our list of barcodes and our spreadsheet, we will loop through list
    with open(userdata, 'r') as ud:
        barcodes = [bc for bc in ud.read().splitlines()]
        
    #now loop through all barcodes
    for item_barcode in barcodes:
        print('\n\n---------------------------------------------------------------------------')
        print('\nWorking on item: %s' % item_barcode)
        
        #get folder variables
        folders = bdpl_folders(unit_name, shipmentDate, item_barcode)
        
        if not check_list(replicated, item_barcode):
        
            #make sure disk image exists
            print('\nCHECKING IF DISK IMAGE EXISTS...')
            image_dir = folders['image_dir']
            iso_imagefile = os.path.join(image_dir, '%s.iso' % item_barcode)
            if not os.path.exists(iso_imagefile):
                print('\nWARNING: disk image does not exist!  Moving on to next item...')
                with open(failed_ingest, 'a') as f:
                    f.write('%s\tDisk image does not exist\n' % item_barcode)
                continue
                
            #get timestamp for disk image
            timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(iso_imagefile)).isoformat()
            
            #write premis information for disk image creation.  Even if image is unreadable, we assume that this operation was successful
            premis_list = pickleLoad('premis_list', folders, item_barcode)
            premis_list.append(premis_dict(timestamp, 'disk image creation', 0, 'RipStation BR6-7604 batch operation', 'Extracted a disk image from the physical information carrier.', 'RipStation v.4.0'))
            pickleDump('premis_list', premis_list, folders)
            
            #run 'first_run' function to get metadata and create folders; if 'false' return
            print('\nLOADING METADATA AND CREATING FOLDERS...')
            gui_vars = {'platform' : 'bdpl_ripstation'}
            status, str = first_run(unit_name, shipmentDate, item_barcode, gui_vars)
            if not status:
                print('\nWARNING: issue with spreadsheet metadata!  Moving on to next item...')
                with open(failed_ingest, 'a') as f:
                    f.write('%s\t%s\n' % (item_barcode, str))
                continue
                
            #mount .ISO so we can verify disk image type
            exitcode = mount_iso(iso_imagefile)
            if exitcode != 0:
                print('\nWARNING: failed to mount disk image!  Moving on to next item...')
                with open(failed_ingest, 'a') as f:
                    f.write('%s\tFailed to mount disk image\n' % (item_barcode, str))
                continue
            
            #set mediaStatus variable: confirms that 'media' (mounted disk image) is present; required by bdpl_ingest functions
            mediaStatus = True
            
            #get drive letter for newly mounted disk image
            drive_letter = get_iso_drive_letter(iso_imagefile)
            
            #run lsdvd to determine if jobType is DVD-Video or Disk_Image
            print('\nCHECKING IF DISC IS DATA OR DVD-VIDEO...')
            titlecount = lsdvd_check(folders, item_barcode, drive_letter)
            
            #set jobType based on titlecount
            if titlecount == 0:
                jobType = 'Disk_image'
                
                #dismount disk image
                exitcode = dismount_iso(iso_imagefile)
                if exitcode != 0:
                    print('\nWARNING: failed to dismount disk image!  Moving on to next item...')
                    with open(failed_ingest, 'a') as f:
                        f.write('%s\tFailed to dismount disk image\n' % (item_barcode, str))
                    continue
                
                #rename to '.dd' file extension
                imagefile = '%s.dd' % os.path.splitext(iso_imagefile)[0]
                os.rename(iso_imagefile, imagefile)
                
                #get info on the disk image (fsstat, ils, mmls, and disktype)
                disk_image_info(folders, item_barcode)
                
                #create a logical copy of content on disk image. This is a little messy, but it seems a little better than making another copy of the disk image...
                fs_list = pickleLoad('fs_list', folders, item_barcode)
                secureCopy_list = ['udf', 'iso9660']
                if any(fs in ' '.join(fs_list) for fs in secureCopy_list):
                    print('\nADDITIONAL STEPS FOR ISO9660/UDF FILE SYSTEM...')
                    os.rename(imagefile, iso_imagefile)
                    mount_iso(iso_imagefile)
                    drive_letter = get_iso_drive_letter(iso_imagefile)
                    secureCopy(drive_letter, folders, item_barcode)
                    dismount_iso(iso_imagefile)
                    os.rename(iso_imagefile, imagefile)
                else:
                    disk_image_replication(folders, item_barcode)
                
            else:
                jobType = 'DVD'
                
                #create .MPG videos for all titles on disk
                normalize_dvd_content(folders, item_barcode, titlecount, drive_letter)
                
                #dismount disk image
                print('\nDISMOUNTING DISK IMAGE FILE...')
                cmd = "Dismount-DiskImage -ImagePath '%s'" % iso_imagefile
                exitcode = subprocess.call('powershell "%s" > null 2>&1' % cmd)
                if exitcode != 0:
                    print('\nWARNING: failed to dismount disk image!  Moving on to next item...')
                    with open(failed_ingest, 'a') as f:
                        f.write('%s\tFailed to dismount disk image\n' % (item_barcode, str))
                    continue
                
                #rename to '.dd' file extension
                imagefile = '%s.dd' % os.path.splitext(iso_imagefile)[0]
                os.rename(iso_imagefile, imagefile)
            
            with open(replicated, 'a') as f:
                f.write('%s\n' % item_barcode)
            
        if not check_list(analyzed, item_barcode):
            #now set variables for analysis procedures
            analysis_vars = {'platform' : 'bdpl_ripstation', 'jobType' : jobType, 're_analyze' : False, 'gui_vars' : {}}
            
            #send content through analysis
            analyzeContent(unit_name, shipmentDate, item_barcode, analysis_vars)
            
            with open(analyzed, 'a') as f:
                f.write('%s\n' % item_barcode)
                
            print('\n%s COMPLETED!!!!' % item_barcode)
            
if __name__ == '__main__':
    main()
    