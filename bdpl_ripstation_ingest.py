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

def mount_iso(orig_imagefile):
    print('\nMOUNTING .ISO DISK IMAGE FILE...')
    cmd = "Mount-DiskImage -ImagePath '%s'" % orig_imagefile
    exitcode = subprocess.call('powershell "%s" > null 2>&1' % cmd)
    
    return exitcode
    
def dismount_iso(orig_imagefile):
    print('\nDISMOUNTING DISK IMAGE FILE...')
    cmd = "Dismount-DiskImage -ImagePath '%s'" % orig_imagefile
    exitcode = subprocess.call('powershell "%s" > null 2>&1' % cmd)
    
    return exitcode
    
def get_iso_drive_letter(orig_imagefile):
    cmd = "(Get-DiskImage '%s' | Get-Volume).DriveLetter" % orig_imagefile
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

def write_list(list_name, message):
    with open(list_name, 'a') as f:
        f.write('%s\n' % message)

def main():

    update_software()
    
    while True:
        unit_name = input('\nEnter unit abbreviation: ')
        
        shipmentDate = input('\nEnter shipment date: ')
        
        ship_dir = os.path.join('Z:\\', unit_name, 'ingest', shipmentDate)
        
        if os.path.exists(ship_dir):
            break
        else:
            continue
    
    print('\n\nOptions:\n\tC: CDDA\n\tD: DVD/Data discs')
    while True:
        rip_option = input('\nIndicate RipStation job (C or D): ').strip().lower()
        if rip_option == 'c':
            rip_option = 'CDs'
            jobType = 'CDDA'
            break
        elif rip_option == 'd':
            rip_option = 'DVD_Data'
            break
        else:
            continue
            
    userdata = os.path.join(ship_dir, 'userdata.txt')
    if not os.path.exists(userdata):
        print('\nWARNING: Could not locate userdata.txt file with item barcodes.  Be sure file is located in %s and run script again.' % ship_dir)
        sys.exit(1)
        
    spreadsheet = os.path.join(ship_dir, '%s_%s.xlsx' % (unit_name, shipmentDate))
    if not os.path.exists(spreadsheet):
        print('\nWARNING: Could not locate %s in shipment folder.  Be sure spreadsheet is present and correctly named (%s_%s.xlsx) and then run script again.' % (spreadsheet, unit_name, shipmentDate))
        sys.exit(1)
    
    #track status in these files
    failed_ingest = os.path.join(ship_dir, 'failed_ingest_ripstation.txt')
    replicated = os.path.join(ship_dir, 'replicated_ripstation.txt')
    analyzed = os.path.join(ship_dir, 'analyzed_ripstation.txt')
    
    #get ripstation log (and its timestamp)
    if rip_option == 'DVD_Data':
        rs_log = os.path.join(ship_dir, 'Log.txt')
    else:
        rs_log = os.path.join(ship_dir, 'log_cdda.txt')
        
    if not os.path.exists(rs_log):
        print('\nWARNING: Could not locate RipStation log in shipment folder.  Be sure file is present and correctly named (Log.txt or log_cdda.txt) and then run script again.')
        sys.exit(1)
        
    #set timestamp variable so we can get YYYY-MM-DD info (ripstation logs only include HH:MM:SS)
    rs_timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(rs_log)).strftime('%Y-%m-%d')

    with open(userdata, 'r') as ud:
        barcodes = ud.read().splitlines()
        
    #loop through all barcodes
    for item_barcode in barcodes:
        print('\nWorking on item: %s' % item_barcode)
        
        #if item has already failed, skip it.
        if check_list(failed_ingest, item_barcode):
            print('\nThis item previously failed.  Moving on to next item...')
            continue
        
        #get folder variables
        folders = bdpl_folders(unit_name, shipmentDate, item_barcode)
        log_dir = folders['log_dir']
        files_dir = folders['files_dir']
        image_dir = folders['image_dir']
        temp_dir = folders['temp_dir']
        ripstation_log = os.path.join(log_dir, 'ripstation.txt')
        jobType_file = os.path.join(temp_dir, 'jobtype.txt')
        
        #get jobType if already recorded
        if os.path.exists(jobType_file):
            with open(jobType_file, 'rb') as f:
                jobType = pickle.load(f)
        
        if not check_list(replicated, item_barcode):
                
            #run 'first_run' function to get metadata and create folders; if 'false' return
            print('\nLOADING METADATA AND CREATING FOLDERS...')
            gui_vars = {'platform' : 'bdpl_ripstation'}
            status, msg = first_run(unit_name, shipmentDate, item_barcode, gui_vars)
            if not status:
                print('\nWARNING: issue with spreadsheet metadata!  Moving on to next item...')
                write_list(failed_ingest, '%s\t%s' % (item_barcode, msg))
                continue

            premis_list = pickleLoad('premis_list', folders, item_barcode)
            
            if rip_option == 'CDs':
                
                #set variables
                wav_file = os.path.join(files_dir, "%s.wav" % item_barcode)
                wav_cue = os.path.join(files_dir, "%s.cue" % item_barcode)
                cdr_bin = os.path.join(image_dir, "%s.bin" % item_barcode)
                cdr_toc = os.path.join(image_dir, "%s.toc" % item_barcode)
                cue = os.path.join(image_dir, "%s.cue" % item_barcode)
                
                #make sure required components are present
                if not '.wav' or not '.cue' in [os.path.splitext(x)[1] for x in os.listdir(files_dir)]:
                    print("\nMissing '.wav' or '.cue' file; moving on to next item...")
                    write_list(failed_ingest, '%s\tMissing .wav or .cue file' % item_barcode)
                    continue
                
                #write premis information for creating WAV; we assume that this operation was successful
                timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(wav_file)).isoformat()
                premis_list.append(premis_dict(timestamp, 'normalization', 0, 'RipStation BR6-7604 batch .WAV file creation', 'Transformed object to an institutionally supported preservation format (.WAV).', 'RipStation V4.4.13.0'))
                pickleDump('premis_list', premis_list, folders)
                
                #save ripstation log information for disc to log_dir.  Have to get album # from txt file...
                txt_file = os.path.join(files_dir, [x for x in os.listdir(files_dir) if os.path.splitext(x)[1] == '.txt'][0])
                album_number = os.path.splitext(os.path.basename(txt_file))[0]
                
                ripstation_log = os.path.join(log_dir, 'ripstation.txt')
                if not os.path.exists(ripstation_log):
                    with open(ripstation_log, 'a') as outf:
                        outf.write('RipStation V4.4.13.0\n')
                        with open(rs_log, 'r') as inf:
                            for line in inf.read().splitlines():
                                if album_number in line:
                                    outf.write('%s %s\n' % (rs_timestamp, line))
                
                #get info about wav file
                print('\nSTEP 1: FORMAT NORMALIZATION TO .BIN\n\n')
                cmd = 'ffprobe -i %s -hide_banner -show_streams -select_streams a' % wav_file
                audio_info = subprocess.check_output(cmd, shell=True, text=True).split('\n')
                
                audio_dict = {}
                
                for a in audio_info:
                    if '=' in a:
                        audio_dict[a.split('=')[0]] = a.split('=')[1]
                
                sample_rate = audio_dict['sample_rate']
                channels = audio_dict['channels']
                
                #now create bin file with raw 16 bit little-endian PCM 
                cmd = 'ffmpeg -y -i %s -hide_banner -ar %s -ac %s -f s16le -acodec pcm_s16le %s' % (wav_file, sample_rate, channels, cdr_bin)
                timestamp = str(datetime.datetime.now())
                exitcode = subprocess.call(cmd, shell=True)
                
                ffmpeg_ver = '; '.join(subprocess.check_output('"C:\\Program Files\\ffmpeg\\bin\\ffmpeg" -version', shell=True, text=True).splitlines()[0:2])

                #record premis
                premis_list = pickleLoad('premis_list', folders, item_barcode)
                premis_list.append(premis_dict(timestamp, 'normalization', exitcode, cmd, 'Transformed object to an institutionally supported preservation format (.BIN)', ffmpeg_ver))
                
                #get path of the original cue file produced by RipStation
                orig_cue = os.path.join(files_dir, [x for x in os.listdir(files_dir) if os.path.splitext(x)[1] == '.cue'][0])
                
                #correct cue file; save to file_dir.  
                with open(wav_cue, 'w') as outfile:
                    with open(orig_cue, 'r') as infile:
                        for line in infile.readlines():
                            if line.startswith('FILE'):
                                outfile.write(line.replace('WAV1', 'WAVE'))
                            elif line.startswith('  TRACK') or line.startswith('    INDEX'):
                                outfile.write(line)
                
                #copy corrected cue file to image_dir; correct FILE reference
                with open(cue, 'w') as outfile:
                    with open(wav_cue, 'r') as infile:
                        for line in infile.readlines():
                            if line.startswith('FILE'):
                                outfile.write('FILE "%s.bin" BINARY' % item_barcode)
                            elif line.startswith('  TRACK') or line.startswith('    INDEX'):
                                outfile.write(line)
                
                #remove original cue and txt file        
                os.remove(orig_cue)
                os.remove(txt_file)
                
                #create toc file
                cue2toc_ver = subprocess.check_output('cue2toc -v', text=True).split('\n')[0]
                timestamp = str(datetime.datetime.now())
                cmd = 'cue2toc -o %s %s' % (cdr_toc, cue)
                exitcode = subprocess.call(cmd, shell=True, text=True)
                
                #record premis
                premis_list.append(premis_dict(timestamp, 'metadata modification', exitcode, cmd, "Converted the CD's .CUE file to the table of contents (.TOC) format.", cue2toc_ver))
                pickleDump('premis_list', premis_list, folders)
               
            if rip_option == 'DVD_Data':
                
                #set variables
                orig_imagefile = os.path.join(image_dir, '%s.iso' % item_barcode)
                imagefile = '%s.dd' % os.path.splitext(orig_imagefile)[0]
                
                #make sure image file is present
                if not os.path.exists(orig_imagefile):
                    if os.path.exists(imagefile):
                        print('\n.ISO file already changed to .DD; converting back to complete operations.')
                        os.rename(imagefile, orig_imagefile)
                        
                    elif os.path.exists(os.path.join(image_dir, '%s.mdf' % item_barcode)):
                        print('\nWARNING: item is Compact Disc Digital Audio; unable to transfer using RipStation DataGrabber.')
                        write_list(failed_ingest, '%s\tDisc is CDDA; transfer using original RipStation' % item_barcode)
                        continue
                        
                    else:
                        print('\nWARNING: disk image does not exist!  Moving on to next item...')
                        write_list(failed_ingest, '%s\tDisk image does not exist' % item_barcode)
                        continue
                
                #write premis information for disk image creation.  Even if image is unreadable, we assume that this operation was successful
                timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(orig_imagefile)).isoformat()
                premis_list.append(premis_dict(timestamp, 'disk image creation', 0, 'RipStation BR6-7604 ISO image batch operation', 'Extracted a disk image from the physical information carrier.', 'RipStation DataGrabber V1.0.35.0'))
                pickleDump('premis_list', premis_list, folders)
            
                #save ripstation log information for disc to log_dir.  Make sure it's only written once...
                if not os.path.exists(ripstation_log):
                    with open(ripstation_log, 'a') as outf:
                        outf.write('RipStation DataGrabber V1.0.35.0\n')
                        with open(rs_log, 'r') as inf:
                            for line in inf.read().splitlines():
                                if item_barcode in line:
                                    outf.write('%s %s\n' % (rs_timestamp, line))
                
                #mount .ISO so we can verify disk image type
                exitcode = mount_iso(orig_imagefile)
                if exitcode != 0:
                    print('\nWARNING: failed to mount disk image!  Moving on to next item...')
                    write_list(failed_ingest, '%s\tFailed to mount disk image' % item_barcode)
                    continue
                
                #set mediaStatus variable: confirms that 'media' (mounted disk image) is present; required by bdpl_ingest functions
                mediaStatus = True
                
                #get drive letter for newly mounted disk image
                drive_letter = get_iso_drive_letter(orig_imagefile)
                
                #run lsdvd to determine if jobType is DVD-Video or Disk_Image
                print('\nCHECKING IF DISC IS DATA OR DVD-VIDEO...')
                titlecount = lsdvd_check(folders, item_barcode, drive_letter)
                
                #set jobType based on titlecount
                if titlecount == 0:
                    jobType = 'Disk_image'
                    
                    with open(jobType_file, 'wb') as f:
                        pickle.dump(jobType, f)
                    
                    #dismount disk image
                    exitcode = dismount_iso(orig_imagefile)
                    if exitcode != 0:
                        print('\nWARNING: failed to dismount disk image!  Moving on to next item...')
                        write_list(failed_ingest, '%s\tFailed to dismount disk image' % item_barcode)
                        continue
                    
                    #rename to '.dd' file extension
                    timestamp = str(datetime.datetime.now())
                    os.rename(orig_imagefile, imagefile)
                    
                    #document change to filename
                    premis_list = pickleLoad('premis_list', folders, item_barcode)
                    premis_list.append(premis_dict(timestamp, 'filename change', 0, 'os.rename(%s, %s)' % (orig_imagefile, imagefile), 'Modified the filename, changing extension from .ISO to .DD to ensure consistency with IUL BDPL practices', 'Python %s' % sys.version.split()[0]))
                    pickleDump('premis_list', premis_list, folders)
                    
                    #get info on the disk image (fsstat, ils, mmls, and disktype)
                    disk_image_info(folders, item_barcode)
                    
                    #create a logical copy of content on disk image. This is a little messy, but it seems a little better than making another copy of the disk image...
                    fs_list = pickleLoad('fs_list', folders, item_barcode)
                    secureCopy_list = ['udf', 'iso9660']
                    if any(fs in ' '.join(fs_list) for fs in secureCopy_list):
                        print('\nADDITIONAL STEPS FOR ISO9660/UDF FILE SYSTEM...')
                        os.rename(imagefile, orig_imagefile)
                        mount_iso(orig_imagefile)
                        drive_letter = get_iso_drive_letter(orig_imagefile)
                        secureCopy(drive_letter, folders, item_barcode)
                        dismount_iso(orig_imagefile)
                        os.rename(orig_imagefile, imagefile)
                    else:
                        disk_image_replication(folders, item_barcode)
                    
                else:
                    jobType = 'DVD'
                    
                    with open(jobType_file, 'wb') as f:
                        pickle.dump(jobType, f)
                    
                    #create .MPG videos for all titles on disk
                    normalize_dvd_content(folders, item_barcode, titlecount, drive_letter)
                    
                    #dismount disk image
                    print('\nDISMOUNTING DISK IMAGE FILE...') 
                    exitcode = dismount_iso(orig_imagefile)
                    if exitcode != 0:
                        print('\nWARNING: failed to dismount disk image!  Moving on to next item...')
                        write_list(failed_ingest, '%s\tFailed to dismount disk image' % item_barcode)
                        continue
                    
                    #rename to '.dd' file extension
                    os.rename(orig_imagefile, imagefile)
                
                if checkFiles(files_dir):
                    with open(replicated, 'a') as f:
                        f.write('%s\n' % item_barcode)
                else:
                    print('\nWARNING: failed to replicate files!  Moving on to next item...')
                    write_list(failed_ingest, '%s\tFailed to replicate files' % item_barcode)
                    continue
            
        if not check_list(analyzed, item_barcode):
            #now set variables for analysis procedures
            analysis_vars = {'platform' : 'bdpl_ripstation', 'jobType' : jobType, 're_analyze' : False, 'gui_vars' : {}}
            
            #send content through analysis
            analyzeContent(unit_name, shipmentDate, item_barcode, analysis_vars)
            
            #TO DO: need to verify if procedures actually completed...
            with open(analyzed, 'a') as f:
                f.write('%s\n' % item_barcode)
                
            print('\n%s completed!' % item_barcode)
            print('\n\n---------------------------------------------------------------------------')
            
if __name__ == '__main__':
    main()
    