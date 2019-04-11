#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

This project was inspired by and includes elements of Brunnhilde, a Siegfried-based digital archives reporting tool
github.com/timothyryanwalsh/brunnhilde
Copyright (c) 2017 Tim Walsh, distributed under The MIT License (MIT)
http://bitarchivist.net

"""
from collections import OrderedDict
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
from lxml import etree as ET
import tempfile
import fnmatch
from Tkinter import *
import tkFileDialog
import glob
import cPickle
import time
import openpyxl
import glob

# from dfxml project
import Objects

def check_premis(term, key_term):
    #check to see if an event is already in our premis list--i.e., it's been successfully completed.
 
    #set up premis_list
    premis_list = cPickle_load('premis_list')
    
    #check to see if an event is already in our premis list--i.e., it's been successfully completed.
    s = set((i['%s' % key_term] for i in premis_list))
    
    if term in s:
        return True
    else:
        return False

def first_run():
    #this function only runs when a record is loaded for the first time.
    
    newscreen()
    
    #check if key data has been entered
    if not verify_data():
        return
        
    #now make sure that barcode is valid and pull 
    if not verify_barcode():
        return
    
    #now create folders
    createFolders()

def bdpl_vars():
    #this function creates folder variables
    vars = {}
    vars['unit_home'] = os.path.join(home_dir, '%s' % unit.get())
    vars['destination'] = os.path.join(vars['unit_home'], "%s" % barcode.get())
    vars['image_dir'] = os.path.join(vars['destination'], "disk-image")
    vars['files_dir'] = os.path.join(vars['destination'], "files")
    vars['metadata'] = os.path.join(vars['destination'], "metadata")
    vars['temp_dir'] = os.path.join(vars['destination'], 'temp')
    vars['reports_dir'] = os.path.join(vars['metadata'], 'reports')
    vars['log_dir'] = os.path.join(vars['metadata'], 'logs')
    vars['imagefile'] = os.path.join(vars['image_dir'], '%s.dd' % barcode.get())
    vars['dfxml_output'] = os.path.join(vars['metadata'], '%s-dfxml.xml' % barcode.get())
    vars['bulkext_dir'] = os.path.join(vars['temp_dir'], 'bulk_extractor')
    vars['bulkext_log'] = os.path.join(vars['log_dir'], 'bulkext-log.txt')
    vars['media_pics'] = os.path.join(vars['metadata'], 'media-image')
    vars['media_image_dir'] = os.path.join(home_dir, 'media-images', '%s' % unit.get())
    
    return vars

def createFolders():       
    #create folders
    try:
        os.makedirs(bdpl_vars()['destination'])
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    try:
        os.makedirs(bdpl_vars()['image_dir'])
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
            
    try:
        os.makedirs(bdpl_vars()['files_dir'])
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
    
    try:
        os.makedirs(bdpl_vars()['metadata'])
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
    
    try:
        os.makedirs(bdpl_vars()['temp_dir'])
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    try:
        os.makedirs(bdpl_vars()['reports_dir'])
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
            
    try:
        os.makedirs(bdpl_vars()['log_dir'])
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
    
    try:
        os.makedirs(bdpl_vars()['media_image_dir'])
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
    
def cPickle_load(list_name):
    temp_file = os.path.join(bdpl_vars()['temp_dir'], '%s.txt' % list_name)
    if list_name == "premis_list":
        temp_list = []
    else:
        temp_list = {}
    if os.path.exists(temp_file):
        with open(temp_file, 'rb') as file:
            temp_list = cPickle.load(file)
    return temp_list

def cPickle_dump(list_name, list_contents):
    temp_dir = bdpl_vars()['temp_dir']
    temp_file = os.path.join(temp_dir, '%s.txt' % list_name)
     
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
        
    with open(temp_file, 'wb') as file:
            cPickle.dump(list_contents, file)

def secureCopy(file_source, file_destination):
    if not os.path.exists(file_source):
        print '\n\nThis file source does not appear to exist: "%s"\n\nPlease verify the correct source has been identified.' % file_source
        return
    
    #function takes the file source and destination as well as  a specific premis event to be used in documenting action
    print '\n\nFILE REPLICATION: TERACOPY\n\tSOURCE: %s \n\tDESTINATION: %s' % (file_source, file_destination)
    
    #set variables for premis
    timestamp = str(datetime.datetime.now())             
    migrate_ver = "TeraCopy v3.26"
    
    #set variables for copy operation 
    
    copycmd = 'TERACOPY COPY "%s" %s /SkipAll /CLOSE' % (file_source, file_destination)
    
    try:
        exitcode = subprocess.call(copycmd, shell=True)
    except subprocess.CalledProcessError as e:
        print '\n\nFILE REPLICATION FAILED:\n\n%s' % e
        return
    
    #check to see if files are actually present (TeraCopy may complete without copying...)
    if not checkFiles(file_destination):
        return
    
    #need to find Teracopy SQLITE db and export list of copied files to csv log file
    list_of_files = glob.glob(os.path.join(os.path.expandvars('C:\Users\%USERNAME%\AppData\Roaming\TeraCopy\History'), '*'))
    tera_db = max(list_of_files, key=os.path.getctime)
    log_cmd = 'sqlite3 -header -csv %s "SELECT * from Files;"' % tera_db
    tera_log = os.path.join(bdpl_vars()['log_dir'], 'teracopy_log.csv')
    with open(tera_log, 'wb') as output:
        migrate_exit = subprocess.call(log_cmd, stdout=output, shell=True)
    
    #capture premis
    premis_list = cPickle_load('premis_list')
    premis_list.append(premis_dict(timestamp, 'replication', exitcode, copycmd, migrate_ver))
    cPickle_dump('premis_list', premis_list)
    
    print '\n\nFILE REPLICATION COMPLETED; PROCEED TO NEXT STEP.'

def ddrescue_image(temp_dir, log_dir, imagefile, image_dir):
    
    print '\n\nDISK IMAGE CREATION: DDRESCUE\n\tSOURCE: %s \n\tDESTINATION: %s\n\n' % (sourceDevice.get(), imagefile)
    
    #set up premis list
    premis_list = cPickle_load('premis_list')
    
    #create variables for mapfile and ddrescue commands (first and second passes)
    mapfile = os.path.join(temp_dir, '%s.map' % barcode.get())
           
    ddrescue_events1 = os.path.join(log_dir, 'ddrescue_events1.txt')
    ddrescue_events2 = os.path.join(log_dir, 'ddrescue_events2.txt')
    
    ddrescue_rates1 = os.path.join(log_dir, 'ddrescue_rates1.txt')
    ddrescue_rates2 = os.path.join(log_dir, 'ddrescue_rates2.txt')
    
    ddrescue_reads1 = os.path.join(log_dir, 'ddrescue_reads1.txt')
    ddrescue_reads2 = os.path.join(log_dir, 'ddrescue_reads2.txt')
    
    migrate_ver = subprocess.check_output('ddrescue -V', shell=True).split('\n', 1)[0]  
    timestamp1 = str(datetime.datetime.now())
    
    copycmd1 = 'ddrescue -n --log-events=%s --log-rates=%s --log-reads=%s %s %s %s' % (ddrescue_events1, ddrescue_rates1, ddrescue_reads1, sourceDevice.get(), imagefile, mapfile)
    
    #run commands via subprocess; per ddrescue instructions, we need to run it twice    

    exitcode1 = subprocess.call(copycmd1, shell=True)
    
    premis_list.append(premis_dict(timestamp1, 'disk image creation', exitcode1, copycmd1, migrate_ver))
    
    #new timestamp for second pass (recommended by ddrescue developers)
    timestamp2 = str(datetime.datetime.now())
    
    copycmd2 = 'ddrescue -d -r2 --log-events=%s --log-rates=%s --log-reads=%s %s %s %s' % (ddrescue_events2, ddrescue_rates2, ddrescue_reads2, sourceDevice.get(), imagefile, mapfile)
    
    exitcode2 = subprocess.call(copycmd2, shell=True)
    
    if checkFiles(image_dir):
        if os.stat(imagefile).st_size > 0L:
            print '\n\nDISK IMAGE CREATED.'
            exitcode2 = 0
            premis_list.append(premis_dict(timestamp2, 'disk image creation', exitcode2, copycmd2, migrate_ver))
        else:
            print '\n\nDISK IMAGE CREATION FAILED\n\n\tIndicate any issues in note to collecting unit.'
    else:
        print '\n\nDISK IMAGE CREATION FAILED\n\n\tIndicate any issues in note to collecting unit.'
    
    #save premis
    cPickle_dump('premis_list', premis_list)

def mediaCheck():
    if mediaStatus.get() == False:
        print '\n\nMake sure that media has been inserted/attached; check the "Media present?" box and continue.'
        return False
    else:
        return True

def TransferContent():    
    files_dir = bdpl_vars()['files_dir']
    log_dir = bdpl_vars()['log_dir']
    imagefile = bdpl_vars()['imagefile']
    temp_dir = bdpl_vars()['temp_dir']
    reports_dir = bdpl_vars()['reports_dir']
    files_dir = bdpl_vars()['files_dir']
    image_dir = bdpl_vars()['image_dir']
    
    #check that barcode exists on spreadsheet; exit if not wrong
    if not verify_data():
        return
    
    newscreen()
    
    print '\n\nSTEP 1. TRANSFER CONTENT'
        
    #check to see if content will include disk image; if nothing entered, exit and prompt user to do so        
    if jobType.get() == 'Copy_only':
        
        teracopy_source = source.get().replace('/', '\\')
        
        secureCopy(teracopy_source, files_dir)
                
    elif jobType.get() == 'Disk_image':     
        #make sure that media is present
        if not mediaCheck():
            return
            
        #special process for 5.25" floppies: use FC5025
        if sourceDevice.get() == '5.25':
            if disk525.get() == 'N/A':
                print '\n\nError; be sure to select the appropriate 5.25" floppy disk type from the drop down menu.'
                return
            
            print '\n\n\DISK IMAGE CREATION: DeviceSideData FC5025\n\tSOURCE: %s \n\tDESTINATION: %s\n\n' % (sourceDevice.get(), imagefile)
            
            #create premis list
            premis_list = cPickle_load('premis_list')
            
            disk_type_options = { 'Apple DOS 3.3 (16-sector)' : 'apple33', 'Apple DOS 3.2 (13-sector)' : 'apple32', 'Apple ProDOS' : 'applepro', 'Commodore 1541' : 'c1541', 'TI-99/4A 90k' : 'ti99', 'TI-99/4A 180k' : 'ti99ds180', 'TI-99/4A 360k' : 'ti99ds360', 'Atari 810' : 'atari810', 'MS-DOS 1200k' : 'msdos12', 'MS-DOS 360k' : 'msdos360', 'North Star MDS-A-D 175k' : 'mdsad', 'North Star MDS-A-D 350k' : 'mdsad350', 'Kaypro 2 CP/M 2.2' : 'kaypro2', 'Kaypro 4 CP/M 2.2' : 'kaypro4', 'CalComp Vistagraphics 4500' : 'vg4500', 'PMC MicroMate' : 'pmc', 'Tandy Color Computer Disk BASIC' : 'coco', 'Motorola VersaDOS' : 'versa' }
  
            timestamp = str(datetime.datetime.now())
            copycmd = 'fcimage -f %s %s | tee -a %s' % (disk_type_options[disk525.get()], imagefile, os.path.join(log_dir, 'fcimage.log'))

            exitcode = subprocess.call(copycmd, shell=True)
            
            if exitcode == 0:
                premis_list.append(premis_dict(timestamp, 'disk image creation', exitcode, copycmd, 'FCIMAGE v1309'))
                
            
            else:
                #FC5025 reports non-0 exitcode if there are any read errors; therefore, if a disk image larger than 0 bytes exists, we will call it a success
                if os.stat(imagefile).st_size > 0L:
                    premis_list.append(premis_dict(timestamp, 'disk image creation', 0, copycmd, 'FCIMAGE v1309'))
                else:
                    print '\n\nDisk image not successfully created; verify you have selected the correct disk type and try again (if possible).  Otherwise, indicate issues in note to collecting unit.'
                    return
            print '\n\nDISK IMAGE CREATION COMPLETED.'
            
            #save premis
            cPickle_dump('premis_list', premis_list)
        
        else:
            
            ddrescue_image(temp_dir, log_dir, imagefile, image_dir)
        
        #now extract/copy files; first, run disktype to determine file systems on image; then check
        fs_list = disktype_info(imagefile, reports_dir)
        
        if any('HFS' in item for item in fs_list):
            carve_ver = subprocess.check_output('unhfs', shell=True).split('\n', 1)[0]
            carve_cmd = 'unhfs -v -resforks APPLEDOUBLE -o "%s" "%s"' % (files_dir, imagefile)
            
            carvefiles(carve_ver, carve_cmd, 'UNHFS', imagefile, files_dir)
                
        elif any('ISO9660' in item for item in fs_list):
            secureCopy(optical_drive_letter(), files_dir)
                
        elif any('UDF' in item for item in fs_list):
            secureCopy(optical_drive_letter(), files_dir)
                
        else:
            carve_ver = 'tsk_recover: %s ' % subprocess.check_output('tsk_recover -V').strip()
            carve_cmd = 'tsk_recover -a "%s" "%s"' % (imagefile, files_dir)
            
            carvefiles(carve_ver, carve_cmd, 'TSK_RECOVER', imagefile, files_dir)
            
    elif jobType.get() == 'DVD':
        #make sure media is present
        if not mediaCheck():
            return
        
        #create disk image of DVD
        ddrescue_image(temp_dir, log_dir, imagefile, image_dir)
        
        #set up PREMIS list
        premis_list = cPickle_load('premis_list')
        
        #rip copies of each title with ffmpeg        
        ffmpeg_source = "%s\\" % optical_drive_letter()
        
        #document information about DVD titles; set variables and get lsdvd version
        lsdvd_temp = os.path.join(temp_dir, 'lsdvd.txt')
        cmd = 'lsdvd -V > %s 2>&1' % lsdvd_temp
        
        subprocess.check_output(cmd, shell=True)
        
        with open(lsdvd_temp, 'rb') as f:
            lsdvd_ver = f.read().split(' - ')[0]
        
        #now run lsdvd to get info about DVD, including # of titles
        lsdvdout = os.path.join(reports_dir, "%s_lsdvd.xml" % barcode.get())
        timestamp = str(datetime.datetime.now())
        lsdvdcmd = 'lsdvd -Ox -x %s > %s 2> NUL' % (ffmpeg_source, lsdvdout)
        exitcode = subprocess.call(lsdvdcmd, shell=True)
        
        premis_list.append(premis_dict(timestamp, 'metadata extraction', exitcode, lsdvdcmd, lsdvd_ver))
        
        #check file to see how many titles are on DVD using lsdvd XML output
        parser = ET.XMLParser(recover=True)
        doc = ET.parse(lsdvdout, parser=parser)
        titlecount = int(doc.xpath("count(//lsdvd//track)"))
        
        #check current directory; change to a temp directory to store files
        bdpl_cwd = os.getcwd()
        
        ffmpeg_temp = os.path.join(temp_dir, 'ffmpeg')
        if not os.path.exists(ffmpeg_temp):
            os.makedirs(ffmpeg_temp)
        
        os.chdir(ffmpeg_temp)
        
        #get ffmpeg version
        ffmpeg_ver =  '; '.join(subprocess.check_output('ffmpeg -version', shell=True).splitlines()[0:2])
        
        print '\n\nMOVING IMAGE FILE NORMALIZATION: FFMPEG'
        
        #loop through titles and rip each one to mpeg using native streams
        for title in range(1, (titlecount+1)):
            titlelist = glob.glob(os.path.join(ffmpeg_source, "VIDEO_TS", "VTS_%s_*.VOB" % str(title).zfill(2)))
            if len(titlelist) > 0:
                timestamp = str(datetime.datetime.now())
                
                ffmpegout = os.path.join(files_dir, '%s-%s.mpg' % (barcode.get(), str(title).zfill(2)))
                ffmpeg_cmd = 'ffmpeg -nostdin -loglevel warning -report -stats -i "concat:%s" -c copy -target ntsc-dvd %s' % ('|'.join(titlelist), ffmpegout)
                
                print '\n\n\tGenerating title %s of %s: %s\n\n' % (str(title), str(titlecount), ffmpegout)
                
                exitcode = subprocess.call(ffmpeg_cmd, shell=True)
                    
                premis_list.append(premis_dict(timestamp, 'normalization: access version', exitcode, ffmpeg_cmd, ffmpeg_ver))
                
                #move and rename ffmpeg log file
                ffmpeglog = glob.glob(os.path.join(ffmpeg_temp, 'ffmpeg-*.log'))[0]
                shutil.move(ffmpeglog, os.path.join(log_dir, '%s-%s-ffmpeg.log' % (barcode.get(), str(title).zfill(2))))
        
        #save PREMIS to file       
        cPickle_dump('premis_list', premis_list)
        
        #move back to original directory
        os.chdir(bdpl_cwd)
        
        print '\n\nMOVING IMAGE NORMALIZATION COMPLETED; PROCEED TO NEXT STEP.'
    
    elif jobType.get() == 'CDDA':
        #make sure media is present
        if not mediaCheck():
            return
        
        #set up PREMIS list
        premis_list = cPickle_load('premis_list')

        print '\n\nDISK IMAGE CREATION: CDRDAO\n\tSOURCE: %s \n\tDESTINATION: %s' % (sourceDevice.get(), image_dir)
        
        #determine appropriate drive ID for cdrdao; save output of command to temp file
        cdr_scan = os.path.join(temp_dir, 'cdr_scan.txt')
        scan_cmd = 'cdrdao scanbus > %s 2>&1' % cdr_scan
        subprocess.check_output(scan_cmd, shell=True)

        #pull drive ID and cdrdao version from file
        with open(cdr_scan, 'rb') as f:
            info = f.read().splitlines()
        cdrdao_ver = info[0].split(' - ')[0]
        drive_id = info[8].split(':')[0]
            
        #get info about CD using cdrdao; record this as a premis event, too.
        disk_info_log = os.path.join(reports_dir, '%s-cdrdao-diskinfo.txt' % barcode.get())
        cdrdao_cmd = 'cdrdao disk-info --device %s --driver generic-mmc-raw > %s 2>&1' % (drive_id, disk_info_log)
        timestamp = str(datetime.datetime.now())
        exitcode = subprocess.call(cdrdao_cmd, shell=True)
        
        premis_list.append(premis_dict(timestamp, 'metadata extraction', exitcode, cdrdao_cmd, cdrdao_ver))

        #read log file to determine # of sessions on disk.
        with open(disk_info_log, 'rb') as f:
            for line in f:
                if 'Sessions             :' in line:
                    sessions = int(line.split(':')[1].strip())
        
        t2c_ver = subprocess.check_output('toc2cue -V', shell=True).strip()
        
        #for each session, create a bin/toc file
        for x in range(1, (sessions+1)):
            cdr_bin = os.path.join(image_dir, "%s-%s.bin") % (barcode.get(), str(x).zfill(2))
            cdr_toc = os.path.join(image_dir, "%s-%s.toc") % (barcode.get(), str(x).zfill(2))
            
            print '\n\n\tGenerating session %s of %s: %s\n\n' % (str(x), str(sessions), cdr_bin)
            
            #create separate bin/cue for each session
            cdr_cmd = 'cdrdao read-cd --read-raw --session %s --datafile %s --device %s --driver generic-mmc-raw -v 1 %s' % (str(x), cdr_bin, drive_id, cdr_toc)
            
            timestamp = str(datetime.datetime.now())
            
            exitcode = subprocess.call(cdr_cmd, shell=True)
            
            premis_list.append(premis_dict(timestamp, 'disk image creation', exitcode, cdr_cmd, cdrdao_ver))
                        
            #convert TOC to CUE
            cue = os.path.join(image_dir, "%s-%s.cue") % (barcode.get(), str(sessions).zfill(2))
            t2c_cmd = 'toc2cue %s %s' % (cdr_toc, cue)
            timestamp = str(datetime.datetime.now())
            exitcode2 = subprocess.call(t2c_cmd, shell=True)
            
            premis_list.append(premis_dict(timestamp, 'metadata modification', exitcode2, t2c_cmd, t2c_ver))
            
            #place a copy of the .cue file for the first session in files_dir for the forthcoming WAV; this session will have audio data
            if x == 1:
                shutil.copy(cue, os.path.join(files_dir, '%s.cue' % barcode.get()))
        
        print '\n\nDISK IMAGE CREATED'
        
        #now rip CDDA to WAV using cd-paranoia (Cygwin build; note hyphen)
         
        #get cdparanoia version
        paranoia_temp = os.path.join(temp_dir, 'paranoia.txt')
        ver_cmd = 'cd-paranoia -V > %s 2>&1' % paranoia_temp
        
        exitcode = subprocess.call(ver_cmd, shell=True)
        with open(paranoia_temp, 'rb') as f:
            paranoia_ver = f.read().splitlines()[0]
        
        paranoia_log = os.path.join(log_dir, '%s-cdparanoia.log' % barcode.get())
        paranoia_out = os.path.join(files_dir, '%s.wav' % barcode.get())
        
        print '\n\nAUDIO CONTENT NORMALIZATION: CDPARANOIA\n\tSOURCE: %s \n\tDESTINATION: %s\n' % (sourceDevice.get(), paranoia_out)
        
        paranoia_cmd = 'cd-paranoia -l %s -w [00:00:00.00]- %s' % (paranoia_log, paranoia_out)
        
        timestamp = str(datetime.datetime.now())
        exitcode = subprocess.call(paranoia_cmd, shell=True)
        
        premis_list.append(premis_dict(timestamp, 'disk image creation', exitcode, paranoia_cmd, paranoia_ver))
        
        #save PREMIS to file
        cPickle_dump('premis_list', premis_list)
        
        print 'AUDIO NORMALIZATION COMPLETE; PROCEED TO NEXT STEP'
        
    else: 
        print '\n\nError; please indicate the appropriate job type'
        return
    
def premis_dict(timestamp, event_type, event_outcome, event_detail, agent_id):
    temp_dict = {}
    temp_dict['eventType'] = event_type
    temp_dict['eventOutcomeDetail'] = event_outcome
    temp_dict['timestamp'] = timestamp
    temp_dict['eventDetailInfo'] = event_detail
    temp_dict['linkingAgentIDvalue'] = agent_id
    return temp_dict
    
def check_fs(fs_type, disktype_output):
    #function to check for specific disk image filetype using disktype output
    
    with open(disktype_output, 'rb') as f:
        for line in f:
            if fs_type in line and 'file system' in line:
                return True
            else:
                continue
        return False

def carvefiles(carve_ver, carve_cmd, tool, location1, location2):
    if tool == 'UNHFS':
        print '\n\nFILE REPLICATION: %s\n\tSOURCE: %s \n\tDESTINATION: %s' % (tool, location2, location1)
    else:
        print '\n\nFILE REPLICATION: %s\n\tSOURCE: %s \n\tDESTINATION: %s' % (tool, location1, location2)
    #carve files from disk images (excluding ISO9660 and UDF)
    timestamp = str(datetime.datetime.now())  
    pipes = subprocess.Popen(carve_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    std_out, std_err = pipes.communicate()
        
    if pipes.returncode == 0 or pipes.returncode == '0':
        print '\n\nFILE REPLICATION COMPLETED; PROCEED TO NEXT STEP.'
    else:
        print '\n\nFILE REPLICATION FAILED:\n\t%s' % std_err
        return
    
    #save preservation event to PREMIS
    premis_list = cPickle_load('premis_list')
    premis_list.append(premis_dict(timestamp, 'replication', pipes.returncode, carve_cmd, carve_ver))
    cPickle_dump('premis_list', premis_list)
    
def time_to_int(str_time):
    """ Convert datetime to unix integer value """
    dt = time.mktime(datetime.datetime.strptime(str_time, 
        "%Y-%m-%dT%H:%M:%S").timetuple())
    return dt
    
def fix_dates(files_dir, dfxml_output):
    #adapted from Timothy Walsh's Disk Image Processor: https://github.com/CCA-Public/diskimageprocessor
    timestamp = str(datetime.datetime.now())
    
    print '\n\nFILE MAC TIME CORRECTION (USING DFXML)'
    
    try:
        for (event, obj) in Objects.iterparse(dfxml_output):
            # only work on FileObjects
            if not isinstance(obj, Objects.FileObject):
                continue

            # skip directories and links
            if obj.name_type:
                if obj.name_type not in ["r", "d"]:
                    continue

            # record filename
            dfxml_filename = obj.filename
            dfxml_filedate = int(time.time()) # default to current time

            # record last modified or last created date
            try:
                mtime = obj.mtime
                mtime = str(mtime)
            except:
                pass

            try:
                crtime = obj.crtime
                crtime = str(crtime)
            except:
                pass

            # fallback to created date if last modified doesn't exist
            if mtime and (mtime != 'None'):
                mtime = time_to_int(mtime[:19])
                dfxml_filedate = mtime
            elif crtime and (crtime != 'None'):
                crtime = time_to_int(crtime[:19])
                dfxml_filedate = crtime
            else:
                continue

            # rewrite last modified date of corresponding file in objects/files
            exported_filepath = os.path.join(files_dir, dfxml_filename)
            if os.path.isdir(exported_filepath):
                os.utime(exported_filepath, (dfxml_filedate, dfxml_filedate))
            elif os.path.isfile(exported_filepath):
                os.utime(exported_filepath, (dfxml_filedate, dfxml_filedate)) 
            else:
                continue

    except ValueError:
       pass
    
    premis_list = cPickle_load('premis_list')
        
    premis_list.append(premis_dict(timestamp, 'metadata modification (timestamp correction)', '0', 'https://github.com/CCA-Public/diskimageprocessor/blob/master/diskimageprocessor.py#L446-L489', 'Adapted from Disk Image Processor Version: 1.0.0 (Tim Walsh)'))

    cPickle_dump('premis_list', premis_list)

def run_antivirus(files_dir, log_dir, metadata):
    
    print '\n\nVIRUS SCAN: MpCmdRun.exe\n\n'
    
    #return if virus scan already run
    if check_premis('virus check', 'eventType'):
        print '\n\nVirus scan already completed.'
        return
    
    virus_log = os.path.join(log_dir, 'viruscheck-log.txt')
    
    #use location of MpCmdRun.log on workstation to set variables to copy log file
    windir = tempfile.gettempdir()
    win_log = os.path.join(windir, "MpCmdRun.log")
    if os.path.exists(win_log):
        os.remove(win_log)
    av_command = '"C:\\Program Files\\Windows Defender\\MpCmdRun.exe" -Scan -ScanType 3 -File %s | tee "%s"' % (files_dir, virus_log)
    
    
    timestamp = str(datetime.datetime.now())
    exitcode = subprocess.call(av_command, shell=True)
    
    #concatenate log file with antivirus stdout
    subprocess.call('TYPE %s >> %s' % (win_log, virus_log), shell=True)
    
    #Get Antivirus signature, definition, etc.; find file with information, convert to ASCII and get most recent 'version' info
    defender_path = "C:\\ProgramData\\Microsoft\\Windows Defender\\Support"
    find_file = fnmatch.filter(os.listdir(defender_path), 'MPDetection-*.log')
    defender_unicode = os.path.join(defender_path, find_file[0])
    defender_ascii = os.path.join(metadata, find_file[0])
    exitcode = subprocess.call('TYPE "%s" > %s' % (defender_unicode, defender_ascii), shell=True)
    
    for line in reversed(open(defender_ascii).readlines()):
        if "Version: " in line:
            av_ver = "Windows Defender MpCmdRun.exe. " + line
            av_ver = av_ver.rstrip()
            break
    
    os.remove(defender_ascii)
    
    #write info to appraisal_dict
    appraisal_dict = cPickle_load('appraisal_dict')
        
    if "found no threats" not in open(virus_log).read():
        appraisal_dict['Virus'] = 'WARNING! Virus or malware found; see %s.' % virus_log
        
    else:
        appraisal_dict['Virus'] = 'No virus or malware identified.'

        
    cPickle_dump('appraisal_dict', appraisal_dict)
    
    #save preservation to PREMIS
    premis_list = cPickle_load('premis_list')
    premis_list.append(premis_dict(timestamp, 'virus check', exitcode, av_command, av_ver))
    cPickle_dump('premis_list', premis_list)
   

def run_bulkext(bulkext_dir, bulkext_log, files_dir, html, reports_dir):
    print '\n\nSENSITIVE DATA SCAN: BULK_EXTRACTOR'
    
    #return if b_e was run before
    if check_premis('PII scan', 'eventType'):
        print '\n\nSensitive data scan already completed.'
        return
    
    try:
        os.makedirs(bulkext_dir)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    #use default command with buklk_extractor; individuak could implement changes to use 'find' scanner at a later date
    bulkext_command = 'bulk_extractor -x aes -x base64 -x elf -x exif -x gps -x hiberfile -x httplogs -x json -x kml -x net -x pdf -x sqlite -x vcard -x winlnk -x winpe -x winprefetch -S ssn_mode=2 -o "%s" -R "%s" > "%s"' % (bulkext_dir, files_dir, bulkext_log)

    #create timestamp
    timestamp = str(datetime.datetime.now())        

    exitcode = subprocess.call(bulkext_command, shell=True)
    
    #get bulk extractor version for premis
    try:
        be_ver = subprocess.check_output(['bulk_extractor', '-V'], shell=True)
    except subprocess.CalledProcessError as e:
        be_ver = e.output
       
    premis_list = cPickle_load('premis_list')       
    premis_list.append(premis_dict(timestamp, 'PII scan', exitcode, bulkext_command, be_ver.rstrip()))
    cPickle_dump('premis_list', premis_list)
    
    #create a cumulative BE report
    cumulative_report = os.path.join(bulkext_dir, 'cumulative.txt')
    for myfile in ('pii.txt', 'ccn.txt', 'email.txt', 'telephone.txt', 'find.txt'):
        myfile = os.path.join(bulkext_dir, myfile)
        if os.path.exists(myfile) and os.stat(myfile).st_size > 0L:
            with open(myfile, 'rb') as filein:
                data = filein.read().splitlines(True)    
            with open(cumulative_report, 'ab') as outfile:
                outfile.write('%s: %s\n' % (os.path.basename(myfile), len(data[5:])))
    if not os.path.exists(cumulative_report):         
        open(cumulative_report, 'ab').close()
        
    write_html('Personally Identifiable Information (PII)', '%s' % cumulative_report, '\n', html)

    #move any b_e histogram files, if needed
    for myfile in ('email_domain_histogram.txt', 'find_histogram.txt', 'telephone_histogram.txt'):
        current_file = os.path.join(bulkext_dir, myfile)
        try:    
            if os.stat(current_file).st_size > 0L:
                shutil.copy(current_file, reports_dir)
        except OSError:
            continue

def run_siegfried(files_dir, reports_dir, siegfried_version):

    print '\n\nFILE FORMAT IDENTIFICATION: SIEGFRIED'
    """Run siegfried on directory"""  
    sf_file = os.path.join(reports_dir, 'siegfried.csv')
    sf_command = 'sf -z -csv -hash md5 "%s" > "%s"' % (files_dir, sf_file)
    
    #create timestamp
    timestamp = str(datetime.datetime.now())
    
    if os.path.exists(sf_file):
        os.remove(sf_file)                                                                 
    
    exitcode = subprocess.call(sf_command, shell=True)
    
    premis_list = cPickle_load('premis_list')
    
    premis_list.append(premis_dict(timestamp, 'format identification', exitcode, sf_command, siegfried_version))
    
    cPickle_dump('premis_list', premis_list)

def import_csv(cursor, conn, reports_dir):
    """Import csv file into sqlite db"""
    sf_file = os.path.join(reports_dir, 'siegfried.csv')
    
    if (sys.version_info > (3, 0)):
        f = open(sf_file, 'r', encoding='utf8')
    else:
        f = open(sf_file, 'rb')
    try:
        reader = csv.reader(x.replace('\0', '') for x in f) # replace null bytes with empty strings on read
    except UnicodeDecodeError:
        f = (x.encode('utf-8').strip() for x in f) # skip non-utf8 encodable characters
        reader = csv.reader(x.replace('\0', '') for x in f) # replace null bytes with empty strings on read
    header = True
    for row in reader:
        if header:
            header = False # gather column names from first row of csv
            sql = "DROP TABLE IF EXISTS siegfried"
            cursor.execute(sql)
            sql = "CREATE TABLE siegfried (filename text, filesize text, modified text, errors text, hash text, namespace text, id text, format text, version text, mime text, basis text, warning text)"
            cursor.execute(sql)
            insertsql = "INSERT INTO siegfried VALUES (%s)" % (", ".join([ "?" for column in row ]))
            rowlen = len(row)
        else:
            # skip lines that don't have right number of columns
            if len(row) == rowlen:
                cursor.execute(insertsql, row)
    conn.commit()
    f.close()

def generate_reports(cursor, html, reports_dir):
    """Run sql queries on db to generate reports, write to csv and html"""
    full_header = ['Filename', 'Filesize', 'Date modified', 'Errors', 'Checksum', 
                'Namespace', 'ID', 'Format', 'Format version', 'MIME type', 
                'Basis for ID', 'Warning']
    
    # sorted format list report
    sql = "SELECT format, id, COUNT(*) as 'num' FROM siegfried GROUP BY format ORDER BY num DESC"
    path = os.path.join(reports_dir, 'formats.csv')
    format_header = ['Format', 'ID', 'Count']
    sqlite_to_csv(sql, path, format_header, cursor)
    write_html('File formats', path, ',', html)

    # sorted format and version list report
    sql = "SELECT format, id, version, COUNT(*) as 'num' FROM siegfried GROUP BY format, version ORDER BY num DESC"
    path = os.path.join(reports_dir, 'formatVersions.csv')
    version_header = ['Format', 'ID', 'Version', 'Count']
    sqlite_to_csv(sql, path, version_header, cursor)
    write_html('File format versions', path, ',', html)

    # sorted mimetype list report
    sql = "SELECT mime, COUNT(*) as 'num' FROM siegfried GROUP BY mime ORDER BY num DESC"
    path = os.path.join(reports_dir, 'mimetypes.csv')
    mime_header = ['MIME type', 'Count']
    sqlite_to_csv(sql, path, mime_header, cursor)
    write_html('MIME types', path, ',', html)

    # dates report
    sql = "SELECT SUBSTR(modified, 1, 4) as 'year', COUNT(*) as 'num' FROM siegfried GROUP BY year ORDER BY num DESC"
    path = os.path.join(reports_dir, 'years.csv')
    year_header = ['Year Last Modified', 'Count']
    sqlite_to_csv(sql, path, year_header, cursor)
    write_html('Last modified dates by year', path, ',', html)

    # unidentified files report
    sql = "SELECT * FROM siegfried WHERE id='UNKNOWN';"
    path = os.path.join(reports_dir, 'unidentified.csv')
    sqlite_to_csv(sql, path, full_header, cursor)
    write_html('Unidentified', path, ',', html)

    # errors report
    sql = "SELECT * FROM siegfried WHERE errors <> '';"
    path = os.path.join(reports_dir, 'errors.csv')
    sqlite_to_csv(sql, path, full_header, cursor)
    write_html('Errors', path, ',', html)

    # duplicates report
    sql = "SELECT * FROM siegfried t1 WHERE EXISTS (SELECT 1 from siegfried t2 WHERE t2.hash = t1.hash AND t1.filename != t2.filename) AND filesize<>'0' ORDER BY hash;"
    path = os.path.join(reports_dir, 'duplicates.csv')
    sqlite_to_csv(sql, path, full_header, cursor)
    write_html('Duplicates', path, ',', html)

def sqlite_to_csv(sql, path, header, cursor):
    """Write sql query result to csv"""
    # in python3, specify newline to prevent extra csv lines in windows
    # in python2, write csv in byte mode
    if (sys.version_info > (3, 0)):
        report = open(path, 'w', newline='', encoding='utf8')
    else:
        report = open(path, 'wb')
    w = csv.writer(report, lineterminator='\n')
    w.writerow(header)
    for row in cursor.execute(sql):
        w.writerow(row)
    report.close()

def write_pronom_links(old_file, new_file):
    """Use regex to replace fmt/# and x-fmt/# PUIDs with link to appropriate PRONOM page"""
    
    if (sys.version_info > (3, 0)):
        in_file = open(old_file, 'r', encoding='utf8')
        out_file = open(new_file, 'w', encoding='utf8')
    else:
        in_file = open(old_file, 'rb')
        out_file = open(new_file, 'wb')

    for line in in_file:
        regex = r"fmt\/[0-9]+|x\-fmt\/[0-9]+" #regex to match fmt/# or x-fmt/#
        pronom_links_to_replace = re.findall(regex, line)
        new_line = line
        for match in pronom_links_to_replace:
            new_line = line.replace(match, "<a href=\"http://nationalarchives.gov.uk/PRONOM/" + 
                    match + "\" target=\"_blank\">" + match + "</a>")
            line = new_line # allow for more than one match per line
        out_file.write(new_line)

    in_file.close()
    out_file.close()

def write_html(header, path, file_delimiter, html):
    temp_dir = os.path.join(home_dir, unit.get(), barcode.get(), 'temp')
    
    """Write csv file to html table"""
    in_file = open(path, 'rb')
    # count lines and then return to start of file
    numline = len(in_file.readlines())
    in_file.seek(0)

    #open csv reader
    r = csv.reader(in_file, delimiter="%s" % file_delimiter)

    # write header
    html.write('\n<a name="%s" style="padding-top: 40px;"></a>' % header)
    html.write('\n<h4>%s</h4>' % header)
    if header == 'Duplicates':
        html.write('\n<p><em>Duplicates are grouped by hash value.</em></p>')
    elif header == 'Personally Identifiable Information (PII)':
        html.write('\n<p><em>Potential PII in source, as identified by bulk_extractor.</em></p>')
    
    # if writing PII, handle separately
    pii_list = []
    if header == 'Personally Identifiable Information (PII)':
        
        appraisal_dict = cPickle_load('appraisal_dict')
        
        #check that there are any PII results
        if os.stat(path).st_size > 0L:
            html.write('\n<table class="table table-sm table-responsive table-hover">')
            html.write('\n<thead>')
            html.write('\n<tr>')
            html.write('\n<th>PII type</th>')
            html.write('\n<th># of matches (may be false)</th>')
            html.write('\n<th>More information (if available)</th>')
            html.write('\n</tr>')
            html.write('\n</thead>')
            html.write('\n<tbody>')
            with open(path, 'rb') as pii_info:
                for line in pii_info:
                    html.write('\n<tr>')
                    if 'pii.txt' in line:
                        # write data
                        html.write('\n<td>SSNs, Account Nos., Birth Dates, etc.</td>')
                        html.write('\n<td>' + line.split()[1] + '</td>')
                        html.write('\n<td>Data not stored locally.</td>')
                        pii_list.append('PII (SSNs, account Nos., and/or birth dates)')
                    if 'ccn.txt' in line:
                        html.write('\n<td>Credit Card Nos.</td>')
                        html.write('\n<td>' + line.split()[1] + '</td>')
                        html.write('\n<td>Data not stored locally.</td>')
                        pii_list.append('credit cards nos.')
                    if 'email.txt' in line:
                        html.write('\n<td>Email address domains (may include 3rd party information)</td>')
                        html.write('\n<td>' + line.split()[1] + '</td>')
                        html.write('\n<td>See: <a href="./email_domain_histogram.txt">Email domain histogram</a></td>')
                        pii_list.append('email addresses')
                    if 'telephone.txt' in line:
                        html.write('\n<td>Telephone numbers (may include 3rd party information)</td>')
                        html.write('\n<td>' + line.split()[1] + '</td>')
                        html.write('\n<td>See: <a href="./telephone_histogram.txt">Telephone # histogram</a></td>')
                        pii_list.append('telephone numbers')
                    if 'find.txt' in line:
                        html.write('\n<td>Sensitive terms and phrases</td>')
                        html.write('\n<td>' + line.split()[1] + '</td>')
                        html.write('\n<td>See: <a href="./find_histogram.txt">Keyword histogram</a></td>')
                        pii_list.append('pre-defined words or phrases')
                    html.write('\n</tr>')   
            html.write('\n</tbody>')
            html.write('\n</table>')
                
            appraisal_dict['PII'] = 'Potential sensitive information identified: %s.' % ', '.join(pii_list)
    
        else:
            html.write('\nNone found.')
            appraisal_dict['PII'] = 'No PII identified'
        
        cPickle_dump('appraisal_dict', appraisal_dict)

    # if writing duplicates, handle separately
    elif header == 'Duplicates':
        if numline > 1: #aka more rows than just header
            # read md5s from csv and write to list
            hash_list = []
            for row in r:
                if row:
                    hash_list.append(row[4])
            # deduplicate md5_list
            hash_list = list(OrderedDict.fromkeys(hash_list))
            hash_list.remove('Checksum')
            # for each hash in md5_list, print header, file info, and list of matching files
            for hash_value in hash_list:
                html.write('\n<p>Files matching checksum <strong>%s</strong>:</p>' % hash_value)
                html.write('\n<table class="table table-sm table-responsive table-bordered table-hover">')
                html.write('\n<thead>')
                html.write('\n<tr>')
                html.write('\n<th>Filename</th><th>Filesize</th>')
                html.write('<th>Date modified</th><th>Errors</th>')
                html.write('<th>Checksum</th><th>Namespace</th>')
                html.write('<th>ID</th><th>Format</th>')
                html.write('<th>Format version</th><th>MIME type</th>')
                html.write('<th>Basis for ID</th><th>Warning</th>')
                html.write('\n</tr>')
                html.write('\n</thead>')
                in_file.seek(0) # back to beginning of file
                html.write('\n<tbody>')
                for row in r:
                    if row[4] == '%s' % hash_value:
                        # write data
                        html.write('\n<tr>')
                        for column in row:
                            html.write('\n<td>' + column + '</td>')
                        html.write('\n</tr>')
                html.write('\n</tbody>')
                html.write('\n</table>')
        else:
            html.write('\nNone found.\n<br><br>')

    # otherwise write as normal
    else:
        if numline > 1: #aka more rows than just header
            # add borders to table for full-width tables only
            full_width_table_headers = ['Unidentified', 'Errors']
            if header in full_width_table_headers:
                html.write('\n<table class="table table-sm table-responsive table-bordered table-hover">')
            else:
                html.write('\n<table class="table table-sm table-responsive table-hover">')
            # write header row
            html.write('\n<thead>')
            html.write('\n<tr>')
            row1 = next(r)
            for column in row1:
                html.write('\n<th>' + column + '</th>')
            html.write('\n</tr>')
            html.write('\n</thead>')
            # write data rows
            html.write('\n<tbody>')
            for row in r:
                # write data
                html.write('\n<tr>')
                for column in row:
                    html.write('\n<td>' + column + '</td>')
                html.write('\n</tr>')
            html.write('\n</tbody>')
            html.write('\n</table>')
        else:
            html.write('\nNone found.\n<br><br>')
    
    in_file.close()
    
def convert_size(size):
    # convert size to human-readable form
    if (size == 0):
        return '0 bytes'
    size_name = ("bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size,1024)))
    p = math.pow(1024,i)
    s = round(size/p)
    s = str(s)
    s = s.replace('.0', '')
    return '%s %s' % (s,size_name[i])
    
def close_html(html):
    """Add JavaScript and write html closing tags"""
    html.write('\n</div>')
    html.write('\n</div>')
    html.write('\n</div>')
    html.write('\n</div>')
    html.write('\n<script src="./assets//js/jquery-3.3.1.slim.min.js"></script>')
    html.write('\n<script src="./assets//js/popper.min.js"></script>')
    html.write('\n<script src="./assets//js/bootstrap.min.js"></script>')
    html.write('\n<script>$(".navbar-nav .nav-link").on("click", function(){ $(".navbar-nav").find(".active").removeClass("active"); $(this).addClass("active"); });</script>')
    html.write('\n<script>$(".navbar-brand").on("click", function(){ $(".navbar-nav").find(".active").removeClass("active"); });</script>')
    html.write('\n</body>')
    html.write('\n</html>')

def close_files_conns_on_exit(html, conn, cursor):
    cursor.close()
    conn.close()
    html.close()
    
 

def get_stats(files_dir, scan_started, cursor, html, siegfried_version, reports_dir, log_dir):
    """Get aggregate statistics and write to html report"""
    
    # get stats from sqlite db
    cursor.execute("SELECT COUNT(*) from siegfried;") # total files
    num_files = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) from siegfried where filesize='0';") # empty files
    empty_files = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT hash) from siegfried WHERE filesize<>'0';") # distinct files
    distinct_files = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(hash) FROM siegfried t1 WHERE EXISTS (SELECT 1 from siegfried t2 WHERE t2.hash = t1.hash AND t1.filename != t2.filename) AND filesize<>'0'") # duplicates
    all_dupes = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT hash) FROM siegfried t1 WHERE EXISTS (SELECT 1 from siegfried t2 WHERE t2.hash = t1.hash AND t1.filename != t2.filename) AND filesize<>'0'") # distinct duplicates
    distinct_dupes = cursor.fetchone()[0]

    duplicate_copies = int(all_dupes) - int(distinct_dupes) # number of duplicate copies of unique files
    duplicate_copies = str(duplicate_copies)

    cursor.execute("SELECT COUNT(*) FROM siegfried WHERE id='UNKNOWN';") # unidentified files
    unidentified_files = cursor.fetchone()[0]

    year_sql = "SELECT DISTINCT SUBSTR(modified, 1, 4) as 'year' FROM siegfried;" # min and max year
    year_path = os.path.join(reports_dir, 'uniqueyears.csv')
    # if python3, specify newline to prevent extra csv line in windows
    # else, open and read csv in bytes mode
    # see: https://stackoverflow.com/questions/3348460/csv-file-written-with-python-has-blank-lines-between-each-row
    if (sys.version_info > (3, 0)):
        year_report = open(year_path, 'w', newline='')
    else:
        year_report = open(year_path, 'wb')
    w = csv.writer(year_report, lineterminator='\n')
    for row in cursor.execute(year_sql):
        w.writerow(row)
    year_report.close()

    if (sys.version_info > (3, 0)):
        year_report_read = open(year_path, 'r', newline='')
    else:
        year_report_read = open(year_path, 'rb')
    r = csv.reader(year_report_read)
    years = []
    for row in r:
        if row == '':
            continue
        elif row:
            years.append(row[0])
    if not years:
        begin_date = "N/A"
        end_date = "N/A"  
    else:
        try:
            begin_date = min(years, key=float)
        except ValueError:
            badfloat = years.index(min(years))
            del years[badfloat]
            begin_date = min(years, key=float)
            
        end_date = max(years, key=float)
        
    year_report_read.close()

    # delete temporary uniqueyear file from csv reports dir
    #os.remove(year_path)

    datemodified_sql = "SELECT DISTINCT modified FROM siegfried;" # min and max full modified date
    datemodified_path = os.path.join(reports_dir, 'datemodified.csv')
    # specify newline in python3 to prevent extra csv lines in windows
    # read and write csv in byte mode in python2
    if (sys.version_info > (3, 0)):
        date_report = open(datemodified_path, 'w', newline='')
    else:
        date_report = open(datemodified_path, 'wb')
    w = csv.writer(date_report, lineterminator='\n')
    for row in cursor.execute(datemodified_sql):
        w.writerow(row)
    date_report.close()

    if (sys.version_info > (3, 0)):
        date_report_read = open(datemodified_path, 'r', newline='')
    else:
        date_report_read = open(datemodified_path, 'rb')
    r = csv.reader(date_report_read)
    dates = []
    for row in r:
        if row:
            dates.append(row[0])
    if not dates:
        earliest_date = "N/A"
        latest_date = "N/A"
    else:
        earliest_date = min(dates)
        latest_date = max(dates)
    date_report_read.close()

    os.remove(datemodified_path) # delete temporary datemodified file from csv reports dir

    cursor.execute("SELECT COUNT(DISTINCT format) as formats from siegfried WHERE format <> '';") # number of identfied file formats
    num_formats = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM siegfried WHERE errors <> '';") # number of siegfried errors
    num_errors = cursor.fetchone()[0]

    # calculate size from recursive dirwalk and format
    size_bytes = 0
    if (sys.version_info > (3, 0)):
        for root, dirs, files in os.walk(files_dir):
            for f in files:
                file_path = os.path.join(root, f)
                file_info = os.stat(file_path)
                size_bytes += file_info.st_size
    else:
        for root, dirs, files in os.walk(unicode(files_dir, 'utf-8')):
            for f in files:
                file_path = os.path.join(root, f)
                try:
                    file_info = os.stat(file_path)
                    size_bytes += file_info.st_size
                except OSError as e: # report when Brunnhilde can't find file
                    pass
    size = convert_size(size_bytes)
    
    # write html
    html.write('<!DOCTYPE html>')
    html.write('\n<html lang="en">')
    html.write('\n<head>')
    html.write('\n<title>IUL Born Digital Preservation Lab report: %s</title>' % barcode.get())
    html.write('\n<meta http-equiv="Content-Type" content="text/html; charset=utf-8">')
    html.write('\n<meta name="description" content="HTML report based upon a template developed by Tim Walsh and distributed as part of Brunnhilde v. 1.7.2">')
    html.write('\n<link rel="stylesheet" href="./assets//css/bootstrap.min.css">')
    html.write('\n</head>')
    html.write('\n<body style="padding-top: 80px">')
    # navbar
    html.write('\n<nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">')
    html.write('\n<a class="navbar-brand" href="#">Brunnhilde</a>')
    html.write('\n<button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNavAltMarkup" aria-controls="navbarNavAltMarkup" aria-expanded="false" aria-label="Toggle navigation">')
    html.write('\n<span class="navbar-toggler-icon"></span>')
    html.write('\n</button>')
    html.write('\n<div class="collapse navbar-collapse" id="navbarNavAltMarkup">')
    html.write('\n<div class="navbar-nav">')
    html.write('\n<a class="nav-item nav-link" href="#Provenance">Provenance</a>')
    html.write('\n<a class="nav-item nav-link" href="#Stats">Statistics</a>')
    html.write('\n<a class="nav-item nav-link" href="#File formats">File formats</a>')
    html.write('\n<a class="nav-item nav-link" href="#File format versions">Versions</a>')
    html.write('\n<a class="nav-item nav-link" href="#MIME types">MIME types</a>')
    html.write('\n<a class="nav-item nav-link" href="#Last modified dates by year">Dates</a>')
    html.write('\n<a class="nav-item nav-link" href="#Unidentified">Unidentified</a>')
    html.write('\n<a class="nav-item nav-link" href="#Errors">Errors</a>')
    html.write('\n<a class="nav-item nav-link" href="#Duplicates">Duplicates</a>')
    html.write('\n<a class="nav-item nav-link" href="#Personally Identifiable Information (PII)">PII</a>')
    html.write('\n</div>')
    html.write('\n</div>')
    html.write('\n</nav>')
    # content
    html.write('\n<div class="container-fluid">')
    html.write('\n<h1 style="text-align: center; margin-bottom: 40px;">Brunnhilde HTML report</h1>')
    # provenance
    html.write('\n<a name="Provenance" style="padding-top: 40px;"></a>')
    html.write('\n<div class="container-fluid" style="margin-bottom: 40px;">')
    html.write('\n<div class="card">')
    html.write('\n<h2 class="card-header">Provenance</h2>')
    html.write('\n<div class="card-body">')
    '''need to check if disk image or not'''
    if jobType.get() == '2':
        html.write('\n<p><strong>Input source: Physical media</strong></p>')
    if jobType.get() == '1':
        html.write('\n<p><strong>Input source: File directory</strong></p>')
    html.write('\n<p><strong>Accession/identifier:</strong> %s</p>' % barcode.get())
    html.write('\n<p><strong>Siegfried version:</strong> %s</p>' % siegfried_version)
    html.write('\n<p><strong>Scan started:</strong> %s</p>' % scan_started)
    html.write('\n</div>')
    html.write('\n</div>')
    html.write('\n</div>')
    # statistics
    html.write('\n<a name="Stats" style="padding-top: 40px;"></a>')
    html.write('\n<div class="container-fluid" style="margin-bottom: 40px;">')
    html.write('\n<div class="card">')
    html.write('\n<h2 class="card-header">Statistics</h2>')
    html.write('\n<div class="card-body">')
    html.write('\n<h4>Overview</h4>')
    html.write('\n<p><strong>Total files:</strong> %s</p>' % num_files)
    html.write('\n<p><strong>Total size:</strong> %s</p>' % size)
    html.write('\n<p><strong>Years (last modified):</strong> %s - %s</p>' % (begin_date, end_date))
    html.write('\n<p><strong>Earliest date:</strong> %s</p>' % earliest_date)
    html.write('\n<p><strong>Latest date:</strong> %s</p>' % latest_date)
    html.write('\n<h4>File counts and contents</h4>')
    html.write('\n<p><em>Calculated by hash value. Empty files are not counted in first three categories. Total files = distinct + duplicate + empty files.</em></p>')
    html.write('\n<p><strong>Distinct files:</strong> %s</p>' % distinct_files)
    html.write('\n<p><strong>Distinct files with duplicates:</strong> %s</p>' % distinct_dupes)
    html.write('\n<p><strong>Duplicate files:</strong> %s</p>' % duplicate_copies)
    html.write('\n<p><strong>Empty files:</strong> %s</p>' % empty_files)
    html.write('\n<h4>Format identification</h4>')
    html.write('\n<p><strong>Identified file formats:</strong> %s</p>' % num_formats)
    html.write('\n<p><strong>Unidentified files:</strong> %s</p>' % unidentified_files)
    html.write('\n<h4>Errors</h4>')
    html.write('\n<p><strong>Siegfried errors:</strong> %s</p>' % num_errors)
    html.write('\n<h2>Virus scan report</h2>')
    with open(os.path.join(log_dir, 'viruscheck-log.txt')) as f:
        virus_report = f.read()
    html.write('\n<p>%s</p>' % virus_report)
    html.write('\n</div>')
    html.write('\n</div>')
    html.write('\n</div>')
    # detailed reports
    html.write('\n<div class="container-fluid" style="margin-bottom: 40px;">')
    html.write('\n<div class="card">')
    html.write('\n<h2 class="card-header">Detailed reports</h2>')
    html.write('\n<div class="card-body">')
    
    #save information to appraisal_dict
    appraisal_dict = cPickle_load('appraisal_dict')
            
    date_range = '%s to %s' % (begin_date, end_date)
    appraisal_dict.update({'Source': barcode.get(), 'Dates': date_range, 'Extent': size, 'Files': num_files, 'Duplicates': distinct_dupes, 'FormatCount': num_formats, 'Unidentified':unidentified_files})  
    
    cPickle_dump('appraisal_dict', appraisal_dict)
    
def print_premis(premis_path):   
    
    premis_list = cPickle_load('premis_list')
    
    attr_qname = ET.QName("http://www.w3.org/2001/XMLSchema-instance", "schemaLocation")

    PREMIS_NAMESPACE = "http://www.loc.gov/premis/v3"

    PREMIS = "{%s}" % PREMIS_NAMESPACE

    NSMAP = {'premis' : PREMIS_NAMESPACE,
            "xsi": "http://www.w3.org/2001/XMLSchema-instance"}

    root = ET.Element(PREMIS + 'premis', {attr_qname: "http://www.loc.gov/premis/v3 https://www.loc.gov/standards/premis/premis.xsd"}, version="3.0", nsmap=NSMAP)
    
    object = ET.SubElement(root, PREMIS + 'object', attrib={ET.QName(NSMAP['xsi'], 'type'): 'premis:file'})
    objectIdentifier = ET.SubElement(object, PREMIS + 'objectIdentifier')
    objectIdentifierType = ET.SubElement(objectIdentifier, PREMIS + 'objectIdentifierType')
    objectIdentifierType.text = 'local'
    objectIdentifierValue = ET.SubElement(objectIdentifier, PREMIS + 'objectIdentifierValue')
    objectIdentifierValue.text = barcode.get()
    objectCharacteristics = ET.SubElement(object, PREMIS + 'objectCharacteristics')
    compositionLevel = ET.SubElement(objectCharacteristics, PREMIS + 'compositionLevel')
    compositionLevel.text = '0'
    format = ET.SubElement(objectCharacteristics, PREMIS + 'format')
    formatDesignation = ET.SubElement(format, PREMIS + 'formatDesignation')
    formatName = ET.SubElement(formatDesignation, PREMIS + 'formatName')
    formatName.text = 'Tape Archive Format'
    formatRegistry = ET.SubElement(format, PREMIS + 'formatRegistry')
    formatRegistryName = ET.SubElement(formatRegistry, PREMIS + 'formatRegistryName')
    formatRegistryName.text = 'PRONOM'
    formatRegistryKey = ET.SubElement(formatRegistry, PREMIS + 'formatRegistryKey')
    formatRegistryKey.text = 'x-fmt/265' 
    
    for entry in premis_list:
        event = ET.SubElement(root, PREMIS + 'event')
        eventID = ET.SubElement(event, PREMIS + 'eventIdentifier')
        eventIDtype = ET.SubElement(eventID, PREMIS + 'eventIdentifierType')
        eventIDtype.text = 'UUID'
        eventIDval = ET.SubElement(eventID, PREMIS + 'eventIdentifierValue')
        eventIDval.text = str(uuid.uuid4())

        eventType = ET.SubElement(event, PREMIS + 'eventType')
        eventType.text = entry['eventType']

        eventDateTime = ET.SubElement(event, PREMIS + 'eventDateTime')
        eventDateTime.text = entry['timestamp']

        eventDetailInfo = ET.SubElement(event, PREMIS + 'eventDetailInformation')
        eventDetail = ET.SubElement(eventDetailInfo, PREMIS + 'eventDetail')
        eventDetail.text = entry['eventDetailInfo']

        eventOutcomeInfo = ET.SubElement(event, PREMIS + 'eventOutcomeInformation')
        eventOutcome = ET.SubElement(eventOutcomeInfo, PREMIS + 'eventOutcome')
        eventOutcome.text = str(entry['eventOutcomeDetail'])
        eventOutDetail = ET.SubElement(eventOutcomeInfo, PREMIS + 'eventOutcomeDetail')
        eventOutDetailNote = ET.SubElement(eventOutDetail, PREMIS + 'eventOutcomeDetailNote')
        if entry['eventOutcomeDetail'] == '0':
            eventOutDetailNote.text = 'Successful completion'
        elif entry['eventOutcomeDetail'] == 0:
            eventOutDetailNote.text = 'Successful completion'
        else:
            eventOutDetailNote.text = 'Unsuccessful completion; refer to logs.'

        linkingAgentID = ET.SubElement(event, PREMIS + 'linkingAgentIdentifier')
        linkingAgentIDtype = ET.SubElement(linkingAgentID, PREMIS + 'linkingAgentIdentifierType')
        linkingAgentIDtype.text = 'local'
        linkingAgentIDvalue = ET.SubElement(linkingAgentID, PREMIS + 'linkingAgentIdentifierValue')
        linkingAgentIDvalue.text = 'IUL BDPL'
        linkingAgentRole = ET.SubElement(linkingAgentID, PREMIS + 'linkingAgentRole')
        linkingAgentRole.text = 'implementer'
        linkingAgentID = ET.SubElement(event, PREMIS + 'linkingAgentIdentifier')
        linkingAgentIDtype = ET.SubElement(linkingAgentID, PREMIS + 'linkingAgentIdentifierType')
        linkingAgentIDtype.text = 'local'
        linkingAgentIDvalue = ET.SubElement(linkingAgentID, PREMIS + 'linkingAgentIdentifierValue')
        linkingAgentIDvalue.text = entry['linkingAgentIDvalue']
        linkingAgentRole = ET.SubElement(linkingAgentID, PREMIS + 'linkingAgentRole')
        linkingAgentRole.text = 'executing software'
        linkingObjectID = ET.SubElement(event, PREMIS + 'linkingObjectIdentifier')
        linkingObjectIDtype = ET.SubElement(linkingObjectID, PREMIS + 'linkingObjectIdentifierType')
        linkingObjectIDtype.text = 'local'
        linkingObjectIDvalue = ET.SubElement(linkingObjectID, PREMIS + 'linkingObjectIdentifierValue')
        linkingObjectIDvalue.text = barcode.get()
    
    premis_tree = ET.ElementTree(root)
    
    premis_tree.write(premis_path, pretty_print=True, xml_declaration=True, encoding="utf-8")

def checkFiles(some_dir):
    #check to see if it exists
    if not os.path.exists(some_dir):
        print '\n\nError; folder "%s" does not exist.' % some_dir
        return False
    
    #make sure there are files in the 'files' directory
    filecounter = 0
    for dirpath, dirnames, contents in os.walk(some_dir):
        filecounter += len(contents)
    if filecounter == 0:
        print '\n\nError; no files located at %s. Check settings and run again; you may need to manually copy or extract files.' % some_dir
        return False
    else:
        return True

def produce_dfxml(target):
    dfxml_output = bdpl_vars()['dfxml_output']
    
    #check if the output file exists AND if the action was recorded in PREMIS; if so, return
    if os.path.exists(dfxml_output) and check_premis('message digest calculation', 'eventType'):
        return
    
    timestamp = str(datetime.datetime.now())
    
    #use fiwalk if we have an image file
    if os.path.isfile(target):
        print '\n\nDIGITAL FORENSICS XML CREATION: FIWALK'
        dfxml_ver_cmd = 'fiwalk-0.6.3 -V'
        dfxml_ver = subprocess.check_output(dfxml_ver_cmd, shell=True).splitlines()[0]
        
        dfxml_cmd = 'fiwalk-0.6.3 -x %s > %s' % (target, dfxml_output)
        exitcode = subprocess.call(dfxml_cmd, shell=True)
    
    #use md5deep if we have a folder. NOTE: this will fail on paths that exceed MAX_PATH    
    elif os.path.isdir(target):
        print '\n\nDIGITAL FORENSICS XML CREATION: MD5DEEP'
        dfxml_ver = subprocess.check_output('md5deep64 -v', shell=True)
        dfxml_ver = 'md5deep64.exe %s' % dfxml_ver
    
        dfxml_cmd = 'md5deep64 -rd %s > %s' % (target, dfxml_output)        
        exitcode = subprocess.call(dfxml_cmd, shell=True)
    
    else:
        print '\n\nERROR: %s does not appear to exist...' % target
        return
    
    #save PREMIS
    premis_list = cPickle_load('premis_list')        
    premis_list.append(premis_dict(timestamp, 'message digest calculation', exitcode, dfxml_cmd, dfxml_ver))
    cPickle_dump('premis_list', premis_list)

def optical_drive_letter():
    drive_cmd = 'wmic logicaldisk get caption, drivetype | FINDSTR /C:"5"'
    drive_ltr = subprocess.check_output(drive_cmd, shell=True).split()[0]
    return drive_ltr

def disk_image_info(imagefile, reports_dir):
 
    print '\n\nDISK IMAGE METADATA EXTRACTION: FSSTAT and ILS'
    premis_list = cPickle_load('premis_list') 
    
    #check to see if fsstat was run; if not, do it
    fsstat_output = os.path.join(reports_dir, 'fsstat.txt')
    fsstat_ver = 'fsstat: %s' % subprocess.check_output('fsstat -V', shell=True).strip()
    fsstat_command = 'fsstat %s > %s' % (imagefile, fsstat_output)
    
    timestamp = str(datetime.datetime.now())
    exitcode = subprocess.call(fsstat_command, shell=True)
    
    premis_list.append(premis_dict(timestamp, 'forensic feature analysis', exitcode, fsstat_command, fsstat_ver))

    #check to see if ils has been run; if not, do it! 
    ils_output = os.path.join(reports_dir, 'ils.txt')
    ils_ver = subprocess.check_output('ils -V', shell=True).strip()
    ils_command = 'ils -e %s > %s' % (imagefile, ils_output)
    
    timestamp = str(datetime.datetime.now())
    exitcode = subprocess.call(ils_command, shell=True) 
    
    premis_list.append(premis_dict(timestamp, 'forensic feature analysis', exitcode, ils_command, ils_ver))
    
    cPickle_dump('premis_list', premis_list)

def disktype_info(imagefile, reports_dir):

    disktype_output = os.path.join(reports_dir, 'disktype.txt')
    if not os.path.exists(disktype_output):
        disktype_command = 'disktype %s > %s' % (imagefile, disktype_output)
        
        timestamp = str(datetime.datetime.now())
        exitcode = subprocess.call(disktype_command, shell=True)
        
        #Save preservation event to PREMIS
        premis_list = cPickle_load('premis_list')
        premis_list.append(premis_dict(timestamp, 'forensic feature analysis', exitcode, disktype_command, 'disktype v9'))
        cPickle_dump('premis_list', premis_list)
    
    #now get list of file systems on disk
    fs_list = []
    with open(disktype_output, 'rb') as f:
        for line in f:
            if 'file system' in line:
                    fs_list.append(line.lstrip().split(' file system', 1)[0])
    return fs_list

def dir_tree(target):
    
    print '\n\nDOCUMENTING FOLDER/FILE STRUCTURE: TREE'
    
    reports_dir = bdpl_vars()['reports_dir']
    
    #make a directory tree to document original structure
    tree_dest = os.path.join(reports_dir, 'tree.txt')
    tree_ver = subprocess.check_output('tree --version', shell=True).split(' (')[0]
    tree_command = 'tree.exe -tDhR "%s" > "%s"' % (target, tree_dest)
    
    timestamp = str(datetime.datetime.now())
    exitcode = subprocess.call(tree_command, shell=True)

    premis_list = cPickle_load('premis_list')
    premis_list.append(premis_dict(timestamp, 'metadata extraction', exitcode, tree_command, tree_ver))
    cPickle_dump('premis_list', premis_list)

def format_analysis(files_dir, reports_dir, log_dir, metadata, html):
    #return if Siegfried already run
    if check_premis('format identification', 'eventType'):
        return
    
    siegfried_db = os.path.join(metadata, 'siegfried.sqlite')
    conn = sqlite3.connect(siegfried_db)
    conn.text_factory = str  # allows utf-8 data to be stored
    cursor = conn.cursor() 
    
    scan_started = str(datetime.datetime.now()) # get time 
    sfcmd = 'sf -version'
    siegfried_version = subprocess.check_output(sfcmd, shell=True).replace('\n', ' ')
    
    run_siegfried(files_dir, reports_dir, siegfried_version) # run siegfried

    import_csv(cursor, conn, reports_dir) # load csv into sqlite db
    get_stats(files_dir, scan_started, cursor, html, siegfried_version, reports_dir, log_dir) # get aggregate stats and write to html file
    generate_reports(cursor, html, reports_dir) # run sql queries, print to html and csv
    close_html(html) # close HTML file tags
    
    # close database connections
    cursor.close()
    conn.close()

def analyzeContent():
    
    files_dir = bdpl_vars()['files_dir']
    log_dir = bdpl_vars()['log_dir']
    metadata = bdpl_vars()['metadata']
    reports_dir = bdpl_vars()['reports_dir']
    imagefile = bdpl_vars()['imagefile']
    files_dir = bdpl_vars()['files_dir']
    bulkext_dir = bdpl_vars()['bulkext_dir']
    bulkext_log = bdpl_vars()['bulkext_log']
    temp_dir = bdpl_vars()['temp_dir']
    image_dir = bdpl_vars()['image_dir']
    dfxml_output = bdpl_vars()['dfxml_output']

    print '\n\n-------------------------------------------------------------\n\nSTEP 2: CONTENT ANALYSIS' 
    
    #if information not 'verified' then go into 'first run'; exit if anything is wrong
    if not verify_data():
        return
    
    #return if no job type is selected
    if jobType.get() not in ['Disk_image', 'Copy_only', 'DVD', 'CDDA']:
        print '\n\nError; please indicate the appropriate job type'
        return
        
    # copy .css and .jc files to assets directory
    assets_dir = os.path.join(bdpl_resources, 'assets')
    assets_target = os.path.join(reports_dir, 'assets')
    if os.path.exists(assets_target):
        pass
    else:
        shutil.copytree(assets_dir, assets_target)
                                                                                                                                   
    #set up html for report
    temp_html = os.path.join(temp_dir, 'temp.html')
    if (sys.version_info > (3, 0)):
        html = open(temp_html, 'w', encoding='utf8')
    else:
        html = open(temp_html, 'wb')  
    
    #run antivirus scan using Windows MpCmdRun.exe
    run_antivirus(files_dir, log_dir, metadata)
    
    #special steps if working on disk image...
    if jobType.get() == 'Disk_image':
                
        #get additional info about the disk image
        disk_image_info(imagefile, reports_dir)
        
        #generate DFXML with checksums
        produce_dfxml(imagefile)
        
        #fix dates from files replicated by tsk_recover; first, gete a list of filesystems identified by Disktype
        fs_list = disktype_info(imagefile, reports_dir)
        
        #now see if our list of file systems include either HFS, UDF, or ISO9660 images; these files were replicated using tools other than tsk_recover and don't require date fixes.
        check_list = ['UDF', 'ISO9660', 'HFS']
        if not any(fs in ' '.join(fs_list) for fs in check_list):
            fix_dates(files_dir, dfxml_output)
    
        #document directory structure
        dir_tree(files_dir)
        
        #run bulk_extractor and prepare b_e report for write_html
        run_bulkext(bulkext_dir, bulkext_log, files_dir, html, reports_dir)
        
    elif jobType.get() == 'Copy_only':
        
        #generate dfxml for preservation copy
        produce_dfxml(files_dir)
        
        #document directory structure
        dir_tree(source.get())
        
        #run bulk_extractor and prepare b_e report for write_html
        run_bulkext(bulkext_dir, bulkext_log, files_dir, html, reports_dir)
    
    elif jobType.get() == 'DVD':
        #generate dfxml for preservation copy
        produce_dfxml(imagefile)
        
        #document directory structure
        dir_tree(files_dir)
    
    elif jobType.get() == 'CDDA':
        #generate dfxml for preservation copy
        produce_dfxml(image_dir)
        
        #document directory structure
        dir_tree(files_dir)
    
    else: 
        print '\n\nError; please indicate the appropriate job type'
        return   
    
    #run siegfried to characterize file formats  
    format_analysis(files_dir, reports_dir, log_dir, metadata, html)
    
    # close HTML file
    html.close()

    # write new html file, with hrefs for PRONOM IDs   
    new_html = os.path.join(reports_dir, 'report.html')
    if not os.path.exists(new_html):
        write_pronom_links(temp_html, new_html)

    # get format list and add to appraisal dictionary
    appraisal_dict = cPickle_load('appraisal_dict')
    
    fileformats = []
    formatcount = 0
    formatlist = ''
    formatcsv = os.path.join(reports_dir, 'formats.csv')
    try:
        with open(formatcsv, 'rb') as csvfile:
            formatreader = csv.reader(csvfile)
            next(formatreader)
            for row in formatreader:
                formatcount += 1
                fileformats.append(row[0])
            fileformats = [element or 'Unidentified' for element in fileformats] # replace empty elements with 'Unidentified'
            if formatcount > 10:
                appraisal_dict['Formats'] = "The most prevalent file formats (out of a total %s) are:\n%s" % (formatcount, '\n'.join(fileformats[:10]))
            elif formatcount <= 10:
                appraisal_dict['Formats'] = "The most prevalent file formats (out of a total %s) are:\n%s" % (formatcount, '\n'.join(fileformats))
            else:
                appraisal_dict['Formats'] = "ERROR! Check format CSV file: %s" % formatcsv
            
    except IOError:
        appraisal_dict['Formats'] = "ERROR! No formats.csv file to pull formats from."
            
    cPickle_dump('appraisal_dict', appraisal_dict)
    
    premis_name = str(barcode.get()) + '-premis.xml'
    premis_path = os.path.join(metadata, premis_name)
    print_premis(premis_path)
    
    #write info to spreadsheet for collecting unit to review
    writeSpreadsheet()
       
    print '\n\n----------------------------------------------------------\n\nContent Analysis is complete; results for item %s:\n' % barcode.get()
    
    du_cmd = 'du64.exe -nobanner "%s" > %s' % (files_dir, os.path.join(temp_dir, 'final_stats.txt'))
    
    subprocess.call(du_cmd, shell=True)   
    
    du_list = ['Files:', 'Directories:', 'Size:', 'Size on disk:']
    with open(os.path.join(temp_dir, 'final_stats.txt'), 'rb') as f:
        for line, term in zip(f.readlines(), du_list):
            if "Directories:" in term:
                print term, ' ', str(int(line.split(':')[1]) - 1).rstrip()
            else: 
                print term, line.split(':')[1].rstrip()
    
    print '\n\n'
    
    #delete temp folder
    shutil.rmtree(temp_dir)
    
    #delete disk image folder if empty
    try:
        os.rmdir(image_dir)
    except WindowsError:
        pass

    # remove temp html file
    try:
        os.remove(temp_html)
    except WindowsError:
        pass

    # remove sqlite db
    os.remove(os.path.join(metadata, 'siegfried.sqlite'))
    
def writePremisToExcel(ws, newrow, eventType, premis_list):
    temp_dict = {}
    temp_dict = next(item for item in premis_list if item['eventType'] == eventType)
    ws.cell(row=newrow, column=12, value = temp_dict['linkingAgentIDvalue'])
    ws.cell(row=newrow, column=13, value = temp_dict['timestamp'])
    if temp_dict['eventOutcomeDetail'] == '0' or temp_dict['eventOutcomeDetail'] == 0:
        ws.cell(row=newrow, column=14, value = "Success")
    else:
        ws.cell(row=newrow, column=14, value = "Failure")

def writeNote():
    if not verify_data():
        return
    
    spreadsheet_copy = glob.glob(os.path.join(home_dir, unit.get(), '*.xlsx'))[0]
    
    wb = openpyxl.load_workbook(spreadsheet_copy)    
    
    #need to account for situations where we need to write a note after conclusion of analysis--in these cases, we don't want to create a temp file again...
    if os.path.exists(os.path.join(home_dir, unit.get(), barcode.get(), 'temp')):
        bc_dict = cPickle_load('bc_dict')
        bc_dict['label_transcript'] = label_transcription.get(1.0, END).replace('LABEL TRANSCRIPTION:\n\n', '')
    else:
        bc_dict = {}
        
        ws1 = wb['Inventory']  
        iterrows = ws1.iter_rows()
        next(iterrows)

        for row in iterrows:
            if str(row[0].value) == '%s' % barcode.get():
                bc_dict['current_barcode'] = row[0].value
                bc_dict['current_accession'] = row[1].value
                bc_dict['current_collection'] = row[2].value
                bc_dict['current_coll_id'] = row[3].value
                bc_dict['current_creator'] = row[4].value
                bc_dict['phys_loc'] = row[5].value
                bc_dict['current_source'] = row[6].value
                bc_dict['label_transcript'] = label_transcription.get(1.0, END).replace('LABEL TRANSCRIPTION:\n\n', '')
                bc_dict['appraisal_notes'] = row[8].value
                bc_dict['bdpl_notes'] = row[9].value
                bc_dict['restriction_statement'] = row[10].value
                bc_dict['restriction_end_date'] = row[11].value
                bc_dict['initial_appraisal'] = row[12].value
                break
            else:
                continue
            
        for val in bc_dict:
            if bc_dict[val] is None:
                bc_dict[val] = '-'
            
    ws = wb['Appraisal']

    #check to make sure barcode hasn't already been written to worksheet; loop through
    for cell in ws['A']:
        if (cell.value is not None):
            if barcode.get() in str(cell.value):
                newrow = cell.row
                break
            else:
                newrow = ws.max_row+1
        else:
            newrow = ws.max_row+1

    ws.cell(row=newrow, column=1, value = bc_dict['current_barcode'])
    ws.cell(row=newrow, column=2, value = bc_dict['current_accession'].encode('utf-8'))
    ws.cell(row=newrow, column=3, value = bc_dict['current_collection'].encode('utf-8'))
    ws.cell(row=newrow, column=4, value = bc_dict['current_coll_id'].encode('utf-8'))
    ws.cell(row=newrow, column=5, value = bc_dict['current_creator'].encode('utf-8'))
    ws.cell(row=newrow, column=6, value = bc_dict['phys_loc'].encode('utf-8'))
    ws.cell(row=newrow, column=7, value = bc_dict['current_source'].encode('utf-8'))
    ws.cell(row=newrow, column=8, value = bc_dict['label_transcript'].encode('utf-8'))
    ws.cell(row=newrow, column=9, value = bc_dict['appraisal_notes'].encode('utf-8'))
    ws.cell(row=newrow, column=10, value = bc_dict['restriction_statement'].encode('utf-8'))
    ws.cell(row=newrow, column=11, value = bc_dict['restriction_end_date'].encode('utf-8'))
    
    #write technician's note
    ws.cell(row=newrow, column=15, value = noteField.get(1.0, END))

    #save and close spreadsheet
    wb.save(spreadsheet_copy)
    
    print '\n\nInformation saved to Appraisal worksheet.' 
    
def writeSpreadsheet():
    premis_list = cPickle_load('premis_list')
            
    bc_dict = cPickle_load('bc_dict')
    
    spreadsheet_copy = glob.glob(os.path.join(home_dir, unit.get(), '*.xlsx'))[0]
    
    wb = openpyxl.load_workbook(spreadsheet_copy)
    ws = wb['Appraisal']

    #check to make sure barcode hasn't already been written to worksheet; loop through
    for cell in ws['A']:
        if barcode.get() in str(cell.value):
            newrow = cell.row
            break
        else:
            newrow = ws.max_row+1

    ws.cell(row=newrow, column=1, value = bc_dict['current_barcode'])
    ws.cell(row=newrow, column=2, value = bc_dict['current_accession'].encode('utf-8'))
    ws.cell(row=newrow, column=3, value = bc_dict['current_collection'].encode('utf-8'))
    ws.cell(row=newrow, column=4, value = bc_dict['current_coll_id'].encode('utf-8'))
    ws.cell(row=newrow, column=5, value = bc_dict['current_creator'].encode('utf-8'))
    ws.cell(row=newrow, column=6, value = str(bc_dict['phys_loc']))
    ws.cell(row=newrow, column=7, value = bc_dict['current_source'].encode('utf-8'))
    #allow BDPL tech to update label transcription and save to spreadsheet
    #ws.cell(row=newrow, column=8, value = bc_dict['label_transcript'].encode('utf-8'))
    ws.cell(row=newrow, column=8, value = label_transcription.get(1.0, END).replace('LABEL TRANSCRIPTION:\n\n', ''))
    ws.cell(row=newrow, column=9, value = bc_dict['appraisal_notes'].encode('utf-8'))
    ws.cell(row=newrow, column=10, value = bc_dict['restriction_statement'].encode('utf-8'))
    ws.cell(row=newrow, column=11, value = bc_dict['restriction_end_date'].encode('utf-8'))
    
    #pull in other information about transfer: timestamp, method, outcome
    temp_dict = {}
    if check_premis('disk image creation', 'eventType'):
        writePremisToExcel(ws, newrow, 'disk image creation', premis_list)
    elif check_premis('replication', 'eventType'):
        writePremisToExcel(ws, newrow, 'replication', premis_list)
    else:
        pass
    
    #write technician's note
    ws.cell(row=newrow, column=15, value = noteField.get(1.0, END))
    
    #write appraisal information from various procedures
    appraisal_dict = cPickle_load('appraisal_dict')
    
    ws.cell(row=newrow, column=16, value = appraisal_dict['Extent'])
    ws.cell(row=newrow, column=17, value = appraisal_dict['Files'])
    ws.cell(row=newrow, column=18, value = appraisal_dict['Duplicates'])
    ws.cell(row=newrow, column=19, value = appraisal_dict['Unidentified'])
    ws.cell(row=newrow, column=20, value = appraisal_dict['Formats'])
    ws.cell(row=newrow, column=21, value = appraisal_dict['Dates'])   
    ws.cell(row=newrow, column=22, value =  appraisal_dict['Virus'])
    if 'PII' in appraisal_dict:
        ws.cell(row=newrow, column=23, value = appraisal_dict['PII'])
        
    if bc_dict['initial_appraisal'] == "No appraisal needed":
        ws.cell(row=newrow, column=24, value = "Transfer to SDA")

    ws.cell(row=newrow, column=25).value = '=HYPERLINK("{}", "{}")'.format(".\\%s\\metadata\\reports\\report.html" % barcode.get(), "View report")
    
    ws.cell(row=newrow, column=26).value = '=HYPERLINK("{}", "{}")'.format(".\\%s\\metadata\\reports\\tree.txt" % barcode.get(), "View directory tree")
    
    if jobType.get() != 'Copy_only':
        ws.cell(row=newrow, column=27).value = '=HYPERLINK("{}", "{}")'.format(".\\%s\\metadata\\media-image" % barcode.get(), "Images of media")
    
    if jobType.get() == 'DVD':
        ws.cell(row=newrow, column=28).value = 'DVD: transfer "files" to MCO'
    if jobType.get() == 'CDDA':
        ws.cell(row=newrow, column=28).value = 'CD-DA: transfer "files" to MCO'
    
    #save and close spreadsheet
    wb.save(spreadsheet_copy)       
        
def cleanUp():
    
    newscreen()
    
    
    #deselect all radio buttons
    jobType1.deselect()
    jobType2.deselect()
    jobType3.deselect()
    jobType4.deselect()
    source1.deselect()
    source2.deselect()
    source3.deselect()
    source4.deselect()
    disk525.set('N/A')

    #clear Int variables
    sourceDevice.set(None)
    jobType.set(None)

    #clear String Variables
    barcode.set('')
    source.set('')
    coll_creator.set('')
    coll_title.set('')
    xfer_source.set('')
        
    mediaStatus.set(False)
    
    #clear text widgets
    bdpl_notes.configure(state='normal')
    bdpl_notes.delete('1.0', END)
    bdpl_notes.insert(INSERT, "TECHNICIAN NOTES:\n")
    bdpl_notes.configure(state='disabled')
    
    appraisal_notes.configure(state='normal')
    appraisal_notes.delete('1.0', END)
    appraisal_notes.insert(INSERT, "APPRAISAL NOTES:\n")
    appraisal_notes.configure(state='disabled')
    
    label_transcription.configure(state='normal')
    label_transcription.delete('1.0', END)
    label_transcription.insert(INSERT, "LABEL TRANSCRIPTION:\n")
    #label_transcription.configure(state='disabled')
    
    
    noteField.delete('1.0', END)
    
    #clear Entry widgets--check if unit will be retained
    barcodeEntry.delete(0, END)
    sourceEntry.delete(0, END)
           

def closeUp():    
    
    try:
        close_files_conns_on_exit(html, conn, cursor)
    except (NameError, sqlite3.ProgrammingError) as e:
        pass
    
    #make sure siegfried is up to date
    sfup = 'sf -update'
    subprocess.call(sfup, shell=True)
    
    window.destroy()

def verify_data():
    #check that data has been entered by user
    if spreadsheet.get() == '':
        spreadsheet_copy = glob.glob(os.path.join(home_dir, unit.get(), '*.xlsx'))
        if spreadsheet_copy:
            spreadsheet.set(spreadsheet_copy[0])
        else:
            print '\n\nError; please enter the path to the collection spreadsheet'
            return False

    if barcode.get() == '':
        print '\n\nError; please make sure you have entered a barcode.'
        return False
        
    if unit.get() == '':
        '\n\nError; please make sure you have entered a 3-character unit abbreviation.'
        return False 

    return True

def verify_barcode():
    unit_home = bdpl_vars()['unit_home']
    spreadsheet_copy = os.path.join(unit_home, os.path.basename(spreadsheet.get()))
    if not os.path.exists(spreadsheet_copy):
        try:
            os.makedirs(unit_home)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
        shutil.copy(spreadsheet.get(), spreadsheet_copy)

    #once we have identified our working spreadsheet (or created it), check data:
    
    wb = openpyxl.load_workbook(spreadsheet_copy)

    #Find the barcode in the inventory sheet; save information to a dictionary so that it can be written to the Appraisal sheet later.
    bc_dict = cPickle_load('bc_dict')
    
    #if dictionary is empty, read info from spreadsheet; otherwise, retain dictionary
    if len(bc_dict) == 0:
        ws = wb['Inventory']  
        iterrows = ws.iter_rows()
        next(iterrows)
    
        for row in iterrows:
            if str(row[0].value) == '%s' % barcode.get():
                bc_dict['current_barcode'] = str(row[0].value)
                bc_dict['current_accession'] = row[1].value
                bc_dict['current_collection'] = row[2].value
                bc_dict['current_coll_id'] = row[3].value
                bc_dict['current_creator'] = row[4].value
                bc_dict['phys_loc'] = row[5].value
                bc_dict['current_source'] = row[6].value
                bc_dict['label_transcript'] = row[7].value
                bc_dict['appraisal_notes'] = row[8].value
                bc_dict['bdpl_notes'] = row[9].value
                bc_dict['restriction_statement'] = row[10].value
                bc_dict['restriction_end_date'] = row[11].value
                bc_dict['initial_appraisal'] = row[12].value
                break
            else:
                continue
                
        #exit if barcode wasn't found
        if len(bc_dict) == 0:
            print '\n\nError; barcode not found in spreadsheet.\n\nPlease review spreadsheet and correct barcode or add item to spreadsheet at %s.' % spreadsheet_copy
            return False
        
        #if the barcode was found, write to fields in GUI
        else:
            #clean up any None values
            for val in bc_dict:
                if bc_dict[val] is None:
                    bc_dict[val] = '-'
                
    coll_title.set(bc_dict['current_collection'].encode('utf-8'))
    coll_creator.set(bc_dict['current_creator'].encode('utf-8'))
    xfer_source.set(bc_dict['current_source'].encode('utf-8'))
    
    label_transcription.configure(state='normal')
    label_transcription.delete('1.0', END)
    label_transcription.insert(INSERT, 'LABEL TRANSCRIPTION:\n\n' + bc_dict['label_transcript'].encode('utf-8'))
    #label_transcription.configure(state='disabled')
    
    bdpl_notes.configure(state='normal')
    bdpl_notes.delete('1.0', END)
    bdpl_notes.insert(INSERT, "TECHNICIAN NOTES:\n\n" + bc_dict['bdpl_notes'].encode('utf-8'))
    bdpl_notes.configure(state='disabled')
    
    appraisal_notes.configure(state='normal')
    appraisal_notes.delete('1.0', END)
    appraisal_notes.insert(INSERT, "APPRAISAL NOTES:\n\n" + bc_dict['appraisal_notes'].encode('utf-8'))
    appraisal_notes.configure(state='disabled')
    
    cPickle_dump('bc_dict', bc_dict)
            
    #Next, check if barcode has already been written to appraisal sheet
    ws1 = wb['Appraisal']
    iterrows = ws1.iter_rows()
    next(iterrows)
    
    for row in iterrows:
        if str(row[0].value) == '%s' % barcode.get():
            notevalue = str(row[14].value).rstrip()
            
            noteField.configure(state='normal')
            noteField.delete('1.0', END)
            noteField.insert(INSERT, notevalue)
            
            if os.path.exists(os.path.join(bdpl_vars()['metadata'], '%s-premis.xml' % barcode.get())):
                print '\n\nNOTE: this item barcode has completed the entire ingest workflow.  Consult with the digital preservation librarian if you believe additional procedures are needed.'
                shutil.rmtree(bdpl_vars()['temp_dir'])
                return False
            else:
                premis_list = cPickle_load('premis_list')
                if len(premis_list) == 0:
                    print '\n\nItem barcode has been added to Appraisal worksheet, but no procedures have been completed.'
                else:
                    print '\n\nItem barcode has been added to Appraisal worksheet; the following procedures have been completed:\n\t', '\n\t'.join(list(set((i['%s' % 'eventType'] for i in premis_list))))
                break
        else: 
                continue
    print '\n\nRecord loaded successfully; ready for next operation.'
    return True
    
def check_unfinished():

    if unit.get() == '':
        print '\n\nEnter a unit ID'
        return
    
    for item_barcode in os.listdir(os.path.join(home_dir, unit.get())):
        if os.path.isdir(os.path.join(home_dir, unit.get(), item_barcode)):
            if os.path.exists(bdpl_vars()['temp_dir']):
                premis_list = cPickle_load('premis_list')
                if len(premis_list) == 0:
                    print '\nBarcode: %s' % item_barcode
                    print '\tItem folder structure has been created, but no ingest procedures have been completed.'
                else:
                    print '\nBarcode: %s' % item_barcode
                    print '\tThe following procedures have been completed:\n\t', '\n\t\t'.join(list(set((i['%s' % 'eventType'] for i in premis_list))))

def check_progress():
    
    if unit.get() == '':
        print '\n\nEnter a unit ID'
        return
    
    spreadsheet_copy = glob.glob(os.path.join(home_dir, unit.get(), '*.xlsx'))[0]
        
    wb = openpyxl.load_workbook(spreadsheet_copy)
    
    try:
        ws = wb['Appraisal']
    except KeyError:
        print '\n\nCheck %s; make sure "Appraisal" worksheet has not been renamed.  Consult with Digital Preservation Librarian if sheet does not exist.'
        return
    
    try:
        ws2 = wb['Inventory']
    except KeyError:
        print '\n\nCheck %s; make sure "Inventory" worksheet has not been renamed.  Consult with Digital Preservation Librarian if sheet does not exist.'
        return    
    current_total = (ws2.max_row - 1) - (ws.max_row - 1)
    
    print '\n\nCurrent status: %s out of %s items have been transferred. \n\n%s remain.' % ((ws.max_row - 1), (ws2.max_row - 1), current_total)
    
    list1 = []
    for col in ws['A'][1:]:
        list1.append(str(col.value))
    
    list2 = []
    for col in ws2['A'][1:]:
        list2.append(str(col.value))
    
    items_not_done = set(list1 + list2)
    
    print '\n\nThe following barcodes require ingest:\n%s' % '\n'.join(list(items_not_done))
    
    if len(list(items_not_done)) > 30:
        print '\n\nCurrent status: %s out of %s items have been transferred. \n\n%s remain.' % ((ws.max_row - 1), (ws2.max_row - 1), current_total)

def move_media_images():
    media_image_dir = bdpl_vars()['media_image_dir']
    unit_home = bdpl_vars()['unit_home']
    media_pics = bdpl_vars()['media_pics']
    
    if unit.get() == '':
        '\n\nError; please make sure you have entered a 3-character unit abbreviation.'
        return 
    
    
    if len(os.listdir(media_image_dir)) == 0:
        print '\n\nNo images of media at %s' % media_image_dir
        return
    
    bad_file_list = []
    for f in os.listdir(media_image_dir):
        if os.path.exists(os.path.join(unit_home, f.split('-')[0])):
            if not os.path.exists(media_pics):
                os.makedirs(media_pics)
            shutil.move(os.path.join(media_image_dir, f), media_pics)
        else:
            bad_file_list.append(f)
    if len(bad_file_list) > 0:
        print '\n\nFilenames for the following images do not match current barcodes:\n%s' % '\n'.join(bad_file_list)
        print '\nPlease correct filenames and try again.'
    else:
        print '\n\nMedia images successfully copied!'

def main():
    
    global window, source, jobType, unit, barcode, mediaStatus, source1, source2, source3, source4, disk525, jobType1, jobType2, jobType3, jobType4, sourceDevice, barcodeEntry, sourceEntry, unitEntry, spreadsheet, coll_creator, coll_title, xfer_source, appraisal_notes, bdpl_notes, noteSave, createBtn, analyzeBtn, transferBtn, noteField, label_transcription, bdpl_home, bdpl_resources, home_dir
    
    home_dir = 'Z:\\'
    bdpl_home = 'C:\\BDPL'
    bdpl_resources = os.path.join(bdpl_home, 'resources')
    
    window = Tk()
    window.title("Indiana University Library Born-Digital Preservation Lab")
    window.geometry('650x750')

    #if user tries to use 'X' button, make sure program closes correctly
    window.protocol('WM_DELETE_WINDOW', closeUp)

    '''
    
    GUI section for spreadsheet, barcode, and unit info
    
    '''
    
    topFrame = Frame(window, width=650, height=50)
    topFrame.pack(fill=BOTH)

    topLeft1 = Frame(topFrame, width=650, height=25)
    topLeft1.pack(fill=BOTH)
    topLeft2 = Frame(topFrame, width=650, height=25)
    topLeft2.pack(fill=BOTH)

    #Get unit name and barcode              
    spreadsheet = StringVar()
    spreadsheet.set('')
    spreadsheetTxt = Label(topLeft1, text="Manifest: ")
    spreadsheetTxt.pack(in_=topLeft1, side=LEFT, padx=5, pady=5)
    spreadsheetEntry = Entry(topLeft1, width=40, textvariable=spreadsheet)
    spreadsheetEntry.pack(in_=topLeft1, side=LEFT, padx=5, pady=5)
    
    spreadsheetBtn = Button(topLeft1, text="Browse", command=spreadsheet_browse)
    spreadsheetBtn.pack(in_=topLeft1, side=LEFT, padx=5, pady=5)
    
    barcode = StringVar()
    barcode.set('')           
    barcodeTxt = Label(topLeft2, text="Barcode:")
    barcodeTxt.pack(in_=topLeft2, side=LEFT, padx=5, pady=5)
    barcodeEntry = Entry(topLeft2, width=20, textvariable=barcode)
    barcodeEntry.pack(in_=topLeft2, side=LEFT, padx=5, pady=5)
    
    unit = StringVar()
    unit.set('')
    unitTxt = Label(topLeft2, text="Unit:")
    unitTxt.pack(in_=topLeft2, side=LEFT, padx=5, pady=5)
    unitEntry = Entry(topLeft2, width=5, textvariable=unit)
    unitEntry.pack(in_=topLeft2, side=LEFT, padx=5, pady=5)

    '''
    
    GUI section for job info
    
    '''

    middleFrame = Frame(window, width=650, height=150)
    middleFrame.pack(fill=BOTH)
    middleFrame.pack_propagate(False)
    
    '''
                UPPER MIDDLE
    '''
    
    upperMiddle = Frame(middleFrame, width=650, height=50)
    upperMiddle.pack(fill=BOTH)
    
    #job types: these determine which operations run on content
    jobTypeLabel = Label(upperMiddle, text="Job type:")
    jobTypeLabel.grid(column=0, row=1, padx=10, pady=5)

    jobType = StringVar()
    jobType.set(None)

    jobType1 = Radiobutton(upperMiddle, text='Copy only', value='Copy_only', variable=jobType)                     
    jobType1.grid(column=1, row=1, padx=10, pady=5)

    jobType2 = Radiobutton(upperMiddle, text='Disk image', value='Disk_image', variable=jobType)
    jobType2.grid(column=2, row=1, padx=10, pady=5)

    jobType3 = Radiobutton(upperMiddle, text='DVD', value='DVD', variable=jobType)
    jobType3.grid(column=3, row=1, padx=10, pady=5)
    
    jobType4 = Radiobutton(upperMiddle, text='CDDA', value='CDDA', variable=jobType)
    jobType4.grid(column=4, row=1, padx=10, pady=5)
    
    '''
                MID MIDDLE
    '''
    midMiddle = Frame(middleFrame, width=650, height=50)
    midMiddle.pack(fill=BOTH)
    
    #Get path to source, if needed
    source = StringVar()
    source.set('')
    sourceTxt = Label(midMiddle, text='Source (copy only): ')
    sourceTxt.pack(in_=midMiddle, side=LEFT, padx=5, pady=5)
    sourceEntry = Entry(midMiddle, width=55, textvariable=source)
    sourceEntry.pack(in_=midMiddle, side=LEFT, padx=5, pady=5)
    sourceBtn = Button(midMiddle, text="Browse", command=source_browse)
    sourceBtn.pack(in_=midMiddle, side=LEFT, padx=5, pady=5)
    
    '''
            LOWER MIDDLE
    '''
    lowerMiddle = Frame(middleFrame, width=650, height=50)
    lowerMiddle.pack(fill=BOTH)
     
    #Get source device, if needed
    sourceDevice = StringVar()
    sourceDevice.set(None)
    
    disk_type_options = ['N/A', 'Apple DOS 3.3 (16-sector)', 'Apple DOS 3.2 (13-sector)', 'Apple ProDOS', 'Commodore 1541', 'TI-99/4A 90k', 'TI-99/4A 180k', 'TI-99/4A 360k', 'Atari 810', 'MS-DOS 1200k', 'MS-DOS 360k', 'North Star MDS-A-D 175k', 'North Star MDS-A-D 350k', 'Kaypro 2 CP/M 2.2', 'Kaypro 4 CP/M 2.2', 'CalComp Vistagraphics 4500', 'PMC MicroMate', 'Tandy Color Computer Disk BASIC', 'Motorola VersaDOS']
    
    disk525 = StringVar()
    disk525.set('N/A')
    
    sourceDeviceLabel = Label(lowerMiddle, text='Media source:')
    sourceDeviceLabel.grid(column=0, row=0)
        
    source1 = Radiobutton(lowerMiddle, text='CD/DVD', value='/dev/sr0', variable=sourceDevice)
    source2 = Radiobutton(lowerMiddle, text='3.5" fd', value='/dev/fd0', variable=sourceDevice)
    source3 = Radiobutton(lowerMiddle, text='5.25" fd', value='5.25', variable=sourceDevice)
    disk_menu = OptionMenu(lowerMiddle, disk525, *disk_type_options)
    #NOTE: should probably use /dev/sdb to copy whole drive; this would also require using MMLS to find sector offset.  Test with Zip disk and USB drive...
    source4 = Radiobutton(lowerMiddle, text='Other (USB, Zip, etc.)', value='/dev/sdb1', variable=sourceDevice)

    source1.grid(column=1, row=0, padx=10, pady=5)
    source2.grid(column=2, row=0, padx=10, pady=5)
    source3.grid(column=3, row=0, padx=10, pady=5)
    disk_menu.grid(column=4, row=0, padx=10, pady=5)
    source4.grid(column=5, row=0, padx=10, pady=5)

    #buttons: kick off various functions    
    newBtn = Button(lowerMiddle, text="New transfer", command=cleanUp)
    newBtn.grid(column=0, row=2, padx=10, pady=5)

    createBtn = Button(lowerMiddle, text="Load record", command=first_run)
    createBtn.grid(column=1, row=2, padx=10, pady=5)

    transferBtn = Button(lowerMiddle, text="Transfer", command=TransferContent)
    transferBtn.grid(column=2, row=2, padx=10, pady=5)

    analyzeBtn = Button(lowerMiddle, text="Analysis", command=analyzeContent)
    analyzeBtn.grid(column=3, row=2, padx=10, pady=5)
        
    closeBtn = Button(lowerMiddle, text="  Quit  ", command=closeUp)
    closeBtn.grid(column=4, row=2, padx=10, pady=5)

    mediaStatus = BooleanVar()
    mediaStatus.set(False)
    mediaStatusChk = Checkbutton(lowerMiddle, text="Media present?", variable=mediaStatus)
    mediaStatusChk.grid(column=5, row=2)
    
    '''
    
    GUI section for BDPL technician note
    
    '''
    noteFrame = Frame(window, width=650, height=50)
    noteFrame.pack(fill=BOTH)
    
    noteLabel = Label(noteFrame, text="BDPL\nnote:", anchor='w')
    noteLabel.grid(row=1, column=0, pady=10)
    
    noteScroll = Scrollbar(noteFrame)
    noteField = Text(noteFrame, height=3)
    noteScroll.config(command=noteField.yview)
    noteField.config(yscrollcommand=noteScroll.set)
    
    noteField.grid(row=1, column=1, sticky="nsew", padx=(10, 0), pady=10)
    noteFrame.grid_rowconfigure(1, weight=1)
    noteFrame.grid_columnconfigure(1, weight=1)
    
    noteScroll.grid(row=1, column=2, padx=(0, 10), pady=10, sticky=NS)
    
    noteSave = Button(noteFrame, text="Save", command=writeNote)
    noteSave.grid(row=1, column=3, padx=10, pady=(2, 10))
    
    '''
    GUI section for additional actions/features
    '''
    bottomFrame = Frame(window, width=650, height=50)
    bottomFrame.pack(fill=BOTH)
    bottomFrame.pack_propagate(False)
    
    check_spreadsheet = Button(bottomFrame, text="Check spreadsheet", command=check_progress)
    check_spreadsheet.grid(row=0, column=0, padx=30)
    
    move_pics = Button(bottomFrame, text="Move media images", command=move_media_images)
    move_pics.grid(row=0, column=1, padx=30)
    
    unfinished_check = Button(bottomFrame, text="Check unfinished", command=check_unfinished)
    unfinished_check.grid(row=0, column=2, padx=30)
    
    '''
    GUI section with metadata      
    '''
    
    borderFrame = Frame(window, width=650, height=5, bg='black')
    borderFrame.pack(fill=BOTH, padx=10, pady=10)
    borderLabel = Label(borderFrame, text="Information about transfer:")
    borderLabel.pack()
    borderLabel.config(fg='white', bg='black')
    
    inventoryFrame = Frame(window, width=650, height=300)
    inventoryFrame.pack(fill=BOTH)
    
    inventoryTop = Frame(inventoryFrame, width=650, height=50)
    inventoryTop.pack(fill=BOTH)
    #inventoryTop.pack_propagate(0)
    inventoryTop.grid_columnconfigure(1, weight=1)
    
    inventoryBottom = Frame(inventoryFrame, width=650, height=250)
    inventoryBottom.pack(fill=BOTH)
    #inventoryBottom.pack_propagate(0)
    #inventoryBottom.grid_columnconfigure(0, weight=1)
    
    #pull in information from spreadsheet so tech can see what's going on
    coll_title = StringVar()
    coll_title_Label = Label(inventoryTop, text="Coll.\ntitle:")
    coll_title_Display = Label(inventoryTop, wraplength=250, justify=LEFT, textvariable=coll_title)
    coll_title_Label.grid(row=0, column=0, padx=5)
    coll_title_Display.grid(row=0, column=1, padx=5, sticky='w')
    
    coll_creator = StringVar()
    coll_creator_Label = Label(inventoryTop, text="Creator:")
    coll_creator_Display = Label(inventoryTop, wraplength=250, justify=LEFT, textvariable=coll_creator)
    coll_creator_Label.grid(row=1, column=0, padx=5)
    coll_creator_Display.grid(row=1, column=1, padx=5, sticky='w')

    xfer_source = StringVar()
    xfer_source_Label = Label(inventoryTop, text="Source:")
    xfer_source_Display = Label(inventoryTop, textvariable=xfer_source)
    xfer_source_Label.grid(row=2, column=0, padx=5)
    xfer_source_Display.grid(row=2, column=1, padx=5, sticky='w')   
    
    #some larger fields with potential for more text   
    appraisal_notes = Text(inventoryBottom, height=4, width=70)
    appraisal_scroll = Scrollbar(inventoryBottom)
    appraisal_scroll.config(command=appraisal_notes.yview)
    appraisal_notes.config(yscrollcommand=appraisal_scroll.set)
    appraisal_notes.insert(INSERT, "APPRAISAL NOTES:\n")
    appraisal_notes.grid(row=0, column=0, pady=5, padx=(5,0))
    appraisal_scroll.grid(row=0, column=1, pady=5, sticky='ns')
    appraisal_notes.configure(state='disabled')
    
    label_transcription = Text(inventoryBottom, height=4, width=70)
    label_scroll = Scrollbar(inventoryBottom)
    label_scroll.config(command=label_transcription.yview)
    label_transcription.config(yscrollcommand=label_scroll.set)
    label_transcription.insert(INSERT, "LABEL TRANSCRIPTION:\n")
    label_transcription.grid(row=1, column=0, pady=5, padx=(5,0))
    label_scroll.grid(row=1, column=1, pady=5, sticky='ns')
    #label_transcription.configure(state='disabled')
    
    bdpl_notes = Text(inventoryBottom, height=4, width=70)
    bdpl_scroll = Scrollbar(inventoryBottom)
    bdpl_scroll.config(command=bdpl_notes.yview)
    bdpl_notes.config(yscrollcommand=bdpl_scroll.set)
    bdpl_notes.insert(INSERT, "TECHNICIAN NOTES:\n")
    bdpl_notes.grid(row=2, column=0, pady=5, padx=(5,0))
    bdpl_scroll.grid(row=2, column=1, pady=5, sticky='ns')
    bdpl_notes.configure(state='disabled')
    
    
    window.mainloop()

def newscreen():
    os.system('cls')
    
    #print BDPL screen
    fname = "C:/BDPL/scripts/bdpl.txt"
    if os.path.exists(fname):
        with open(fname, 'r') as fin:
            print fin.read()
    else:
        print 'Missing ASCII art header file; download to: %s' % fname
        
def spreadsheet_browse():
    currdir = "Z:\\spreadsheets"
    selected_file = tkFileDialog.askopenfilename(parent=window, initialdir=currdir, title='Please select your inventory spreadsheet')
    if len(selected_file) > 0:
        spreadsheet.set(selected_file)
        
def source_browse():
    currdir = "Z:\\"
    selected_dir = tkFileDialog.askdirectory(parent=window, initialdir=currdir, title='Please select the source directory')
    if len(selected_dir) > 0:
        source.set(selected_dir)
        

if __name__ == '__main__':
    main()
