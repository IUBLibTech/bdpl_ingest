import subprocess
import os
import shutil
import bagit
import sys
import openpyxl
import hashlib
import datetime
import pickle
import glob
import csv
from lxml import etree
import uuid

def list_write(list_name, barcode, message=None):
    with open(list_name, 'a') as current_list:
        if message is None:
            current_list.write('%s\n' % barcode)
        else:
            current_list.write('%s\t%s\n' % (barcode, message))
    
def check_list(list_name, barcode):
    if not os.path.exists(list_name):
        return False
    with open(list_name, 'r') as f:
        for item in f:
            if barcode in item.strip():
                return True
            else:
                continue
        return False

def get_size(start_path):
    total_size = 0
    if os.path.isfile(start_path):
        total_size = os.path.getsize(start_path)
    else:
        for dirpath, dirnames, filenames in os.walk(start_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    return total_size

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def raw_text(text):
    """Returns a raw string representation of text"""
    
    escape_dict={'\a':r'\a',
           '\b':r'\b',
           '\c':r'\c',
           '\f':r'\f',
           '\n':r'\n',
           '\r':r'\r',
           '\t':r'\t',
           '\v':r'\v',
           '\'':r'\'',
           '\"':r'\"',
           '\0':r'\0',
           '\1':r'\1',
           '\2':r'\2',
           '\3':r'\3',
           '\4':r'\4',
           '\5':r'\5',
           '\6':r'\6',
           '\7':r'\7',
           '\8':r'\8',
           '\9':r'\9'}
    
    new_string=''
    for char in text:
        try: 
            new_string+=escape_dict[char]
        except KeyError: 
            new_string+=char
    return new_string

def separate_content(sep_dest, file, log, report_dir, barcode):
    #We will store results for this file in a temp file for the whole barcode.
    separated_file_stats = os.path.join(report_dir, '%s-separation-stats.txt' % barcode)
    temp_list = []
    
    #if this temp file exists, retrieve it and check if this file has already been moved.
    if os.path.exists(separated_file_stats):
        with open(separated_file_stats, 'rb') as f:
            temp_list = pickle.load(f)    
        
        #if we already have a list of separated files, check to see if file has already been moved
        for f in temp_list:
            if f['file'] == file:
                #if file previously failed, remove it from the list.
                if f['result'] != 'Moved':
                    temp_list.remove(f)
                    break
                #if the file was moved, return to main() and go on to next one.
                else:
                    return
    
    print('\t\t%s' % file)
    
    #create folder structure, if needed
    if not os.path.exists(os.path.dirname(sep_dest)):
        os.makedirs(os.path.dirname(sep_dest))
    
    #get separation stats: size 
    size = get_size(file)
    
    mod_date = datetime.datetime.fromtimestamp(os.path.getmtime(file)).isoformat()
    
    #get format puid using siegrfried
    try:
        cmd = 'sf -csv "%s"' % file
        #just pull out the puid from siegrfried output
        puid = subprocess.check_output(cmd, text=True).split('\n')[1].split(',')[5]
    except subprocess.CalledProcessError as e:
        pass
    
    #check if file is a disk image or extracted file
    if 'disk-image' in file:
        type = 'disk-image'
    else:
        type = 'extracted-file'
    
    try:
        #remove read only attribute, if necessary
        # cmd = 'ATTRIB -r "%s"' % file
        # subprocess.call(cmd, shell=True)
        
        #now move file
        shutil.move(file, sep_dest)
        with open(log, 'a') as f:
            f.write('%s\t%s\t%s\n' % (file, size, mod_date))
        result = 'Moved'
            
    except (shutil.Error, OSError, IOError, PermissionError) as e:
        #print('\n\tError separating %s: %s' % (file, e))
        result = e

    #add file to our temp list, noting result of operation and file size.  Write temp list to file.
    temp_list.append({'barcode' : barcode, 'file' : file, 'size' : size, 'result' : result, 'type' : type, 'puid' : puid})
    
    with open(separated_file_stats, 'wb') as f:
        pickle.dump(temp_list, f)
    
def main():
    
    '''SET VARIABLES'''
    #Identify where files will be moved ###### UPDATE TO ARCHIVER location #######
    destination = 'Y:/Archiver_spool/general%2fmediaimages'
    
    #open master workbook and get ready to write
    master_spreadsheet = 'Y:/spreadsheets/bdpl_master_spreadsheet.xlsx'
    master_wb = openpyxl.load_workbook(master_spreadsheet)
    item_ws = master_wb['Item']
    cumulative_ws = master_wb['Cumulative']
    
    print('\nNOTE: destination is at %s and master spreadsheet is at %s' % (destination, master_spreadsheet))
    
    #get unit name and shipment folder
    while True:

        shipment = input('\nFull path to shipment folder: ')
        shipment = shipment.replace('"', '').rstrip()
        
        if not os.path.exists(shipment):
            print('Shipment folder not recognized; enclose in quotes and use "/".\n')
            continue
        
        packaging_info = os.path.join(shipment, 'packaging-info.txt')
        if not os.path.exists(packaging_info):
            #check on spreadsheet before we go any further; the following will help make sure that a hidden temp file doesn't foul things up
            spreadsheets = list(set(glob.glob(os.path.join(shipment, '*.xlsx'))) - set(glob.glob(os.path.join(shipment, '~*.xlsx'))))
            if len(spreadsheets) !=1:
                print('\nWARNING: cannot identify shipment spreadsheet.  Please check directory to make sure .XLSX file is present.')
                print(spreadsheets)
                continue
            else:
                spreadsheet = spreadsheets[0]
                
            unit = input('\nUnit name: ')

            sep_check = input('\nDoes shipment have any content to be separated/deaccessioned? (y/n) ')
            
            check = input('\nIs information entered correctly? (y/n) ')
            
            if check.lower().strip() == 'y':           
                    
                #make sure separations manifest is here
                if sep_check.lower().strip() == 'y':
                    manifest_check = glob.glob(os.path.join(shipment, 'separations.txt'))
                    
                    if len(manifest_check) != 1:
                        print('\nWARNING: cannot identify separations manifest.  Please check directory to make sure separations.txt is present.')
                        continue
                    else:
                        separations_manifest = manifest_check[0]
                else:
                    separations_manifest = ''
                    
                info = {'unit' : unit, 'spreadsheet' : spreadsheet, 'separations_manifest' : separations_manifest}
                
                with open(packaging_info, 'wb') as f:
                    pickle.dump(info, f)
        else:
            info = {}
            with open(packaging_info, 'rb') as f:
                info = pickle.load(f)
            
            unit = info['unit'] 
            spreadsheet = info['spreadsheet']
            separations_manifest = info['separations_manifest']    
            
        break


    shipmentID = os.path.basename(shipment)

    #set shipment directory as current working directory
    os.chdir(shipment) 
     
    #open shipment workbook
    wb = openpyxl.load_workbook(spreadsheet)
    ws = wb['Appraisal']
    
    #folders/files
    report_dir = os.path.join(shipment, 'reports')
    stats_doc = os.path.join(report_dir, 'shipment_stats.txt')
    started_list = os.path.join(report_dir, 'started.txt') #to document all the barcodes that we started to package/process; all should be accounted for in the fail/complete lists
    deaccession_list = os.path.join(report_dir, 'deaccession.txt')    #for barcodes/items that will not be sent to SDA
    failed_list = os.path.join(report_dir, 'failed-packaging.txt') #for any failures; will note which stage the failure occurred 
    bagged_list = os.path.join(report_dir, 'bagged.txt') #for barcodes that were successfully bagged
    tarred_list = os.path.join(report_dir, 'tarred.txt') #for barcodes that were successfully tarred
    cleaned_list = os.path.join(report_dir, 'cleaned.txt') #for barcode folders that were successfully cleaned and SIP creation completed
    other_list = os.path.join(report_dir, 'other-decision.txt') #for barcode folders have an alternate appraisal decision
    moved_list = os.path.join(report_dir, 'moved.txt') #for barcodes that reached the end of the process; should include any that were deaccessioned
    metadata_list = os.path.join(report_dir, 'metadata.txt') #for barcodes that have metadata written to spreadsheet
    separated_list = os.path.join(report_dir, 'separated-content.txt') #for barcodes that have undergone separations
    unaccounted_list = os.path.join(report_dir, 'unaccounted.txt') #for barcodes that are in directory, but not in spreadsheet; need to check for data entry errors
    format_report = os.path.join(report_dir, 'cumulative-formats.txt') # for tracking information on file formats
    puid_report = os.path.join(report_dir, 'puid-report.txt') # list of all puids in shipment
    duration_doc = os.path.join(report_dir, 'duration.txt')
    missing_doc = os.path.join(report_dir, 'missing.txt')
    
    '''SET UP: VERIFY SPREADSHEET, GATHER INITIAL STATS, AND CHECK FOR INCONSISTENCIES WITH BARCODE FOLDERS IN SHIPMENT'''
    #make sure column headings are correct; exit if they don't match targets
    if ['Appraisal results', 'Source type', 'Label transcription', 'Initial appraisal notes'] != [ws['AA1'].value, ws['G1'].value, ws['H1'].value, ws['I1'].value]:
        print('\n\nERROR: SPREADSHEET COLUMNS ARE NOT IN CORRECT ORDER')
        print('Current headings:\n -AA1 (Appraisal results) = %s\n - G1 (Source type) = %s\n - H1 (Label transcription) = %s\n - I1 (Initial appraisal notes) = %s' % (ws['AA1'].value, ws['G1'].value, ws['H1'].value, ws['I1'].value))
        sys.exit(1)

    #we only want to run these steps the first time through...
    if not os.path.exists(report_dir):
        
        #determine if directory includes folders not on spreadsheet and/or if spreadsheet has barcodes not in directory
        spreadsheet_list = []
        for col in ws['A']:
            if col.value == 'Identifier':
                continue
            else:
                spreadsheet_list.append(col.value)
        
        dir_list = next(os.walk(shipment))[1]
    
        missing_from_dir = list(set(spreadsheet_list) - set(dir_list))
        missing_from_spreadsheet = list(set(dir_list) - set(spreadsheet_list))
    
        #before we create any temp folders, get date range of barcode directories for shipment stats (acquired by using max/min of dir_list, using modified date as key)
        latest_date = datetime.datetime.fromtimestamp(os.stat(max(dir_list, key=os.path.getmtime)).st_ctime).strftime('%Y%m%d')
        earliest_date = datetime.datetime.fromtimestamp(os.stat(min(dir_list, key=os.path.getmtime)).st_ctime).strftime('%Y%m%d')
        
        tdelta = datetime.datetime.strptime(latest_date, '%Y%m%d') - datetime.datetime.strptime(earliest_date, '%Y%m%d')
        
        #use 1 day as minimum timedelta
        if tdelta < datetime.timedelta(days=1):
            duration = 1
        else:
            duration = int(str(tdelta).split()[0])
            
        duration_stats = {'duration' : duration, 'earliest' : earliest_date, 'latest' : latest_date}
    
        #make our report directory
        if not os.path.exists(report_dir):
            os.mkdir(report_dir)
        
        #write duration and 'missing' stats to file
        with open(duration_doc, 'wb') as file:
            pickle.dump(duration_stats, file)
        with open(missing_doc, 'wb') as file:
            pickle.dump((missing_from_dir, missing_from_spreadsheet), file)
            
        #If we have unaccounted barcodes; save list to file and move the dirs themselves to an 'unaccounted' folder
        if len(missing_from_spreadsheet) > 0:
            
            if not os.path.exists('unaccounted'):
                os.mkdir('unaccounted')   
                
            with open(unaccounted_list, 'a') as f:
                for item in missing_from_spreadsheet:
                    try:
                        shutil.move(item, 'unaccounted')
                        # cmd = 'mv -f %s "unaccounted"' % item
                        # subprocess.call(cmd, shell=True)
                        f.write('%s\n' % item)
                    except (PermissionError, OSError) as e:
                    #except subprocess.CalledProcessError as e:
                        list_write(failed_list, barcode, 'move_unaccounted\t%s' % e)
                    
    
    #if this is a subsequent run-through, get missing stats from file
    else:
        with open(missing_doc, 'rb') as file:
            missing_from_dir, missing_from_spreadsheet = pickle.load(file)

    '''INITIATE PACKAGING'''
    #loop through spreadsheet, skipping header row
    iterrows = ws.iter_rows()
    next(iterrows)
    
    for row in iterrows:

        barcode = str(row[0].value)
        
        #skip to next barcode if current one does not have a folder in the shipment direcetory
        if barcode in missing_from_dir:
            continue
        
        #document that we've started working on this barcode
        if not check_list(started_list, barcode):
            list_write(started_list, barcode)
        
        #skip to next barcode if current one has already finished workflow
        if check_list(cleaned_list, barcode):
            print('\n%s completed.' % barcode)
            continue
            
        print('\nWorking on item: %s' % barcode)    
        
        #if content will not be moved to SDA, just skip folder for now and write to skipped and moved lists
        if str(row[26].value) == "Delete content":
            if not check_list(deaccession_list, barcode):
                
                if not os.path.exists('deaccessioned'):
                    os.mkdir('deaccessioned')
                
                try:
                    shutil.move(barcode, "deaccessioned")
                    # cmd = 'mv -f %s "deaccessioned"' % barcode
                    # subprocess.call(cmd, shell=True)
                    print('\n\tContent will not be transferred to SDA.  Continuing with next item.')
                    list_write(deaccession_list, barcode)
                except (PermissionError, OSError) as e:
                #except subprocess.CalledProcessError as e:
                    list_write(failed_list, barcode, 'deaccession\t%s' % e)
                    
                continue
            
            else:
                print('\n\t%s has been moved to the "deaccession" folder.' % barcode)
        
        #if content has been determined to be of value, complete prep workflow.
        elif str(row[26].value) == "Transfer to SDA":
            
            '''CHECK THAT FOLDER EXISTS'''
            #get full path to barcode folder.
            target = os.path.join(shipment, barcode)
            
            #make sure folder exists; note failure if missing
            if not os.path.exists(target):
                print('\n\tBarcode folder does not exist!')
                list_write(failed_list, barcode, 'check_folder\tFOLDER DOES NOT EXIST')
                continue
            
            #make sure that target contains content; first, check the appraisal spreadsheet 
            if not check_list(bagged_list, barcode):
                try:
                    if row[17].value == 0 and len(os.listdir(os.path.join(target, 'disk-image'))) == 0: 
                        list_write(failed_list, barcode, 'check_folder\tNO CONTENT IN BARCODE FOLDER: CHANGE APPRAISAL DECISION?')
                        continue
                except TypeError:
                    if get_size(os.path.join(target, 'files')) == 0 and len(os.listdir(os.path.join(target, 'disk-image'))) == 0: 
                        list_write(failed_list, barcode, 'check_folder\tNO CONTENT IN BARCODE FOLDER: CHANGE APPRAISAL DECISION?')
                        continue
            
            #get file format info to include with master spreadsheet.  See if we've already saved a tally of this
            format_list = []
            if os.path.exists(format_report):
                with open(format_report, 'rb') as f:
                    format_list = pickle.load(f)
            
            puid_list = []                    
            if os.path.exists(puid_report):
                with open(puid_report, 'rb') as f:
                    puid_list = pickle.load(f)
            
            #Copy format information
            format_csv = os.path.join(target, 'metadata', 'reports', 'formatVersions.csv')
            if os.path.exists(format_csv):
                temp_list = []
                with open(format_csv, 'r') as fi:
                    fi = csv.reader(fi)
                    #skip header row
                    next(fi)
                    #loop through format csv; create a dictionary for each row 
                    for line in fi:
                        temp_dict = {}
                        temp_dict['puid'] = line[1]
                        temp_dict['format'] = line[0]
                        temp_dict['version'] = line[2]
                        temp_dict['count'] = int(line[3])
                        
                        #add temp dict to a temp list
                        temp_list.append(temp_dict)
                        
                        #add puids to a master list
                        puid_list.append(line[1])
                
                #first time through, just append our barcode format dictionary
                if len(format_list) == 0:
                    format_list.append({ barcode : temp_list})
                
                #otherwise, make sure barcode hasn't already been included.
                else:
                    if not barcode in [list(c)[0] for c in format_list]:
                        format_list.append({ barcode :temp_list})
                    
                #and now write this back to file in case program closes.
                with open(format_report, 'wb') as f:
                    pickle.dump(format_list, f)
                
                with open(puid_report, 'wb') as f:
                    pickle.dump(puid_list, f)
            
            '''REMOVE SEPARATED CONTENT AND TEMP FILES/FOLDERS'''
            if not check_list(separated_list, barcode):
                print('\n\tSeparating unnecessary files...\n')
                
                #remove bulk_extractor folder, if present, as well as reports used solely for appraisal/review
                for dir in ['bulk_extractor', 'temp']:
                    remove_dir = os.path.join(target, dir)
                    if os.path.exists(remove_dir):
                        shutil.rmtree(remove_dir)
             
                for f in ["duplicates.csv", "errors.csv", "formats.csv", "formatVersions.csv", "mimetypes.csv", "unidentified.csv", "uniqueyears.csv", "years.csv", 'email_domain_histogram.txt', 'find_histogram.txt', 'telephone_histogram.txt', 'report.html']:
                    report = os.path.join(target, 'metadata', 'reports', f)
                    if os.path.exists(report):
                        os.remove(report)
                        
                assets = os.path.join(target, 'metadata', 'reports', 'assets')
                if os.path.exists(assets):
                    shutil.rmtree(assets)
                        
                #remove any files that need to be separated
                if os.path.isfile(separations_manifest):
                    #set up a log file
                    separations_log = os.path.join(target, 'metadata', 'logs', 'separations.txt')
                    
                    #get a list of relevant lines from the separations manifest, splitting at the barcode (to avoid any differences with absolute paths)
                    to_be_separated = []
                    with open(separations_manifest, 'r') as f:
                        sep_list = f.read().splitlines()
                    for file in sep_list:
                        if barcode in file:
                            name = raw_text(file.replace('"', '').rstrip())
                            to_be_separated.append(name.split('%s\\' % shipmentID, 1)[1])
                    
                    #if we've found any files, loop through list
                    if len(to_be_separated) > 0:
                        
                        for item in to_be_separated:
                            wildcard_list = []
                            
                            #if a wildcard is used, we will use glob to build a list of all files/folders matching pattern
                            if '*' in item:
                                
                                #recursive option
                                if '\\**' in item:
                                    wildcard_list = glob.glob(item, recursive=True)
                            
                                #wildcard at one level
                                elif '\\*' in item:
                                    wildcard_list = glob.glob(item)
                                
                                #now loop through this list of files/folders identified by glob
                                for wc in wildcard_list:
                                    sep_dest = os.path.join('deaccessioned', wc)
                                    separate_content(sep_dest, wc, separations_log, report_dir, barcode)
                            
                            elif os.path.isdir(item):
                                #build recursive list of all files in the folder
                                for root, dirs, files in os.walk(item):
                                    for f in files:
                                        wildcard_list.append(os.path.join(root, f))
                                #loop through the list
                                for wc in wildcard_list:
                                    sep_dest = os.path.join('deaccessioned', wc)
                                    separate_content(sep_dest, wc, separations_log, report_dir, barcode)   

                                #now remove the folder
                                cmd = 'RD /S /Q "%s"' % item
                                try:
                                    subprocess.call(cmd, shell=True)
                                except subprocess.CalledProcessError as e:
                                    pass
                                             
                            elif os.path.isfile(item):
                                sep_dest = os.path.join('deaccessioned', item)
                                separate_content(sep_dest, item, separations_log, report_dir, barcode)                        
                
                        #compile stats and check to see if any errors reported
                        separated_file_stats = os.path.join(report_dir, '%s-separation-stats.txt' % barcode)
                        file_stat_list = []
                        if os.path.exists(separated_file_stats):
                            with open(separated_file_stats, 'rb') as f:
                                file_stat_list = pickle.load(f)
                            
                            #loop though list of stats; add to file and byte counts and note any failures
                            sep_files = 0
                            sep_size = 0
                            di_sep = 0
                            temp_puid = []
                            success = True
                            for f in file_stat_list:
                                if f['result'] != 'Moved':
                                    success = False
                                    pass
                                else:
                                    if f['type'] == 'extracted-file':
                                        sep_files += 1
                                        sep_size += f['size']
                                        #create a running list of puids for later
                                        temp_puid.append(f['puid'])
                                    else:
                                        di_sep += 1
                                        
                                        #remove disk-image folder if no longer needed
                                        try:
                                            os.rmdir(os.path.join(target, 'disk-image'))
                                        except OSError:
                                            pass
                            
                            #if we have any failures, this barcode will fail: we need to make sure any files designated for separation have been removed.        
                            if not success:
                                print('\n\tNOTE: one or more errors with separations.')
                                list_write(failed_list, barcode, 'Separations\tSee %s for details.' % separated_file_stats)
                                continue
                            else:
                                print('\n\tSeparations completed: %s files separated (%s bytes)' % (sep_files, sep_size))
                                list_write(separated_list, barcode, '%s\t%s\t%s' % (str(sep_files), str(sep_size), str(di_sep)))
                                
                                #get a count for each puid in our temp list; add to dictionary
                                sep_puid_count = {puid:temp_puid.count(puid) for puid in temp_puid}
                                
                                separated_puids = os.path.join(report_dir, '%s_separated_puids.txt' % barcode)
                                with open(separated_puids, 'wb') as f:
                                    pickle.dump(sep_puid_count, f)
                                
                                #os.remove(separated_file_stats)
                                
                                #add information on preservation event to PREMIS metadata with lxml
                                premis = os.path.join(target, 'metadata', '%s-premis.xml' % barcode)
                                PREMIS_NAMESPACE = "http://www.loc.gov/premis/v3"
                                PREMIS = "{%s}" % PREMIS_NAMESPACE
                                NSMAP = {'premis' : PREMIS_NAMESPACE, "xsi": "http://www.w3.org/2001/XMLSchema-instance"}

                                parser = etree.XMLParser(remove_blank_text=True)
                                
                                tree = etree.parse(premis, parser=parser)
                                root = tree.getroot()
                                
                                event = etree.SubElement(root, PREMIS + 'event')
                                eventID = etree.SubElement(event, PREMIS + 'eventIdentifier')
                                eventIDtype = etree.SubElement(eventID, PREMIS + 'eventIdentifierType')
                                eventIDtype.text = 'UUID'
                                eventIDval = etree.SubElement(eventID, PREMIS + 'eventIdentifierValue')
                                eventIDval.text = str(uuid.uuid4())

                                eventType = etree.SubElement(event, PREMIS + 'eventType')
                                eventType.text = 'deaccession'

                                eventDateTime = etree.SubElement(event, PREMIS + 'eventDateTime')
                                eventDateTime.text = str(datetime.datetime.now())

                                eventDetailInfo = etree.SubElement(event, PREMIS + 'eventDetailInformation')
                                eventDetail = etree.SubElement(eventDetailInfo, PREMIS + 'eventDetail')
                                if di_sep == 0:
                                    eventDetail.text = 'Removed %s files (%s bytes).  See "./logs/separations.txt" for list of deaccessioned content.' % (sep_files, sep_size) 
                                else:
                                    eventDetail.text = 'Removed %s files (%s bytes) and %s disk image.  See "./logs/separations.txt" for list of deaccessioned content.' % (sep_files, sep_size, di_sep) 
                                eventDetailInfo = etree.SubElement(event, PREMIS + 'eventDetailInformation')
                                eventDetail = etree.SubElement(eventDetailInfo, PREMIS + 'eventDetail')
                                eventDetail.text = 'Formal removal of an object from the inventory of a repository.'

                                eventOutcomeInfo = etree.SubElement(event, PREMIS + 'eventOutcomeInformation')
                                eventOutcome = etree.SubElement(eventOutcomeInfo, PREMIS + 'eventOutcome')
                                eventOutcome.text = '0'
                                eventOutDetail = etree.SubElement(eventOutcomeInfo, PREMIS + 'eventOutcomeDetail')
                                eventOutDetailNote = etree.SubElement(eventOutDetail, PREMIS + 'eventOutcomeDetailNote')
                                eventOutDetailNote.text = 'Successful completion'

                                linkingAgentID = etree.SubElement(event, PREMIS + 'linkingAgentIdentifier')
                                linkingAgentIDtype = etree.SubElement(linkingAgentID, PREMIS + 'linkingAgentIdentifierType')
                                linkingAgentIDtype.text = 'local'
                                linkingAgentIDvalue = etree.SubElement(linkingAgentID, PREMIS + 'linkingAgentIdentifierValue')
                                linkingAgentIDvalue.text = 'IUL BDPL'
                                linkingAgentRole = etree.SubElement(linkingAgentID, PREMIS + 'linkingAgentRole')
                                linkingAgentRole.text = 'implementer'
                                linkingAgentID = etree.SubElement(event, PREMIS + 'linkingAgentIdentifier')
                                linkingAgentIDtype = etree.SubElement(linkingAgentID, PREMIS + 'linkingAgentIdentifierType')
                                linkingAgentIDtype.text = 'local'
                                linkingAgentIDvalue = etree.SubElement(linkingAgentID, PREMIS + 'linkingAgentIdentifierValue')
                                linkingAgentIDvalue.text = 'bdpl_pag-prep.py (https://github.com/IUBLibTech/bdpl_ingest/blob/master/bdpl_bag-prep.py)'
                                linkingAgentRole = etree.SubElement(linkingAgentID, PREMIS + 'linkingAgentRole')
                                linkingAgentRole.text = 'executing software'
                                linkingObjectID = etree.SubElement(event, PREMIS + 'linkingObjectIdentifier')
                                linkingObjectIDtype = etree.SubElement(linkingObjectID, PREMIS + 'linkingObjectIdentifierType')
                                linkingObjectIDtype.text = 'local'
                                linkingObjectIDvalue = etree.SubElement(linkingObjectID, PREMIS + 'linkingObjectIdentifierValue')
                                linkingObjectIDvalue.text = barcode
                                
                                #add new event to root etree.Element; write to etree.ElementTree
                                root.append(event)
                                premis_text = etree.tostring(tree, pretty_print=True, xml_declaration=True, encoding="UTF-8")
                                
                                #now write to file
                                with open(premis, 'wb') as f:
                                    f.write(premis_text)
                                
                    else:
                        print('\tSeparations completed.')
                        list_write(separated_list, barcode, '0\t0\t0') #include 0s for # of files separated, associated bytes, disk image
                        
            '''BAG FOLDER'''
            #make sure we haven't already bagged folder
            if not check_list(bagged_list, barcode):
            
                print('\n\tCreating bag for barcode folder...')
                
                #description for bag-info.txt currently includes source media, label transcription, and appraisal note.
                desc = '%s. %s. %s' %(str(row[6].value), str(row[7].value), str(row[8].value))
                desc.replace('\n', ' ')    
        
                try:
                    bagit.make_bag(target, {"Source-Organization" : unit, "External-Description" : desc, "External-Identifier" : barcode}, checksums=["md5"])
                    list_write(bagged_list, barcode)
                    print('\tBagging complete.')
                except (RuntimeError, PermissionError, bagit.BagError, OSError) as e:
                    print("\tUnexpected error: ", e)
                    list_write(failed_list, barcode, 'bagit\t%s' % e)
                    
                    #write info so we can use it later
                    info = {'unit' : unit, 'description' : desc}
                    with open(os.path.join(target, '%s-bag-info.txt' % barcode), 'wb') as fi:
                        pickle.dump(info, fi)
                    
                    continue
            
            '''CREATE TAR'''
            #make sure file hasn't already been tarred
            if not check_list(tarred_list, barcode):
                
                #Make sure we have enough space to create tar file (just to be sure; we should check first, as a rule)
                print('\n\tChecking available space..')
                
                #first check available space
                (total, used, free) = shutil.disk_usage(os.getcwd())
                
                #now get size of target
                cmd = 'du -s %s' % target
                dir_size = int(subprocess.check_output(cmd, shell=True, text=True).split()[0])
                
                #check if the new archive will have sufficient space on disk; include addition 10240 bytes for tar file. Ff so, continue.  If not, exit with a warning
                available_space = int(free) - (dir_size * 2 + 10240)
                
                if available_space <= 0:
                    print('\n\tWARNING! Insufficient space to create tar archive.\n\t\tAvailable space: %s\n\t\tSize needed for archive: %s' % (free, string(dir_size)))
                    list_write(failed_list, barcode, 'Insufficient space to create tar archive; Need minium of %s bytes' % (free, string(dir_size)))
                    continue
                else:
                    print('\tCheck complete; sufficient space for tar file.')
                
                #tar folder
                print('\n\tCreating tar archive...')
                tar_file = '%s.tar' % os.path.basename(target)
                cmd = 'tar -cf %s %s' % (tar_file, os.path.basename(target))
                
                try:
                    subprocess.check_output(cmd, shell=True)
                    list_write(tarred_list, barcode)
                    print('\tTar archive created')
                except (RuntimeError, PermissionError, IOError, EnvironmentError, subprocess.CalledProcessError) as e:
                    print("\tUnexpected error: ", e)
                    list_write(failed_list, barcode, 'tar\t%s' % e)
                    continue
                  
            '''MOVE TAR TO ARCHIVER LOCATION'''
            if not check_list(moved_list, barcode):
                #Just in case we are restarting from an error   
                tar_file = '%s.tar' % os.path.basename(target)
                
                print('\n\tMoving tar file to Archiver folder...')
                
                complete_sip = os.path.join(shipment, tar_file)
                
                #get some stats on SIP
                print('\tCalculating SIP size...')
                SIP_size = get_size(complete_sip)
                
                print('\tCalculating SIP md5 checksum...')
                SIP_md5 = md5(complete_sip)
                
                SIP_dict = {'size' : SIP_size, 'md5' : SIP_md5}
                
                #store values in a file just in case we have a failure or are otherwise interrupted...
                with open(os.path.join(report_dir, 'SIP_%s.txt' % barcode), 'wb') as file:
                    pickle.dump(SIP_dict, file)
                    
                try:
                    shutil.move(complete_sip, destination)
                    list_write(moved_list, barcode)
                    print('\tTar file moved.')
                except (RuntimeError, PermissionError, IOError, EnvironmentError) as e:
                    print("\tUnexpected error: ", e)
                    list_write(failed_list, barcode, 'move\t%s' % e)
                    continue
                        
            '''WRITE STATS TO MASTER SPREADSHEET'''
            if not check_list(metadata_list, barcode):
                
                sep_size = 0
                sep_files = 0
                di_sep = 0
                
                #check to see if files were separated; if so, get stats and adjust previous totals.
                if os.path.exists(separated_list):
                    with open(separated_list, 'r') as fi:
                        fi = csv.reader(fi, delimiter='\t')
                        for line in fi:
                            try:
                                if barcode == line[0]:
                                    sep_files = int(line[1])
                                    sep_size = int(line[2])
                                    di_sep = int(line[3])
                                    break
                            except IndexError as e:
                                print(e)
                                continue
                                    
                #Recalculate size of extracted files
                if row[16].value is None:
                    extracted_size = get_size(os.path.join(target, 'data', 'files')) 
                else:
                    extracted_size = (int(row[16].value) - sep_size)
                    
                #write corrected size back to spreadsheet
                ws.cell(row=row[0].row, column=17, value=extracted_size)
                
                #if there are any files separated, adjust extracted file count
                extracted_no = int(row[17].value) - sep_files
                if extracted_no != row[17].value:
                    ws.cell(row=row[0].row, column=18, value=extracted_no)
                    
                wb.save(spreadsheet)
                
                #Retrieve SIP info (just in case process closed unexpectedly)
                try:
                    SIP_dict
                except NameError:
                    SIP_stats = os.path.join(report_dir, 'SIP_%s.txt' % barcode)
                    SIP_dict = {}
                    with open(SIP_stats, 'rb') as file:
                        SIP_dict = pickle.load(file)
                
                #write information on the specfic barcode
                rowlist = [barcode, unit, shipmentID, str(row[2].value), str(row[3].value), str(row[4].value), str(row[6].value), str(row[7].value), str(row[8].value), str(row[9].value), str(row[10].value), str(row[12].value), str(datetime.datetime.now()), extracted_no, extracted_size, SIP_dict['size'], SIP_dict['md5']]
                
                #append list and save
                item_ws.append(rowlist)   
                master_wb.save(master_spreadsheet)
                
                #add info to cumulative stats for shipment
                shipment_stats = {}
               
                #if we already have shipment stats, retrieve from file and update.  Otherwise, add values to dictionary
                if os.path.exists(stats_doc):
                    with open(stats_doc, 'rb') as file:
                        shipment_stats = pickle.load(file)
                    shipment_stats['sip_count'] += 1
                    shipment_stats['extracted_no'] += extracted_no
                    shipment_stats['extracted_size'] += extracted_size
                    shipment_stats['SIP_size'] += SIP_dict['size']
                else:
                    shipment_stats = {'sip_count' : 1, 'extracted_no' : extracted_no, 'extracted_size' : extracted_size, 'SIP_size' : SIP_dict['size']}
                
                #Write shipment stat back to file
                with open(stats_doc, 'wb') as file:
                    pickle.dump(shipment_stats, file)
                
                list_write(metadata_list, barcode)

                os.remove(os.path.join(report_dir, 'SIP_%s.txt' % barcode))
                
            '''CLEAN ORIGINAL BARCODE FOLDER'''
            #remove original folder
            if not check_list(cleaned_list, barcode):
                print('\n\tRemoving original folder...')
                cmd = 'RD /S /Q "%s"' % target
                try:
                    subprocess.check_output(cmd, shell=True)
                    list_write(cleaned_list, barcode)
                    print('\tFolder removed')
                #note failure, but write metadata to master spreadsheet
                except (PermissionError, subprocess.CalledProcessError, OSError) as e:
                    print("\tUnexpected error: ", e)
                    list_write(failed_list, barcode, 'clean_original\t%s' % e)
                    continue
            
            #barcode is now done!
            print('\n\t%s COMPLETED\n---------------------------------------------------------------' % barcode)
            
            #if barcode had previously failed, remove it from list.
            if os.path.exists(failed_list):
                with open(failed_list, "r") as f:
                    lines = f.read().splitlines()
            
                with open(failed_list, "w") as f:
                    for line in lines:
                        if not barcode in line:
                            f.write('%s\n' % line)
    
        #if other appraisal decision is indicated, note barcode in a list and move folder.
        else:
            if not check_list(other_list, barcode):
                list_write(other_list, barcode, str(row[26].value))
                
                if not os.path.exists('review'):
                    os.mkdir('review')
                
                shutil.move(barcode, 'review')
                
                print('\n\tAlternate appraisal decision: %s. \n\tConfer with collecting unit as needed.' % str(row[26].value))
            
            else:
                print('\n\t%s has been moved to the "Review" folder.' % barcode)
            
    '''CHECK PROCESS FOR MISSING/FAILED ITEMS'''    
    #get lists from status files: how many barcodes are in each list
    list_dict = {'started' : started_list, 'moved' : moved_list, 'deaccessioned' : deaccession_list, 'other' : other_list, 'failed' : failed_list}
    tally_dict = {}
    for key, value in list_dict.items():
        if os.path.isfile(value):
            tally = []
            with open(value, 'r') as v:
                for line in v:
                    try:
                        tally.append(line.split()[0])
                    except IndexError:
                        tally = []
        else:
            tally = []
        tally_dict[key] = tally
    
    #determine if any didn't make it through the process to one of the conclusions (skipped, failed, moved, or other)
    subtotal = tally_dict['moved'] + tally_dict['other'] + tally_dict['failed'] + tally_dict['deaccessioned']
    missing = list(set(tally_dict['started']) - set(subtotal))
    
    print('\n\nSTATS:\n# of barcodes processed:\t\t%s' % len(tally_dict['started']))
    print('# of barcodes moved to Archiver:\t%s' % len(tally_dict['moved']))
    print('# of barcodes to be deaccessioned:\t%s' % len(tally_dict['deaccessioned']))
    print('# of barcodes that failed:\t\t%s' % len(tally_dict['failed']))
    print('# of barcodes that require review:\t%s' % len(tally_dict['other']))
        
    #make sure all items are accounted for
    if len(missing) > 0:
        print("\n# of barcodes not accounted for:\t%s\n\t%s" % (len(missing), '\n\t'.join(missing)))
    
    #give alert if there are barcode folders not listed in spreadsheet
    if len(missing_from_spreadsheet) > 0:
        print('\n\nNOTE: shipment folder included %s barcode folder(s) not listed in spreadsheet.  These have been moved to "unaccounted" folder; review as needed.' % len(missing_from_spreadsheet))
        
    #if any items have failed, quit; only update cumulative information once we are all done.
    if len(tally_dict['failed']) > 0 or len(tally_dict['other']) > 0:
        print('\n***Exiting process. Address remaining barcodes and run again to write shipment information to master spreadsheet.***')
        if len(tally_dict['failed']) > 0:
            cmd = 'notepad %s' % failed_list
            subprocess.check_output(cmd)
        if len(tally_dict['other']) > 0:
            cmd = 'notepad %s' % other_list
            subprocess.check_output(cmd)
        
        sys.exit(1)
        
    
    '''UPDATE CUMULATIVE INFORMATION'''
    print('\n\n------------------------------------------------------------\n\nUPDATING MASTER SPREADSHEET')
    #get info from stats document
    if os.path.exists(stats_doc):
        shipment_stats = {}
        with open(stats_doc, 'rb') as file:
            shipment_stats = pickle.load(file)
            
        #get duration info 
        duration_stats = {}
        with open(duration_doc, 'rb') as file:
            duration_stats = pickle.load(file)
        
        #check to make sure shipment hasn't already been written to worksheet; if so, we will update that information.
        newrow = cumulative_ws.max_row+1

        iterrows = cumulative_ws.iter_rows()
        next(iterrows)
        
        for row in iterrows:    
            if unit in row[0].value and shipmentID in row[1].value:
                newrow = row[0].row
                break
        
        print('\nWriting cumulative information to spreadsheet...')
        
        #write (or update) shipment info in cumulative sheet of master workbook
        cumulative_ws.cell(row=newrow, column=1, value = unit) 
        cumulative_ws.cell(row=newrow, column=2, value = shipmentID)
        cumulative_ws.cell(row=newrow, column=3, value = shipment_stats['sip_count'])
        cumulative_ws.cell(row=newrow, column=4, value = shipment_stats['extracted_no'])
        cumulative_ws.cell(row=newrow, column=5, value = shipment_stats['extracted_size'])
        cumulative_ws.cell(row=newrow, column=6, value = shipment_stats['SIP_size'])
        cumulative_ws.cell(row=newrow, column=7, value = duration_stats['earliest'])
        cumulative_ws.cell(row=newrow, column=8, value = duration_stats['latest'])
        cumulative_ws.cell(row=newrow, column=9, value = duration_stats['duration'])
    else:
        print('\nNOTE: Shipment statistics failed to be recorded.')
        
    #now record this shipment's format information, to be saved in master workbook.
    #first, reecover our puid and format reports
    format_list = []
    if os.path.exists(format_report):
        with open(format_report, 'rb') as f:
            format_list = pickle.load(f)
    
    puid_list = []
    if os.path.exists(puid_report):
        with open(puid_report, 'rb') as f:
            puid_list = pickle.load(f)
    
    #remove any duplicate entries in the puid list, sort, and append 'barcode' as the first item
    puid_list = list(set(puid_list))
    puid_list.sort()
    puid_list.insert(0, 'barcode')
    
    #now create a puid dictionary so that we can refer to columns in our 'formats' worksheet.  Add one to each enumerator value so we align with openpyxl column index
    puid_dict = {}
    for pu, id in enumerate(puid_list):
        puid_dict[id] = pu+1

    format_sheet = 'puids_%s_%s' % (unit, shipmentID)
    if not format_sheet in master_wb.sheetnames:
        fws = master_wb.create_sheet(format_sheet)
        fws.append(puid_list)
    else:
        fws = master_wb[format_sheet]
    
    print('\nWriting format information...')
    
    #loop through the dictionaries of our format report
    for index in range(len(format_list)):
        for key in format_list[index]:
        
            #retrieve the information on puids we separated; this is a dictionary with puid as key and # of files separated as value
            separated_puids = os.path.join(report_dir, '%s_separated_puids.txt' % key)
            sep_puid_count = {}
            if os.path.exists(separated_puids):
                with open(separated_puids, 'rb') as f:
                    sep_puid_count = pickle.load(f)
            
            #set the row variable; write barcode info to first cell
            newrow = fws.max_row+1
            fws.cell(row=newrow, column=puid_dict['barcode'], value=key)
            
            #now loop through of our list of dictionaries with stats on specific formats
            for item in format_list[index][key]:
                
                #see if files of a given puid were separated; if so, reduce the count accordingly
                if len(sep_puid_count) > 0:
                    if item['puid'] in list(sep_puid_count):
                        item['count'] -= sep_puid_count[item['puid']]
                
                #if count is now 0, continue to next puid; otherwise write information to format sheet
                if item['count'] < 1:
                    continue
                else:
                    fws.cell(row=fws.max_row, column=puid_dict[item['puid']], value=item['count'])
        
            #os.remove(separated_puids)
            
    #print format totals for shipment
    max_row = fws.max_row + 1
    
    fws.cell(row=max_row, column=1, value='Totals:')
    
    #loop through sheet and sum each column
    for col in fws.iter_cols(2, fws.max_column, 2, fws.max_row):
        count = 0
        for c in col:
            if not c.value is None:
                count += c.value
                colno = c.column
        fws.cell(row=max_row, column=colno, value=count)
    
    
    
    print('\nPackaging for shipment %s shipment %s completed!!' % (unit, shipmentID))
    
    #save workbooks
    wb.save(spreadsheet)
    master_wb.save(master_spreadsheet)
    

if __name__ == '__main__':
    
    os.system('cls')
    
    #print BDPL screen
    fname = "C:/BDPL/scripts/bdpl.txt"
    if os.path.exists(fname):
        with open(fname, 'r') as fin:
            print(fin.read())
    
    main()