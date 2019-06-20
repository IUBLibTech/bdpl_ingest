import subprocess
import os
import shutil
import bagit
import sys
import openpyxl
import hashlib
import datetime
import pickle

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

def main():
    
    '''SET VARIABLES'''
    #Identify where files will be moved ###### UPDATE TO ARCHIVER location #######
    destination = "C:\\BDPL\\Archiver"
    
    #open master workbook and get ready to write
    master_spreadsheet = 'C:\\BDPL\\bdpl_master_spreadsheet.xlsx'
    master_wb = openpyxl.load_workbook(master_spreadsheet)
    item_ws = master_wb['Item']
    cumulative_ws = master_wb['Cumulative']
    
    #get unit name
    while True:
        unit = input('\nUnit name: ')
        check = input('\nIs name entered correctly? (y/n) ')
        if check.lower().strip() == 'y':
            break
        else:
            continue

    #need to identify the shipment folder; check to make sure it exists
    while True:
        spreadsheet = input('\nFull path to shipment spreadsheet: ')
        spreadsheet = spreadsheet.replace('"', '').rstrip()

        if os.path.exists(spreadsheet):
            break
        else:
            print('Spreadsheet path not recognized; enclose in quotes and use "/".\n')

    #open shipment workbook
    wb = openpyxl.load_workbook(spreadsheet)
    ws = wb['Appraisal']

    #get shipment direcetory from spreadsheet
    shipment = os.path.dirname(spreadsheet)
    shipmentID = os.path.basename(os.path.dirname(shipment))    

    #set shipment directory as current working directory
    os.chdir(shipment) 
    
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
    unaccounted_list = os.path.join(report_dir, 'unaccounted.txt') #for barcodes that are in directory, but not in spreadsheet; need to check for data entry errors
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
        if not os.path.isdir(report_dir):
            os.mkdir(report_dir)
        
        #write duration and 'missing' stats to file
        with open(duration_doc, 'wb') as file:
            pickle.dump(duration_stats, file)
        with open(missing_doc, 'wb') as file:
            pickle.dump((missing_from_dir, missing_from_spreadsheet), file)
            
        #If we have unaccounted barcodes; save list to file and move the dirs themselves to an 'unaccounted' folder
        if len(missing_from_spreadsheet) > 0:
            
            if not os.path.isdir('unaccounted'):
                os.mkdir('unaccounted')   
                
            with open(unaccounted_list, 'w') as f:
                for item in missing_from_spreadsheet:
                    shutil.move(item, 'unaccounted')
                    f.write(item)
    
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
        
        #skip to next barcode if current one has already finished workflow
        if check_list(cleaned_list, barcode):
            continue
            
        #document that we've started working on this barcode
        if not check_list(started_list, barcode):
            list_write(started_list, barcode)
        
        print('\nWorking on item: %s' % barcode)    
        
        #if content will not be moved to SDA, just skip folder for now and write to skipped and moved lists
        if str(row[26].value) == "Delete content":
            if not check_list(deaccession_list, barcode):
                
                if not os.path.isdir('deaccessioned'):
                    os.mkdir('deaccessioned')
                
                shutil.move(barcode, 'deaccessioned')
                
                print('\n\tContent will not be transferred to SDA.  Continuing with next item.')
                
                list_write(deaccession_list, barcode)
                
                continue
        
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
            
            '''CLEAN FOLDERS'''
            #remove bulk_extractor folder, if present, as well as reports used solely for appraisal/review
            print('\tRemoving appraisal and review files...')
            
            for dir in ['bulk_extractor', 'temp']:
                remove_dir = os.path.join(target, dir)
                if os.path.exists(remove_dir):
                    shutil.rmtree(remove_dir)
         
            for f in ["duplicates.csv", "errors.csv", "formats.csv", "formatVersions.csv", "mimetypes.csv", "unidentified.csv", "uniqueyears.csv", "years.csv",]:
                report = os.path.join(target, 'metadata', 'reports', f)
                if os.path.exists(report):
                    os.remove(report)
            
            '''BAG FOLDER'''
            #make sure we haven't already bagged folder
            if not check_list(bagged_list, barcode):
            
                #description for bag-info.txt currently includes source media, label transcription, and appraisal note.
                desc = '%s. %s. %s' %(str(row[6].value), str(row[7].value), str(row[8].value))
                desc.replace('\n', ' ')    
        
                try:
                    print('\n\tBagging folder...')
                    bagit.make_bag(target, {"Source-Organization" : unit, "External-Description" : desc, "External-Identifier" : barcode}, checksums=["md5"])
                    list_write(bagged_list, barcode)
                    print('\tBagging complete.')
                except (RuntimeError, PermissionError, IOError, EnvironmentError) as e:
                    print("\nUnexpected error: ", e)
                    list_write(failed_list, barcode, 'bagit\t%s' % e)
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
                except (RuntimeError, PermissionError, IOError, EnvironmentError) as e:
                    print("\tUnexpected error: ", e)
                    list_write(failed_list, barcode, 'tar\t%s' % e)
                    continue
                  
            '''MOVE TAR TO ARCHIVER LOCATION'''
            if not check_list(moved_list, barcode):
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
                print('\n\t%s MOVED.' % barcode)
                        
            '''WRITE STATS TO MASTER SPREADSHEET'''
            if not check_list(metadata_list, barcode):
                
                #If size of extracted files was not recorded, calculate extracted_size
                if row[16].value is None:
                    extracted_size = get_size(os.path.join(target, 'data', 'files')) 
                    ws.cell(row=row[0].row, column=17, value=extracted_size)
                else:
                    extracted_size = int(row[16].value)
                
                #in case previous step failed, may need to retrieve SIP info
                try:
                    SIP_dict
                except NameError:
                    SIP_dict = {}
                    with open(os.path.join(report_dir, 'SIP_%s.txt' % barcode), 'rb') as file:
                        SIP_dict = pickle.load(file)
                    
                rowlist = [barcode, unit, shipmentID, str(row[2].value), str(row[3].value), str(row[4].value), str(row[6].value), str(row[7].value), str(row[8].value), str(row[9].value), str(row[10].value), str(row[12].value), str(datetime.datetime.now()), int(row[17].value), extracted_size, SIP_dict['size'], SIP_dict['md5']]
                
                item_ws.append(rowlist)        
                
                #add info to stats for shipment
                shipment_stats = {}
               
                #if we already have shipment stats, retrieve from file and update.  Otherwise, create dictionary values
                if os.path.exists(stats_doc):
                    with open(stats_doc, 'rb') as file:
                        shipment_stats = pickle.load(file)
                    shipment_stats['sip_count'] += 1
                    shipment_stats['extracted_no'] += int(row[17].value)
                    shipment_stats['extracted_size'] += extracted_size
                    shipment_stats['SIP_size'] += SIP_dict['size']
                else:
                    shipment_stats = {'sip_count' : 1, 'extracted_no' : int(row[17].value), 'extracted_size' : extracted_size, 'SIP_size' : SIP_dict['size']}
                
                #Write shipment info back to file
                with open(stats_doc, 'wb') as file:
                    pickle.dump(shipment_stats, file)
                
                list_write(metadata_list, barcode)
                
            '''CLEAN ORIGINAL BARCODE FOLDER'''
            #remove original folder
            if not check_list(cleaned_list, barcode):
                print('\n\tRemoving original folder...')
                try:
                    shutil.rmtree(target)
                    list_write(cleaned_list, barcode)
                    print('\tFolder removed')
                #note failure, but write metadata to master spreadsheet
                except PermissionError as e:
                    print("\tUnexpected error: ", e)
                    list_write(failed_list, barcode, 'clean_original\t%s' % e)
                    continue
            
            #barcode is now done!
            print('\n\t%s COMPLETED\n---------------------------------------------------------------' % barcode)
            
            #if barcode had previously failed, remove it from list.
            if os.path.exists(failed_list):
                with open(failed_list, "r") as f:
                    lines = f.read().splitlines()
                if barcode in lines:
                    with open(failed_list, "w") as f:
                        for line in lines:
                            if line != barcode:
                                f.write('%s\n' % line)
        
        else:
            #if other appraisal decisions are indicated, note barcode in a list and move folder.
            list_write(other_list, barcode, str(row[26].value))
            
            if not os.path.isdir('review'):
                os.mkdir('review')
            
            shutil.move(barcode, 'review')
            
            print('\n\tAlternate appraisal decision: %s. \n\tConfer with collecting unit as needed.' % str(row[26].value))
            
    '''CHECK PROCESS FOR MISSING/FAILED ITEMS'''    
    #get lists from status files: how many barcodes are in each list
    list_dict = {'started' : started_list, 'moved' : moved_list, 'deaccessioned' : deaccession_list, 'other' : other_list, 'failed' : failed_list}
    tally_dict = {}
    for key, value in list_dict.items():
        if os.path.isfile(value):
            tally = []
            with open(value, 'r') as v:
                for line in v:
                    tally.append(line.split()[0])
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
    
    '''UPDATE CUMULATIVE INFORMATION'''
    #get info from stats document
    if os.path.exists(stats_doc):
        shipment_stats = {}
        with open(stats_doc, 'rb') as file:
            shipment_stats = pickle.load(file)
            
        #get duration info 
        duration_stats = {}
        with open(duration_doc, 'rb') as file:
            duration_stats = pickle.load(file)
        
        #check to make sure shipment hasn't already been written to worksheet
        iterrows = cumulative_ws.iter_rows()
        next(iterrows)
        
        for cell in iterrows:    
            if unit in cell[0].value and shipmentID in cell[1].value:
                newrow = cell[0].row
                break
            else:
                newrow = cumulative_ws.max_row+1
        
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