import subprocess
import os
import shutil
import bagit
import sys
import openpyxl

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

def main():

    #Identify where files will be moved ###### UPDATE TO ARCHIVER location #######
    destination = "C:\\BDPL\\Archiver"
    
    #need to identify the shipment folder; check to make sure it exists
    while True:
        spreadsheet = input('Full path to shipment spreadsheet: ')
        spreadsheet = spreadsheet.replace('"', '').rstrip()

        if os.path.exists(spreadsheet):
            break
        else:
            print('Spreadsheet path not recognized; enclose in quotes and use "/".\n')

    #open workbook
    wb = openpyxl.load_workbook(spreadsheet)
    ws = wb['Appraisal']
    
    #make sure column headings are correct; exit if they don't match targets
    if ['Appraisal results', 'Source type', 'Label transcription', 'Initial appraisal notes'] != [ws['AA1'].value, ws['G1'].value, ws['H1'].value, ws['I1'].value]:
        print('\n\nERROR: SPREADSHEET COLUMNS ARE NOT IN CORRECT ORDER')
        print('Current headings:\n -AA1 (Appraisal results) = %s\n - G1 (Source type) = %s\n - H1 (Label transcription) = %s\n - I1 (Initial appraisal notes) = %s' % (ws['AA1'].value, ws['G1'].value, ws['H1'].value, ws['I1'].value))
        sys.exit(1)

    #get unit name
    while True:
        unit = input('Unit name: ')
        check = input('Is name entered correctly? (y/n) ')
        if check.lower().strip() == 'y':
            break
        else:
            continue
    
    #get shipment direcetory from spreadsheet
    shipment = os.path.dirname(spreadsheet)

    #set shipment directory as current working directory
    os.chdir(shipment)
    
    
    #determine if directory includes folders not on spreadsheet and/or if spreadsheet has barcodes not in directory
    spreadsheet_list = []
    for col in ws['A']:
        if col.value == 'Identifier':
            continue
        else:
            spreadsheet_list.append(col.value)
    
    dir_list = next(os.walk(shipment))[1]

    missing_from_dir = list(set(spreadsheet_list) - set(dir_list))
    missing_from_spreadsheet = list(set(spreadsheet_list) - set(dir_list))
    
    #save the 'missing' lists to files, for future reference
    unaccounted_folders = os.path.join(shipment, 'unaccounted.txt') #for barcodes that are in directory, but not in spreadsheet; need to check for data entry errors
    with open(unaccounted_folders, 'w') as f:
        f.write('\n'.join(missing_from_spreadsheet))
    
    #create lists to track work (NOTE: there is probably a better way to do this...)
    started_list = os.path.join(shipment, 'started.txt') #to document all the barcodes that we started to package/process; all should be accounted for in the fail/complete lists
    deaccession_list = os.path.join(shipment, 'deaccession.txt')    #for barcodes/items that will not be sent to SDA
    failed_list = os.path.join(shipment, 'failed-packaging.txt') #for any failures; will note which stage the failure occurred 
    bagged_list = os.path.join(shipment, 'bagged.txt') #for barcodes that were successfully bagged
    tarred_list = os.path.join(shipment, 'tarred.txt') #for barcodes that were successfully tarred
    cleaned_list = os.path.join(shipment, 'cleaned.txt') #for barcode folders that were successfully cleaned
    other_list = os.path.join(shipment, 'other-decision.txt') #for barcode folders have an alternate appraisal decision
    completed_list = os.path.join(shipment, 'completed.txt') #for barcodes that reached the end of the process; should include any that were deaccessioned

    #loop through spreadsheet, skipping header row
    iterrows = ws.iter_rows()
    next(iterrows)
    
    for row in iterrows:

        barcode = str(row[0].value)
        
        #skip to next barcode if current one does not have a folder in the shipment direcetory
        if barcode in missing_from_dir:
            continue
        
        #skip to next barcode if current one has already finished workflow
        if check_list(completed_list, barcode):
            continue
            
        #document that we've started working on this barcode
        if not check_list(started_list, barcode):
            list_write(started_list, barcode)
        
        print('\nWorking on item: %s' % barcode)

        #description for bag-info.txt currently includes source media, label transcription, and appraisal note.
        desc = '%s. %s. %s' %(str(row[6].value), str(row[7].value), str(row[8].value))
        desc.replace('\n', ' ')        
        
        #if content will not be moved to SDA, just skip folder for now and write to skipped and completed lists
        if str(row[26].value) == "Delete content":
            list_write(deaccession_list, barcode)
            print('\n\tContent will not be transferred to SDA.  Continuing with next item.')
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
                    print('\tCheck complete.')
                
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
        
            '''CLEAN ORIGINAL BARCODE FOLDER'''
            #remove original folder
            if not check_list(cleaned_list, barcode):
                print('\n\tRemoving original folder...')
                try:
                    shutil.rmtree(target)
                    list_write(cleaned_list, barcode)
                    print('\tFolder removed')
                except PermissionError as e:
                    print("\tUnexpected error: ", e)
                    list_write(failed_list, barcode, 'clean_original\t%s' % e)
                    continue
            
            '''MOVE TAR TO ARCHIVER LOCATION'''
            print('\n\tMoving tar file to Archiver folder...')
            
            complete_sip = os.path.join(shipment, tar_file)
            
            try:
                shutil.move(complete_sip, destination)
                list_write(completed_list, barcode)
                print('\tTar file moved.')
            except (RuntimeError, PermissionError, IOError, EnvironmentError) as e:
                print("\tUnexpected error: ", e)
                list_write(failed_list, barcode, 'move\t%s' % e)
                continue
            print('\n\t%s COMPLETED.' % barcode)
        
        else:
            list_write(other_list, barcode, str(row[26].value))
            print('\n\tAlternate appraisal decision: %s. \n\tConfer with collecting unit as needed.' % str(row[26].value))
            
    '''CHECK PROCESS FOR MISSING/FAILED ITEMS'''    
    #get lists from status files
    list_dict = {'started' : started_list, 'completed' : completed_list, 'deaccessioned' : deaccession_list, 'other' : other_list, 'failed' : failed_list}
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
    
    #determine if any didn't make it through the process to one of the conclusions (skipped, failed, completed, or other)
    subtotal = tally_dict['completed'] + tally_dict['other'] + tally_dict['failed'] + tally_dict['deaccessioned']
    missing = list(set(tally_dict['started']) - set(subtotal))
    
    print('\n\nSTATS:\n# of barcodes processed:\t\t%s' % len(tally_dict['started']))
    print('# of barcodes moved to Archiver:\t%s' % len(tally_dict['completed']))
    print('# of barcodes to be deaccessioned:\t%s' % len(tally_dict['deaccessioned']))
    print('# of barcodes that failed:\t\t%s' % len(tally_dict['failed']))
    print('# of barcodes that require review:\t%s' % len(tally_dict['other']))
        
    #make sure all items are accounted for
    if len(missing) > 0:
        print("\n# of barcodes not accounted for:\t%s\n\t%s" % (len(missing), '\n\t'.join(missing)))
    
    #give alert if there are barcode folders not listed in spreadsheet
    if len(missing_from_spreadsheet) > 0:
        print('\n\nNOTE: shipment folder includes %s barcode folder(s) not listed in spreadsheet.  Review as needed.' % len(missing_from_spreadsheet))
        

if __name__ == '__main__':
    main()