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
    
    #ID list of identifiers; this could be simplified by just getting a link to the spreadsheet and using it's parent folder.
    fi_source = 'C:\\temp\\list.txt'

    #Identify where files will be moved
    destination = "C:\\BDPL\\Archiver"
    
    #need to identify the shipment folder; check to make sure it exists
    while True:
        spreadsheet = input('Full path to shipment spreadsheet: ')
        spreadsheet = spreadsheet.replace('"', '')
        if os.path.exists(spreadsheet):
            break
        else:
            print('Spreadsheet path not recognized; enclose in quotes and use "/".\n')

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
    
    #create lists to track work (NOTE: probably a better way to do this...)
    start_list = os.path.join(shipment, 'started.txt') #to document all the barcodes that we started to package/process; all should be accounted for in the fail/complete lists
    skipped_list = os.path.join(shipment, 'skipped.txt')    #for barcodes/items that will not be sent to SDA
    fail_list = os.path.join(shipment, 'failed-packaging.txt') #for any failures; will note which stage the failure occurred 
    bagged_list = os.path.join(shipment, 'bagged.txt') #for barcodes that were successfully bagged
    tarred_list = os.path.join(shipment, 'tarred.txt') #for barcodes that were successfully tarred
    clean_list = os.path.join(shipment, 'cleaned.txt') #for barcode folders that were successfully cleaned
    complete_list = os.path.join(shipment, 'completed.txt') #for barcodes that reached the end of the process; should include any that were skipped

    #open workbook
    # wb = openpyxl.load_workbook(spreadsheet)
    # ws = wb['Appraisal']

    with open(fi_source, 'r') as item_list:
        for barcode in item_list:
            
            barcode = barcode.strip()
            
            #skip to next barcode if current one has already finished workflow
            if check_list(complete_list, barcode):
                continue
                
            #document that we've started working on this barcode
            if not check_list(start_list, barcode):
                list_write(start_list, barcode)
            
            print('\nWorking on item: %s' % barcode)
            
            '''
            IDEALLY, WE SHOULD CHECK APPRAISAL COLUMN IN SPREADSHEET TO SEE IF CONTENT SHOULD GO TO SDA
            
            Do something like:
            
            if appraisal == 'no':
                os.remiove(folder)
            
            '''
            
            #get full path to barcode folder
            target = os.path.join(shipment, barcode)
            
            #parse spreadsheet to get info; CHANGE THIS DEFAULT
            desc = 'Default description'
            
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
                    list_write(fail_list, barcode, 'bagit\t%s' % e)
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
                    list_write(fail_list, barcode, 'Insufficient space to create tar archive; Need minium of %s bytes' % (free, string(dir_size)))
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
                    list_write(fail_list, barcode, 'tar\t%s' % e)
                    continue
        
            '''CLEAN ORIGINAL BARCODE FOLDER'''
            #remove original folder
            if not check_list(clean_list, barcode):
                print('\n\tRemoving original folder...')
                try:
                    shutil.rmtree(target)
                    list_write(clean_list, barcode)
                    print('\tFolder removed')
                except PermissionError as e:
                    print("\tUnexpected error: ", e)
                    list_write(fail_list, barcode, 'clean_original\t%s' % e)
                    continue
            
            '''MOVE TAR TO ARCHIVER LOCATION'''
            print('\n\tMoving tar file to Archiver folder...')
            
            complete_sip = os.path.join(shipment, tar_file)
            
            try:
                shutil.move(complete_sip, destination)
                list_write(complete_list, barcode)
                print('\tTar file moved.')
            except (RuntimeError, PermissionError, IOError, EnvironmentError) as e:
                print("\tUnexpected error: ", e)
                list_write(fail_list, barcode, 'move\t%s' % e)
                continue
            print('\n\t%s COMPLETED.' % barcode)
            
    #now that we've finished looping through list, check to see if we have any failures; if so stop and notify user
    if os.path.isfile(fail_list):
        with open(fail_list, 'r') as fi:
            failures = fi.readlines()
            if len(failures) > 0:
                print("\n\nATTENTION: %s item(s) failed packaging; see list below:\n\n%s" % (len(failures), '\n'.join(failures)))
                sys.exit(1)
    else:
        print('\n\nPackaging for shipment %s is complete.' % os.path.basename(shipment))
        

if __name__ == '__main__':
    main()