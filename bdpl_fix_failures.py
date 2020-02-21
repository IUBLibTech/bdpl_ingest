import bagit
import os
import shutil
import sys
import subprocess
import pickle

def failed_status(failed_list, barcode, error=None):
    if os.path.exists(failed_list):
        with open(failed_list, "r") as f:
            lines = f.read().splitlines()
       
        with open(failed_list, "w") as f:
            for line in lines:
                if barcode in line:
                    if not error is None:
                        f.write('%s\tbagit\terror\n' % barcode)
                else:
                    f.write('%s\n' % line)

def list_write(list_name, barcode, message=None):
    with open(list_name, 'a') as current_list:
        if message is None:
            current_list.write('%s\n' % barcode)
        else:
            current_list.write('%s\t%s\n' % (barcode, message))
                    
def main():

    while True:

        shipment = input('\nFull path to shipment folder: ')
        shipment = shipment.replace('"', '').rstrip()
        
        if not os.path.exists(shipment):
            print('\nShipment folder not recognized; enclose in quotes and use "/".\n')
            continue
        
        bag_reports = os.path.join(shipment, 'bag_reports')
        
        failed_list = os.path.join(bag_reports, 'failed-packaging.txt')
        bagged_list = os.path.join(bag_reports, 'bagged.txt')
        deaccession_list = os.path.join(bag_reports, 'deaccession.txt')
        cleaned_list = os.path.join(bag_reports, 'cleaned.txt')
        
        if os.path.exists(failed_list):
            break
        else:
            print('\n"Failed packaging" list not found.  Exiting script...')
            sys.exit(1)
            
    with open(failed_list, 'r') as f:
        for line in f.read().splitlines():
            barcode = line.split()[0]
            barcode_path = os.path.join(shipment, barcode)
            
            print('\nWorking on %s\n\n\tIssue: %s' % (barcode, line.split()[1]))
                        
            #make sure we're dealing with a bagit issue
            if line.split()[1] == 'bagit':            
            
                bag_info = os.path.join(bag_reports, '%s-bag-info.txt' % barcode)
                
                info = {}
                if os.path.exists(bag_info):
                    with open(bag_info, 'rb') as f:
                        info = pickle.load(f)
                    unit = info['unit']
                    desc = info['description']
                    desc = desc.replace('\n', ' ')
                    
                
                else:
                    desc = ''
                    unit = ''
                
                contents = os.listdir(barcode_path)
                
                #look to see if there is a temp folder created by tempfile.mkdtemp
                tempdir = [d for d in os.listdir(barcode_path) if d[:3] == 'tmp' and len(d) == 11]
                
                #if no 'tmp...' folder exists, check to see if a 'data' folder was created
                if not tempdir:
                    tempdir = [d for d in os.listdir(barcode_path) if d == 'data']
                
                #if there is such a folder, we need to move anything in it  back to the main barcode folder
                if tempdir:
                
                    bad_bag = os.path.join(barcode_path, tempdir[0])
                    
                    #move contents back to main barcode folder
                    for dir in os.listdir(bad_bag):
                        target = os.path.join(bad_bag, dir)
                        try:
                            shutil.move(target, barcode_path)
                        except (shutil.Error, OSError, IOError, PermissionError) as e:
                            print('\n\tFailed to move %s: %s' % (target, e))
                            break
                    
                    #delete bad bag 
                    try:
                        os.rmdir(bad_bag)
                    except OSError as e:
                        print('\n\Failed to delete temp folder: %s' % e)                
                    
                try:
                    bagit.make_bag(barcode_path, {"Source-Organization" : unit, "External-Description" : desc, "External-Identifier" : barcode}, checksums=["md5"])
                    failed_status(failed_list, barcode)
                    list_write(bagged_list, barcode)
                    os.remove(bag_info)
                    
                except (RuntimeError, PermissionError, bagit.BagError, OSError) as e:
                    failed_status(failed_list, barcode, e)
                    print('\n\t%s failed: %s' % (barcode, e))
            
            elif line.split()[1] == 'deaccession':
                if not os.path.exists(barcode_path):
                    failed_status(failed_list, barcode)
                    list_write(deaccession_list, barcode)
                else:
                    print('\n\tRemoving stubborn folder...')
                    if '[WinError 145] The directory is not empty' in line.split('\t')[2]:
                        cmd = 'RD /S /Q "%s"' % barcode_path
                        try:
                            subprocess.call(cmd, shell=True)
                            failed_status(failed_list, barcode)
                            list_write(deaccession_list, barcode)
                        except subprocess.CalledProcessError as e:
                            print('\n\tFailed to remove folder...')
            
            elif line.split()[1] == 'clean_original':
                if not os.path.exists(barcode_path):
                    failed_status(failed_list, barcode)
                    list_write(cleaned_list, barcode)
            
            elif line.split()[1] == 'tar':
                tar_file = os.path.join(shipment, '%s.tar' % barcode)
                if os.path.exists(tar_file):
                    os.remove(tar_file)
                    
                
if __name__ == '__main__':
    main()
            
            
                
            
            