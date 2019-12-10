import os
import pyperclip
import sys
from bdpl_ingest import newscreen

def main():
    newscreen()
    
    #get unit abbreviation and shipment date
    while True:
        unit_name = input('\nEnter unit abbreviation: ')
        
        shipmentDate = input('\nEnter shipment date: ')
        
        ship_dir = os.path.join('Z:\\', unit_name, 'ingest', shipmentDate)
        
        if os.path.exists(ship_dir):
            break
        else:
            print('\n\tWARNING: %s does not appear to exist... Please re-enter information.' % ship_dir)
            continue
    
    failed_ingest = os.path.join(ship_dir, 'failed_ingest_ripstation.txt')
    replicated = os.path.join(ship_dir, 'replicated_ripstation.txt')
    analyzed = os.path.join(ship_dir, 'analyzed_ripstation.txt')
    userdata = os.path.join(ship_dir, 'userdata.txt')
    
    missing = [x for x in [replicated, analyzed, userdata] if not os.path.exists(x)]
    if len(missing) > 0:
        print('\nWARNING: the following file(s) are missing from %s:\n' % ship_dir)
        print('\t%s' % '\n\t'.join(missing))
        print('\nRecover files and run bdpl_ripstation_postprocess again.')
        sys.exit(1)
        
    with open(replicated, 'r') as f:
        replicated_list = f.read().splitlines()
    
    with open(analyzed, 'r') as f:
        analyzed_list = f.read().splitlines()
    
    if os.path.exists(failed_ingest):
        with open(failed_ingest, 'r') as f:
            failed_list = f.read().splitlines()
    else:
        failed_list = []
        
    with open(userdata, 'r') as f:
        data_list = f.read().splitlines()
    
    newscreen()
    
    #scan in barcode and check to see if folder exists
    while True:
        item_barcode = input('\nEnter barcode (or "q" to quit): ').strip()
        
        if item_barcode.lower() == 'q':
            break
        else:
            target = os.path.join(ship_dir, item_barcode)
        
        newscreen()
        
        #if barcode folder exists in shipment, check success/failure:
        if os.path.exists(target):
            
            success = True
            
            print('\n----------------------------------------------------------\n\nReviewing %s' % item_barcode)
            
            #make sure barcode was in our original userdata.txt file
            present = False
            for item in data_list:
                if item_barcode in item:
                    present = True
                    break
            
            for item in replicated_list:
                if item_barcode in item:
                    print('\n\tStep 1: Replication completed.')
                    break
            
            for item in analyzed_list:
                if item_barcode in item:   
                    print('\n\tStep 2: Analysis completed.')
                    break
                    
            for item in failed_list:
                if item_barcode in item:
                    print('\n\t*****WARNING: %s*****' % item.split('\t')[1])
                    success = False
            
            if not success:
                print('\n\t***Determine if additional attempt to recover and analyse data is required***')
            
            if not present:
                print('\nThis item was not included in the RipStation batch operation.')
            
        else:
            print('\nFolder %s was not created.  Determine if barcode was missing or if additional attempt to recover data is necessary.' % target)
            continue
        
        pyperclip.copy(item_barcode)


if __name__ == '__main__':
    main()