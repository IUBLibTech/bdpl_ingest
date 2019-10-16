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
    
    failed_ingest = os.path.join(ship_dir, 'failed_ingest.txt')
    replicated = os.path.join(ship_dir, 'replicated.txt')
    analyzed = os.path.join(ship_dir, 'analyzed.txt')
    userdata = os.path.join(ship_dir, 'userdata.txt')
    
    missing = [x for x in [failed_ingest, replicated, analyzed, userdata] if not os.path.exists(x)]
    if len(missing) > 0:
        print('\nWARNING: the following file(s) are missing from %s:\n' % ship_dir)
        print('\t%s' % '\n\t'.join(missing))
        print('\nRecover files and run bdpl_ripstation_postprocess again.')
        sys.exit(1)
    
    newscreen()
    
    #scan in barcode and check to see if folder exists
    while True:
        item_barcode = input('\nEnter barcode (or "q" to quit): ').strip()
        
        if item_barcode.lower() == 'q':
            break
        else:
            target = os.path.join(ship_dir, item_barcode)
        
        #if barcode folder exists in shipment, check success/failure:
        if os.path.exists(target):
            
            success = True
            
            newscreen()
            
            print('\n----------------------------------------------------------\n\nReviewing %s' % item_barcode)
            
            present = False
            with open(userdata, 'r') as f:
                for item in f.read().splitlines():
                    if item_barcode in item:
                        present = True
                        break
            
            with open(replicated, 'r') as f:
                for item in f.read().splitlines():
                    if item_barcode in item:
                        print('\n\tStep 1: Replication completed.')
                        break
            
            with open(analyzed, 'r') as f:
                for item in f.read().splitlines():
                    if item_barcode in item:   
                        print('\n\tStep 2: Analysis completed.')
                        break
                        
            with open(failed_ingest, 'r') as f:
                for item in f.read().splitlines():
                    if item_barcode in item:
                        print('\n\tWARNING: %s' % item.split('\t')[1])
                        success = False
            if not success:
                print('\n\t***Determine if additional attempt to recover and analyse data is required***')
            
            if not present:
                print('\nThis item was not included in the RipStation batch operation.')
            
        else:
            print('\n%s does not exist.  Please enter a new barcode.' % target)
            continue


if __name__ == '__main__':
    main()