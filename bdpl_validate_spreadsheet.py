import shutil
import os
import openpyxl
from collections import Counter

def main():
    
    #get a list of barcodes in new spreadsheet inventory
    while True:
        spreadsheet = input('\nEnter Python-appropriate path to the spreadsheet: ')
    
        if os.path.exists(spreadsheet):
            break
    
    wb = openpyxl.load_workbook(spreadsheet)
    ws = wb['Inventory']
    
    new_bcs = {}
    for barcode in ws['A'][1:]:
        if not barcode.value is None:
            new_bcs[barcode.value] = barcode.row
    
    new_bcs_list = list(new_bcs.keys())
    
    #check for any duplicate barcodes in new spreadsheet
    duplicate_barcodes = [item for item, count in Counter(new_bcs_list).items() if count > 1]
    
    #make a copy of the master workbook
    master_spreadsheet = 'Y:/spreadsheets/bdpl_master_spreadsheet.xlsx'
    master_copy = os.path.join('C:/temp', 'bdpl_master_copy.xlsx')
    
    if not os.path.exists('C:/temp'):
        os.mkdir('C:/temp')
    
    shutil.copy(master_spreadsheet, master_copy)
    
    #add all current barcodes into a list
    master_wb = openpyxl.load_workbook(master_copy)
    item_ws = master_wb['Item']

    master_list = []
    
    for barcode in item_ws['A'][1:]:
        if not barcode.value is None:
            master_list.append(barcode.value)
    
    already_used = [x for x in new_bcs_list if x in master_list]
    
    if len(duplicate_barcodes) > 0:
        print('\n\nWARNING: spreadsheet includes duplicate barcode values:')
        for dup in duplicate_barcodes:
            print('\t%s\tRow: %s' % (dup, new_bcs[dup]))
            
    if len(already_used) > 0:
        print('\n\nWARNING: spreadsheet includes barcodes that have already been deposited to the SDA:')
        for dup in already_used:
            print('\t%s\tRow: %s' % (dup, new_bcs[dup]))

if __name__ == '__main__':

    os.system('cls')
    
    #print BDPL screen
    fname = "C:/BDPL/scripts/bdpl.txt"
    if os.path.exists(fname):
        with open(fname, 'r') as fin:
            print(fin.read())

    main()