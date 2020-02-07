from argparse import ArgumentParser, RawTextHelpFormatter
import os
import openpyxl
import datetime
from bdpl_ingest import *
import pickle
import shutil
from lxml import etree
import glob

def calc_end_time(timestamp):
    
    min, sec = timestamp.split(':')
    
    if sec == '00':
        sec = 59
        min = int(min) - 1
    else:
        sec = int(sec) - 1
    
    return "{}:{}".format(str(min).zfill(2), str(sec).zfill(2))

def fix_cue(folder, cue_file):
    
    print('\tFixing {}...'.format(cue_file))
    
    #get audio file
    if 'disk-image' in folder:
        audio = '{}.bin'.format(os.path.basename(os.path.splitext(cue_file)[0]))
        type = 'BINARY'
    else: 
        audio = '{}.wav'.format(os.path.basename(os.path.splitext(cue_file)[0]))
        type = 'WAVE'
    
    #get info from cue file
    with open(cue_file, 'rb') as infile:
        cue_info = infile.readlines()[1:]
    
    #write correct 1st line
    with open(cue_file, 'w') as outfile:
        if type == 'BINARY':
            outfile.write('FILE "{}" BINARY\n'.format(audio))
        else:
            outfile.write('FILE "{}" WAVE\n'.format(audio))
    
    #append rest of cue info
    with open(cue_file, 'ab') as outfile:
        for line in cue_info:
            outfile.write(line) 

def main():
    #clear screen
    newscreen()
    
    #set up arg parser so user is required to add shipment directory
    parser = ArgumentParser(
        description='This script prepares specified content for deposit to Media Collections Online from the IUL BDPL.',
        formatter_class=RawTextHelpFormatter
    )

    parser.add_argument(
        'unit_name', 
        help='Unit abbreviation',
    )

    parser.add_argument(
        'shipmentDate', 
        help='Shipment date',
    )
    
    parser.add_argument(
        'mco_dropbox', 
        help='Path to MCO dropbox',
    )
    
    args = vars(parser.parse_args())

    #make sure valid unit name and shipment date were entered
    if not all (k in args for k in ('unit_name', 'shipmentDate', 'mco_dropbox')):
        parser.error('\n\nScript requires a valid unit abbreviation, associated shipment date, and MCO dropbox location.')
    else:
        unit_name = args['unit_name']
        shipmentDate = args['shipmentDate']
        mco_dropbox = args['mco_dropbox']
        
        folders = bdpl_folders(unit_name, shipmentDate)
        ship_dir = folders['ship_dir']
        
        if not os.path.exists(folders['unit_home']):
            print('\n')
            parser.error('\n\nScript requires a valid unit abbreviation; no directory exists for unit {}.'.format(folders['unit_home']))
        if not os.path.exists(ship_dir):
            print('\n')
            parser.error('\n\nScript requires a valid shipment date for unit {}; shipment "{}" does not exist.'.format(unit_name, shipmentDate))
        if not os.path.exists(mco_dropbox):
            print('\n')
            parser.error('\n\nScript requires a valid path to an MCO dropbox location; {} does not exist.'.format(mco_dropbox))
    
    #set up destination for files in dropbox
    mco_assets = os.path.join(mco_dropbox, 'assets')
    if not os.path.exists(mco_assets):
        os.makedirs(mco_assets)
    
    #get shipment spreadsheet and open with openpyxl
    spreadsheet = find_spreadsheet(folders, unit_name, shipmentDate)
    wb = openpyxl.load_workbook(spreadsheet)
    app_ws = wb['Appraisal']
    
    #set up a log file to record completed items
    status_log = os.path.join(ship_dir, 'mco_prep_complete.txt')
    
    #set up MCO deposit spreadsheet
    mco_spreadsheet = os.path.join(ship_dir, '{}_{}_MCO-deposit.xlsx'.format(unit_name, shipmentDate))
    
    #If MCO spreadsheet doesn't exist, add header rows
    if not os.path.exists(mco_spreadsheet):
        mco_wb = openpyxl.Workbook()
        mco_ws = mco_wb.active
        
        deposit_date = datetime.datetime.today().strftime('%Y-%m-%d')
        
        reference_info = ['BDPL deposit to MCO: {}'.format(deposit_date), 'micshall@iu.edu']
        mco_ws.append(reference_info)
        
        mco_header = ['Other Identifier', 'Other Identifier Type', 'Other Identifier', 'Other Identifier Type', 'Other Identifier', 'Other Identifier Type', 'Title', 'Creator', 'Date Issued', 'Abstract', 'Physical Description', 'Publish', 'File', 'Label']
        mco_ws.append(mco_header)
        
        mco_wb.save(mco_spreadsheet)
    
    else:
        mco_wb = openpyxl.load_workbook(mco_spreadsheet)
        mco_ws = mco_wb['Sheet']
        
    '''Give user opportunity to add other file extensions'''
    format_lists = os.path.join(ship_dir, 'mco_formats.txt')
    if os.path.exists(format_lists):
        with open(format_lists, 'rb') as f:
            audio_formats, video_formats = pickle.load(f)
    else:
        audio_formats = ['.wav']
        video_formats = ['.mpg', '.mkv']
    
    print('\nNOTE: this script currently assumes that all content is from optical media.  Need to adjust "PhysicalDescription" variable if other content sources are used.')
    
    print('\nThis script will look for files with the following extensions:\n\tAudio: {}\n\tVideo: {}'.format(', '.join(audio_formats), ', '.join(video_formats)))
    
    print('\nAdd additional file extensions?\n\tA: add AUDIO formats\n\tV: add VIDEO formats\n\tC: skip adding formats and CONTINUE')
    
    while True:
        add_formats = input('\nEnter selection (A, V, or C): ')
        
        if add_formats.upper() == 'C':
            break
            
        elif add_formats.upper() == 'A':
            new_audio = input('Enter exact audio file format extension (with "."): ')
            audio_formats.append(new_audio.lower())
            
        elif add_formats.upper() == 'V':
            new_video = input('Enter exact video file format extension (with "."): ')
            video_formats.append(new_video.lower())
        
        else:
            continue
    
    #save lists in case process is interrupted
    with open(format_lists, 'wb') as f:
        pickle.dump((audio_formats, video_formats), f)
    
    '''loop through each barcode in appraisal spreadsheet'''
    for barcode in app_ws['A'][1:]:
        if not barcode is None:
            
            #set key variables based on barcode
            item_barcode = barcode.value
            folders = bdpl_folders(unit_name, shipmentDate, item_barcode)
            files_dir = folders['files_dir']
            image_dir = folders['image_dir']
            
            print('\nCurrent item: {}'.format(item_barcode))

            #check to see if item was already completed
            if os.path.exists(status_log):
                with open(status_log, 'r') as f:
                    if item_barcode in f.read().splitlines():
                        print('\tItem already completed')
                        continue
            
            #get metadata for barcode and pickle it
            print('\n\tGathering metadata for barcode...')
            if not load_metadata(folders, item_barcode, spreadsheet):
                continue
            
            #get pickled metadata so we can use it
            metadata_dict = pickleLoad('metadata_dict', folders, item_barcode)
            
            #check to see if content should go to MCO; if not, move on to next barcode.
            if not 'mco' in metadata_dict['initial_appraisal'].lower():
                print('\tDo not deposit to MCO')
                continue
            
            '''Some metadata fields need to be compiled and/or selected from alternatives'''
            #check for title:
            if metadata_dict.get('item_title') and metadata_dict.get('item_title') not in ['', '-', 'N/A']:
                item_title = metadata_dict['item_title']
            else:
                item_title = metadata_dict['label_transcription']
                
            #check for a description
            item_description = metadata_dict.get('item_description', '')
                
            #check for dates
            if metadata_dict.get('assigned_dates') and metadata_dict.get('assigned_dates') not in ['', '-', 'N/A']:
                date_issued = metadata_dict['assigned_dates']
            else:
                if metadata_dict['begin_date'] == metadata_dict['end_date']:
                    date_issued = metadata_dict['begin_date'].replace('undated', '')
                else:
                    date_issued = "{}/{}".format(metadata_dict['begin_date'], metadata_dict['end_date'])
                
                
            #check if content came from a DVD or CD (by job type).  NOTE: we are currently assuming all content comes from optical media
            phys_descr = 'Optical disc'
            
            # if metadata_dict.get('jobType') and metadata_dict.get('jobType') in ['DVD', 'CDDA']:
                # phys_descr = 'Optical disc'
            # else:
                # phys_descr = ''
            
            '''add metadata to MCO dictionary'''
            mco_metadata = {'BDPLID' : item_barcode, 
                            'ID_Type_3' : 'BDPL ID', 
                            'CollectionID' : metadata_dict['current_coll_id'], 
                            'ID_Type_1' : 'Collection ID',  
                            'AccessionID' : metadata_dict['current_accession'], 
                            'ID_Type_2' : 'Accession ID', 
                            'Title' : item_title, 
                            'Creator' : metadata_dict['collection_creator'], 
                            'DateIssued' : date_issued, 
                            'Abstract' : item_description, 
                            'PhysicalDescription' : phys_descr, 
                            'Publish' : 'No'}
            
            #try to clear out any bad data
            for k, v in mco_metadata.items():
                if v == '-' or v == 'N/A':
                    mco_metadata[k] = ''
                    
            '''Now look for our files'''
            
            audio_file_list = [f for f in os.listdir(files_dir) if os.path.splitext(f)[-1].lower() in audio_formats]
            cue_file_list = [f for f in os.listdir(files_dir) if os.path.splitext(f)[-1].lower() == '.cue']
            video_file_list = [f for f in os.listdir(files_dir) if os.path.splitext(f)[-1].lower() in video_formats]
            
            for ls in (audio_file_list, video_file_list):
                
                #if list is empty, continue.
                if len(ls) == 0:
                    continue
                
                #establish type of content. NOTE: may need to change labels, based upon content source...
                if ls == audio_file_list:
                    label = 'CD'
                else:
                    label = 'DVD'
                
                for i in range(0, len(ls)):
                    
                    mco_file = ls[i]
                    
                    print('\n\tWorking on {}'.format(mco_file))
                    
                    #get current count of 'file' fields in MCO spreadshet
                    column_count = {i : cell.value for i, cell in enumerate(mco_ws[2], 1) if cell.value == 'File'}
                    
                    #assign values to mco metadata dictionary
                    mco_metadata['File_{}'.format(i)] = mco_file
                    mco_metadata['Label_{}'.format(i)] = '{} part {}'.format(label, i+1)
                    
                    #if we exceed current # of 'File'/'Label' fields in the spreadsheet, we need to add a new one
                    if i+1 > len(column_count):
                        #new 'file' column will be two over from the last one
                        current_max = max(column_count.keys()) + 2
                        mco_ws.insert_cols(current_max)
                        mco_ws.cell(row=2, column=current_max, value='File')
                        
                        #add new 'label' column
                        current_max += 1
                        mco_ws.insert_cols(current_max)
                        mco_ws.cell(row=2, column=current_max, value='Label')
                    
                    #copy file to MCO dropbox location
                    target = os.path.join(files_dir, mco_file)
                    destination = os.path.join(mco_assets, mco_file)
                    print('\t\tCopying file...')
                    shutil.copy(target, mco_assets)
                    
                    #for audio files with CUE: create structure.xml file
                    if label == 'CD':
                        found_cue = [c for c in cue_file_list if os.path.splitext(c)[0] == os.path.splitext(mco_file)[0]]
                        
                        #if we've found a wav_cue_file file, convert to structure.xml
                        if found_cue:
                            
                            #old files: make sure we note it's an optical disk...
                            if mco_metadata['PhysicalDescription'] == '':
                                mco_metadata['PhysicalDescription'] = 'Optical disc'
                            
                            cue_file = os.path.join(files_dir, found_cue[0])
                            
                            print('\t\tCreating structure.xml file for audio...', cue_file)
                            
                            #get info from wav_cue_file file.  NOTE: old procedures resulted in an encoding issue and do not reference the WAV file; let's try to fix those!
                            while True:
                                try:
                                    with open(cue_file, 'r') as f:
                                        cue_contents = f.read().splitlines()
                                        
                                    if 'BINARY' in cue_contents[0]:
                                        fix_cue(files_dir, cue_file)
                                    elif 'WAVE' in cue_contents[0]:
                                        break
                                
                                #if we get UnicodeDecodeError, we should fix cue files in both files_dir and image_dir
                                except UnicodeDecodeError:
                                    #fix the wav cue file
                                    fix_cue(files_dir, cue_file)
                                    
                                    #grab bin cue(s) and fix them, too.
                                    bin_cues = glob.glob(os.path.join(image_dir, '*.cue'))
                                    for bc in bin_cues:
                                        fix_cue(image_dir, bc)
                            
                            #pull out tracks and time indices
                            tracks = [c.strip().replace(' AUDIO', '').capitalize() for c in cue_contents if "TRACK" in c]
                            times = [':'.join(c.split()[2].split(':', 2)[:2]) for c in cue_contents if "INDEX 01" in c]
                            
                            #set up dictionary to store track information
                            track_info = {}
                            
                            #loop through our list of tracks; each track should have a corresponding INDEX 01 timestamp
                            for i in range(0, len(tracks)):
                                
                                #get the begin time.  
                                if times[i] == '00:00':
                                    begin_time = '0'
                                else:
                                    begin_time = times[i]
                                
                                #get end time; final track does not need one
                                if times[i] == times[-1]:
                                    track_info[tracks[i]] = {'begin':begin_time}
                                else:
                                    end_time = calc_end_time(times[i+1])
                                    track_info[tracks[i]] = {'begin':begin_time, 'end':end_time}
                                
                            #start creating our structure xml doc with lxml
                            structure_xml = os.path.join(mco_assets, '{}.structure.xml'.format(mco_file))
                            
                            item = etree.Element('item')
                            item.attrib['label'] = mco_metadata.get('Title', 'Audio recording').replace('"', "'").replace('&', 'and').strip()
                            
                            #loop through our track info to pull in info for the individual 'spans'
                            for track in track_info.keys():
                                span = etree.SubElement(item, 'span')
                                span.attrib['label'] = track
                                span.attrib['begin'] = track_info[track]['begin']
                                #only include 'end' attribute if we have an end time
                                if track_info[track].get('end'):
                                    span.attrib['end'] = track_info[track]['end']
                            
                            #write etree to file in mco assets folder
                            structure_tree = etree.ElementTree(item)
                            structure_tree.write(structure_xml, pretty_print=True)

            '''write info for this barcode to mco spreadsheet'''
            mco_ws.append(list(mco_metadata.values()))
            mco_wb.save(mco_spreadsheet)
            
            '''record completed status in log file'''
            with open(status_log, 'a') as f:
                f.write('{}\n'.format(item_barcode))
            
    print('\n\n-----------------------------------------------------------------------------\n\nMCO preparation complete.\n\nNOTE: {} must be copied to {} for content to be uploaded to Media Collections Online.'.format(mco_spreadsheet, mco_dropbox))

                
            
            
            
            
    
    
    
        
if __name__ == "__main__":
    main()