'''
Script to add PREMIS preservation metadata to a transfer. Primary use case is 
one in which the automated disk imaging or replication procedure has failed 
and staff must use alternative method to capture content.

Staff should launch bdpl_ingest.py, with 're-analyze files' option.
'''

import pickle
import os
import datetime

def pickleLoad(target, barcode):
    metadata = os.path.join(target, 'metadata')
    temp_dir = os.path.join(target, 'temp')
    temp_file = os.path.join(temp_dir, 'premis_list.txt')
    
    #this list will be used to store anything pulled in from premis xml; we'll check later to see if anything was added
    temp_premis = []

    temp_list = []
    
    premis_path = os.path.join(metadata, '%s-premis.xml' % barcode)
    premis_xml_included = os.path.join(temp_dir, 'premis_xml_included.txt')
    
    #for our list of premis events, we want to pull in information that may have already been written to premis xml
    if os.path.exists(premis_path):
        
        #check to see if operation has already been completed (we'll write an empty file once we've done so)
        if not os.path.exists(premis_xml_included):
            PREMIS_NAMESPACE = "http://www.loc.gov/premis/v3"
            NSMAP = {'premis' : PREMIS_NAMESPACE, "xsi": "http://www.w3.org/2001/XMLSchema-instance"}
            parser = etree.XMLParser(remove_blank_text=True)
            tree = etree.parse(premis_path, parser=parser)
            root = tree.getroot()
            events = tree.xpath("//premis:event", namespaces=NSMAP)
            
            for e in events:
                temp_dict = {}
                temp_dict['eventType'] = e.findtext('./premis:eventType', namespaces=NSMAP)
                temp_dict['eventOutcomeDetail'] = e.findtext('./premis:eventOutcomeInformation/premis:eventOutcome', namespaces=NSMAP)
                temp_dict['timestamp'] = e.findtext('./premis:eventDateTime', namespaces=NSMAP)
                temp_dict['eventDetailInfo'] = e.findall('./premis:eventDetailInformation/premis:eventDetail', namespaces=NSMAP)[0].text
                temp_dict['eventDetailInfo_additional'] = e.findall('./premis:eventDetailInformation/premis:eventDetail', namespaces=NSMAP)[1].text
                temp_dict['linkingAgentIDvalue'] = e.findall('./premis:linkingAgentIdentifier/premis:linkingAgentIdentifierValue', namespaces=NSMAP)[1].text
                temp_premis.append(temp_dict)
                
            #now create our premis_xml_included.txt file so we don't go through this again.
            open(premis_xml_included, 'a').close()
        
    #make sure there's something in the file
    if os.path.exists(temp_file):
        with open(temp_file, 'rb') as file:
            temp_list = pickle.load(file)
    
    #if anything was added from our premix.xml file, 
    if len(temp_premis) > 0:
        for d in temp_premis:
            if not d in temp_list:
                temp_list.append(d)
        
        #now sort based on ['timestamp']
        temp_list.sort(key=lambda x:x['timestamp'])
            
    return temp_list

def premis_dict(timestamp, event_type, event_outcome, event_detail, event_detail_note, agent_id):
    temp_dict = {}
    temp_dict['eventType'] = event_type
    temp_dict['eventOutcomeDetail'] = event_outcome
    temp_dict['timestamp'] = timestamp
    temp_dict['eventDetailInfo'] = event_detail
    temp_dict['eventDetailInfo_additional'] = event_detail_note
    temp_dict['linkingAgentIDvalue'] = agent_id
    return temp_dict

def main():
    print('\nNOTE: timestamp should be recorded immediately before running manual operation.')
    timestamp = input('\nEnter ISO-formatted timestamp, path to log file, or hit "Enter" to record timestamp: ')

    if timestamp == '':
        timestamp = str(datetime.datetime.now())
    elif os.path.isfile(timestamp):
        timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(timestamp)).isoformat()

    event_list = ['replication', 'disk image creation', 'normalization', 'forensic feature analysis', 'format identification', 'message digest calculation', 'metadata extraction', 'forensic feature analysis', 'virus check']

    while True:
     
        event_type = input('\nEnter "event type"; be sure to use a term defined in PREMIS data dictionary: ')
        
        if not event_type in event_list:
            continue
        else:
            break
            
    event_outcome = 0

    event_detail = input('\nEnter command line option or note GUI operation: ')

    event_notes_dict = {'replication' : 'Created a copy of an object that is, bit-wise, identical to the original.', 'disk image creation' : 'Extracted a disk image from the physical information carrier.', 'normalization' : 'Transformed object to an institutionally supported preservation format.', 'virus check' : 'Scanned files for malicious programs.', 'format identification' : 'Determined file format and version numbers for content recorded in the PRONOM format registry.', 'metadata extraction' : '',  'forensic feature analysis' : '', 'message digest calculation' : 'Extracted information about the structure and characteristics of content, including file checksums.'}

    if event_type == 'metadata extraction' or event_type == 'forensic feature analysis':
        event_detail_note = inpit('\nEnter description of event: ')
    else:
        event_detail_note = event_notes_dict[event_type]
        
    agent_id = input('\nEnter software name and version: ')

    while True:
        target = input('\nEnter path to barcode folder: ')
        
        if os.path.exists(target):
            break
    
    barcode = os.path.basename(target)
    
    premis_list = pickleLoad(target, barcode)
    
    manual_info = premis_dict(timestamp, event_type, event_outcome, event_detail, event_detail_note, agent_id)
    
    premis_list.append(manual_info)
    
    temp_dir = os.path.join(target, 'temp')
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)
    
    temp_file = os.path.join(temp_dir, 'premis_list.txt')
    with open(temp_file, 'wb') as file:
        pickle.dump(premis_list, file)
        
    print('\nPREMIS preservation metadata added to barcode transfer information')

if __name__ == '__main__':
    
    os.system('cls')
    
    #print BDPL screen
    fname = "C:/BDPL/scripts/bdpl.txt"
    if os.path.exists(fname):
        with open(fname, 'r') as fin:
            print(fin.read())
    
    main()

