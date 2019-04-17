# bdpl_ingest
Python tool for the initial ingest and SIP creation of content in the Indiana University Libraries' Born Digital Preservation Lab (BDPL).

This project was inspired by and includes significant elements of [Brunnhilde](https://github.com/timothyryanwalsh/brunnhilde) and [Disk Image Processor](https://github.com/CCA-Public/diskimageprocessor), both &copy; by [Timothy Walsh](https://www.bitarchivist.net/) and released under an MIT License.

## Overview
bdpl_ingest provides a graphical user interface to assist BDPL with the initial ingest and Submission Information Package (SIP) creation for digital materials acquired by the Indiana University Libraries.  It provides a graphical user interface to guide staff through transfer and analysis workflow steps as determined by the type of content. The tool generates log files for each preservation action and also records events in a basic Preservation Metadata: Implementation Strategies (PREMIS) XML file.

bdpl_ingest is currently used on Windows 10 workstations managed by IU Libraries Desktop Support, but could be adapted to run in a Linux or Mac environment with the substitution of appropriate tools for Windows-specific resources. 

## Preservation Events
bdpl_ingest employs a micro-service design to address four main job types:
* __Disk image creation__: use cases involving digital material stored on physical media, including 5.25" floppies, 3.5" floppies, zip disks, optical media, USB drives, and hard drives.
* __Copy__: use cases where disk imaging is not appropriate or where content has arrived via email, network transfer, or download.
* __DVD__: use cases where moving image content is stored as DVD-Video on optical media.
* __CDDA__: use cases where sound recordings are stored as Compact Disk Digital Audio on optical media.

Each job type is comprised of two main steps: transfer and migration. Significant preservation events include:
* Transfer:
  * Disk imaging 
    * _ddrescue_ (production of raw images)
    * _cdrdao_ (production of bin and cue files for CDDA use cases)
  * File replication
    * _tsk_rescue_ (file extraction from disk images with file systems that include ntfs, fat, exfat, hfs+, etc.)
    * _unhfs_ (file extraction from disk images with file systems that include hfs and hfsx)
    * _TeraCopy_ (replication of files in other use cases, including from optical media with ISO9660 or UDF file systems)
  * Normalization
    * _cdparanoia_ (production of single .wav and cue files for CDDA use cases)
    * _ffmpeg_ (production of one .mpeg per title for DVD-Video use cases, with content information provided by _lsdvd_)
* Analysis: 
  * Virus scan: _mpcmdrun.exe_
  * Sensitive data scan: _bulk_extractor_
  * Forensic feature analysis:
    * _disktype_ (document disk image file system information)
    * _fsstat_ (document range of meta-data values (inode numbers) and blocks or clusters)
    * _ils_ (document allocated and unallocated inodes on the disk image)
    * _mmls_ (document the layout of partitions on the disk image)
  * Format identification: _Siegfried_
  * Documentation of file directory structure (_tree_)
  * Checksum creation (_fiwalk_ or _md5deep_, depending on use case)

## Results
bdpl_ingest produces a standardized SIP as well as a report and documentation of ingest procedures.  Each transfer item is identified by  a unique barcode identifier; these typically correspond to an individual storage media item or other transfer (though in some cases, particularly large items may be subudivided in consultation with collecting units).

A barcode folder has the following structure:

 [barcode]/
 |
 |_ disk-image/ (if produced)
 |
 |_ files/ (including normalized versions of content from DVD-Video and CDDA use cases)
 |
 |- metadata/
    |
    |_ [barcode]-dfxml.xml
    |
    |_ [barcode]-premis.xml
    |
    |_ logs/
    |
    |_ reports/ (including version of Brunnhilde html report)
    
In addition, highlevel information about each object and the ingest process is saved to a spreadsheet to assist collecting units with the review and appraisal of content before it is saved to secure storage to await final ingest and AIP creation procedures.

## Dependencies
bdpl_ingest requires the following to be installed on a Windows 10 operating system:
* Python 3: basic installation plus
  * lxml
  * openpyxl
* Cygwin: basic installation plus
  * ddrescue
  * tree
  * cd-paranoia
  * Additional tools for compiling source code (gcc-core, gcc-g++, make)
* Utilities compiled from source (and saved to /cygwin64/usr/local/bin):
  * [bchunk](https://github.com/hessu/bchunk)
  * [lsdvd](https://sourceforge.net/projects/lsdvd/files/lsdvd/lsdvd-0.17.tar.gz/download) (requires [libdvdread](https://download.videolan.org/pub/videolan/libdvdread/6.0.1) to be compiled and installed first)
  * [toc2cue](https://sourceforge.net/projects/cdrdao/files/) (utility associated with cdrdao)
* Additional resources:
  * [hfs-explorer](https://sourceforge.net/projects/catacombae/files/HFSExplorer/0.23.1%20%28snapshot%202016-09-02%29/) (snapshot release 0.23.1)
  * [md5deep](https://github.com/jessek/hashdeep/releases)
  * [siegfried](https://www.itforarchivists.com/siegfried/)
  * [bulk_extractor](http://downloads.digitalcorpora.org/downloads/bulk_extractor/)
  * [The Sleuth Kit](http://www.sleuthkit.org/sleuthkit/download.php) (available as pre-compiled Windows binaries)
  * [disktype](http://disktype.sourceforge.net/)
  * [cdrao](http://www.exactaudiocopy.de/en/index.php/resources/download/) (pre-combiled version available in Exact Audio Copy)
  * [ffmpeg](https://ffmpeg.zeranoe.com/builds/) (available as precompiled Windows binaries)
  * [du64](https://docs.microsoft.com/en-us/sysinternals/downloads/du)
  * [TeraCopy](https://www.codesector.com/downloads)
  * [sqlite3](http://www.sqlitetutorial.net/download-install-sqlite/)
  * [FC5025 driver](http://www.deviceside.com/drivers.html) (for use with Device Side Data FC5025 floppy controller for 5.25" floppy disk drives)

In addition, long paths needs to be enableed on Windows 10 (as outlined in [this guide](https://web.archive.org/web/20181129150128/https://lifehacker.com/windows-10-allows-file-names-longer-than-260-characters-1785201032)).  'AutoPlay' should also be turned off USB and drives and the default for all optical disc media should be set to 'do nothing.'
  
  
  

    
