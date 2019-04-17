# bdpl_ingest
Python tool for the initial ingest and SIP creation of content in the Indiana University Libraries' Born Digital Preservation Lab (BDPL).

This project was inspired by and includes significant elements of [Brunnhilde](https://github.com/timothyryanwalsh/brunnhilde) and [Disk Image Processor](https://github.com/CCA-Public/diskimageprocessor), both &copy; by [Timothy Walsh](https://www.bitarchivist.net/) and released under an MIT License.

## Overview
bdpl_ingest provides a graphical user interface to assist BDPL with the initial ingest and Submission Information Package (SIP) creation for digital materials acquired by the Indiana University Libraries.  It provides a graphical user interface to guide staff through transfer and analysis workflow steps as determined by the type of content. The tool generates log files for each preservation action and also records events in a basic Preservation Metadata: Implementation Strategies (PREMIS) XML file.

bdpl_ingest addresses four main job types:
* __Disk image creation__: use cases involving digital material stored on physical media, including 5.25" floppies, 3.5" floppies, zip disks, optical media, USB drives, and hard drives.
* Transfer:
    * _ddrescue_ (disk image creation)
    * _tsk_rescue_ (file extraction from disk images with file systems that include ntfs, fat, exfat, hfs+, etc.)
    * _unhfs_ (file extraction from disk images with file systems that include hfs and hfsx)
    * _TeraCopy_ (replication of files from optical media with ISO9660 or UDF file systems)
  * Analysis: 
    * Forensic feature analysis with _disktype_, _fsstat_, _ils_, and _mmls_ (document disk image and produce information used by other tools).
    * DFXML creation with _fiwalk_ or _md5deep_ (depending on source file system)
* __Copy__: use cases where disk imaging is not appropriate or where content has arrived via email, network transfer, or download.
* __DVD__: use cases where moving image content is stored as DVD-Video on optical media.
* __CD-DA__: use cases where sound recordings are stored as Compact Disk Digital Audio on optical media.

Each job type is comprised of two main steps: transfer and migration.

####Transfer
During the transfer step, bdpl_ingest seeks to capture an authentic copy of the digital archives, e
