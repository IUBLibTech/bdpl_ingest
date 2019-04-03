#This file was adapted with some modifications from the DFXML git repository: https://github.com/simsong/dfxml/blob/master/python/dfxml/objects.py

# This software was developed at the National Institute of Standards
# and Technology in whole or in part by employees of the Federal
# Government in the course of their official duties. Pursuant to
# title 17 Section 105 of the United States Code portions of this
# software authored by NIST employees are not subject to copyright
# protection and are in the public domain. For portions not authored
# by NIST employees, NIST has been granted unlimited rights. NIST
# assumes no responsibility whatsoever for its use by other parties,
# and makes no guarantees, expressed or implied, about its quality,
# reliability, or any other characteristic.
#
# We would appreciate acknowledgement if the software is used.

"""
This file re-creates the major DFXML classes with an emphasis on type safety, serializability, and de-serializability.
With this module, reading disk images or DFXML files is done with the parse or iterparse functions.  Writing DFXML files can be done with the DFXMLObject.print_dfxml function.
"""

__version__ = "0.2.1"

#Remaining roadmap to 1.0.0:
# * Documentation.
# * User testing.
# * Compatibility with the DFXML schema, version >1.1.0.

import logging
import re
import copy
import xml.etree.ElementTree as ET
import subprocess
import dfxml
import os
import sys
import platform

_logger = logging.getLogger(os.path.basename(__file__))

#Contains: (namespace, local name) qualified XML element name pairs
_warned_elements = set([])
_warned_byterun_attribs = set([])

#Contains: Unexpected 'facet' values on byte_runs elements.
_warned_byterun_facets = set([])

#Issue some log statements only once per program invocation.
_nagged_alloc = False
_warned_byterun_badtypecomp = False

def _ET_tostring(e):
    """Between Python 2 and 3, there are some differences in the ElementTree library's tostring() behavior.  One, the method balks at the "unicode" encoding in 2.  Two, in 2, the XML prototype's output with every invocation.  This method serves as a wrapper to deal with those issues."""
    if sys.version_info[0] < 3:
        tmp = ET.tostring(e, encoding="UTF-8")
        if tmp[0:2] == "<?":
            #Trim away first line; it's an XML prototype.  This only appears in Python 2's ElementTree output.
            return tmp[ tmp.find("?>\n")+3 : ]
        else:
            return tmp
    else:
        return ET.tostring(e, encoding="unicode")

def _boolcast(val):
    """Takes Boolean values, and 0 or 1 in string or integer form, and casts them all to Boolean.  Preserves nulls.  Balks at everything else."""
    if val is None:
        return None
    if val in [True, False]:
        return val

    _val = val
    if val in ["0", "1"]:
        _val = int(val)
    if _val in [0, 1]:
        return _val == 1

    _logger.debug("val = " + repr(val))
    raise ValueError("Received a not-straightforwardly-Boolean value.  Expected some form of 0, 1, True, or False.")

def _bytecast(val):
    """Casts a value as a byte string.  If a character string, assumes a UTF-8 encoding."""
    if val is None:
        return None
    if isinstance(val, bytes):
        return val
    return _strcast(val).encode("utf-8")

def _intcast(val):
    """Casts input integer or string to integer.  Preserves nulls.  Balks at everything else."""
    if val is None:
        return None
    
    if sys.version_info[0] < 3:
        if isinstance(val, long):
            return val
    
    if isinstance(val, int):
        return val

    if isinstance(val, str):
        if val[0] == "-":
            if val[1:].isdigit():
                return int(val)
        else:
            if val.isdigit():
                return int(val)

    _logger.debug("val = " + repr(val))
    raise ValueError("Received a non-int-castable value.  Expected an integer or an integer as a string.")

def _read_differential_annotations(annodict, element, annoset):
    """
    Uses the shorthand-to-attribute mappings of annodict to translate attributes of element into annoset.
    """
    #_logger.debug("annoset, before: %r." % annoset)
    #Start with inverting the dictionary
    _d = { annodict[k].replace("delta:",""):k for k in annodict }
    #_logger.debug("Inverted dictionary: _d = %r" % _d)
    for attr in element.attrib:
        #_logger.debug("Looking for differential annotations: %r" % element.attrib)
        (ns, an) = _qsplit(attr)
        if an in _d and ns == dfxml.XMLNS_DELTA:
            #_logger.debug("Found; adding %r." % _d[an])
            annoset.add(_d[an])
    #_logger.debug("annoset, after: %r." % annoset)

def _qsplit(tagname):
    """Requires string input.  Returns namespace and local tag name as a pair.  I could've sworn this was a basic implementation gimme, but ET.QName ain't it."""
    _typecheck(tagname, str)
    if tagname[0] == "{":
        i = tagname.rfind("}")
        return ( tagname[1:i], tagname[i+1:] )
    else:
        return (None, tagname)

def _strcast(val):
    if val is None:
        return None
    return str(val)

def _typecheck(obj, classinfo):
    if not isinstance(obj, classinfo):
        _logger.info("obj = " + repr(obj))
        if isinstance(classinfo, tuple):
            raise TypeError("Expecting object to be one of the types %r." % (classinfo,))
        else:
            raise TypeError("Expecting object to be of type %r." % classinfo)

class LibraryObject(object):
    def __init__(self, *args, **kwargs):
        self.name = None
        self.version = None

        if len(args) >= 1:
            self.name = args[0]
        if len(args) >= 2:
            self.version = args[1]

    def __eq__(self, other):
        """
        This equality function tests the name and version values strictly.  For less-strict testing, like allowing matching on missing versions, use relaxed_eq.
        This function can compare against another LibraryObject.
        """
        if not isinstance(other, LibraryObject):
            return False
        return self.name == other.name and \
          self.version == other.version

    def __repr__(self):
        parts = []
        if self.name:
            parts.append("name=%r" % self.name)
        if self.version:
            parts.append("version=%r" % self.version)
        return "LibraryObject(" + ", ".join(parts) + ")"

    def populate_from_Element(self, e):
        if "name" in e.attrib:
            self.name = e.attrib["name"]
        if "version" in e.attrib:
            self.version = e.attrib["version"]

    def relaxed_eq(self, other):
        """
        This function can compare against another LibraryObject.
        """
        if not isinstance(other, LibraryObject):
            return False
        if self.name != other.name:
            return False
        if self.version is None or other.version is None:
            return True
        return self.version == other.version

    def to_Element(self):
        outel = ET.Element("library")
        if not self.name is None:
            outel.attrib["name"] = self.name
        if not self.version is None:
            outel.attrib["version"] = self.version
        return outel

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = _strcast(value)

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, value):
        self._version = _strcast(value)

class DFXMLObject(object):
    def __init__(self, *args, **kwargs):
        self.command_line = kwargs.get("command_line")
        self.version = kwargs.get("version")
        self.sources = kwargs.get("sources", [])
        self.dc = kwargs.get("dc", dict())

        self._namespaces = dict()
        self._volumes = []
        self._files = []

        input_volumes = kwargs.get("volumes") or []
        input_files = kwargs.get("files") or []
        for v in input_volumes:
            self.append(v)
        for f in input_files:
            self.append(f)

        #Add default namespaces
        self.add_namespace("", dfxml.XMLNS_DFXML)
        self.add_namespace("dc", dfxml.XMLNS_DC)
        
        self._creator_libraries = []

    def __iter__(self):
        """Yields all VolumeObjects, recursively their FileObjects, and the FileObjects directly attached to this DFXMLObject, in that order."""
        for v in self._volumes:
            yield v
            for f in v:
                yield f
        for f in self._files:
            yield f

    
    
    def add_creator_library(self, *args, **kwargs):
        self._add_library(self.creator_libraries, *args, **kwargs)
        
    def _add_library(self, target_list, *args, **kwargs):
        #_logger.debug("_add_library:args = %r." % (args,))
        _library = None
        if len(args) == 1 and isinstance(args[0], LibraryObject):
            _library = args[0]
        elif len(args) > 1 and isinstance(args[0], str) and isinstance(args[1], str):
            _library = LibraryObject(args[0], args[1])
        else:
            raise ValueError("Unexpected arguments format (expecting (string, string) or a LibraryObject): %r." % (args,))
        #_logger.debug("_library = %r." % _library)
        if not _library is None:
            target_list.append(_library)
    
    def add_namespace(self, prefix, url):
        self._namespaces[prefix] = url
        ET.register_namespace(prefix, url)

    def append(self, value):
        if isinstance(value, VolumeObject):
            self._volumes.append(value)
        elif isinstance(value, FileObject):
            self._files.append(value)
        else:
            _logger.debug("value = %r" % value)
            raise TypeError("Expecting a VolumeObject or a FileObject.  Got instead this type: %r." % type(value))

    def iter_namespaces(self):
        """Yields (prefix, url) pairs of each namespace registered in this DFXMLObject."""
        for prefix in self._namespaces:
            yield (prefix, self._namespaces[prefix])

    def populate_from_Element(self, e):
        if "version" in e.attrib:
            self.version = e.attrib["version"]

        for elem in e.findall(".//*"):
            (ns, ln) = _qsplit(elem.tag)
            if ln == "command_line":
                self.command_line = elem.text
            elif ln == "image_filename":
                self.sources.append(elem.text)

    def print_dfxml(self, output_fh=sys.stdout):
        """Memory-efficient DFXML document printer.  However, it assumes the whole element tree is already constructed."""
        pe = self.to_partial_Element()
        dfxml_wrapper = _ET_tostring(pe)
        dfxml_foot = "</dfxml>"
        #Check for an empty element
        if dfxml_wrapper.strip()[-3:] == " />":
            dfxml_head = dfxml_wrapper.strip()[:-3] + ">"
        elif dfxml_wrapper.strip()[-2:] == "/>":
            dfxml_head = dfxml_wrapper.strip()[:-2] + ">"
        else:
            dfxml_head = dfxml_wrapper.strip()[:-len(dfxml_foot)]

        output_fh.write("""<?xml version="1.0"?>\n""")
        output_fh.write(dfxml_head)
        output_fh.write("\n")
        _logger.debug("Writing %d volume objects." % len(self._volumes))
        for v in self._volumes:
            v.print_dfxml(output_fh)
            output_fh.write("\n")
        _logger.debug("Writing %d file objects." % len(self._files))
        for f in self._files:
            e = f.to_Element()
            output_fh.write(_ET_tostring(e))
            output_fh.write("\n")
        output_fh.write(dfxml_foot)
        output_fh.write("\n")

    def to_Element(self):
        outel = self.to_partial_Element()
        for v in self._volumes:
            tmpel = v.to_Element()
            outel.append(tmpel)
        for f in self._files:
            tmpel = f.to_Element()
            outel.append(tmpel)
        return outel

    def to_dfxml(self):
        """Serializes the entire DFXML document tree into a string.  Then returns that string.  RAM-intensive.  Most will want to use print_dfxml() instead"""
        return _ET_tostring(self.to_Element())

    def to_partial_Element(self):
        outel = ET.Element("dfxml")

        tmpel0 = ET.Element("metadata")
        for key in sorted(self.dc):
            _typecheck(key, str)
            if ":" in key:
                raise ValueError("Dublin Core key-value entries should have keys without the colon character.  If this causes an interesting namespace issue for you, please report it as a bug.")
            tmpel1 = ET.Element("dc:" + key)
            tmpel1.text = self.dc[key]
            tmpel0.append(tmpel1)
        outel.append(tmpel0)

        if self.command_line:
            tmpel0 = ET.Element("creator")
            tmpel1 = ET.Element("execution_environment")
            tmpel2 = ET.Element("command_line")
            tmpel2.text = self.command_line
            tmpel1.append(tmpel2)
            tmpel0.append(tmpel1)
            outel.append(tmpel0)

        if len(self.sources) > 0:
            tmpel0 = ET.Element("source")
            for source in self.sources:
                tmpel1 = ET.Element("image_filename")
                tmpel1.text = source
                tmpel0.append(tmpel1)
            outel.append(tmpel0)

        if self.version:
            outel.attrib["version"] = self.version

        #Apparently, namespace setting is only available with the write() function, which is memory-impractical for significant uses of DFXML.
        #Ref: http://docs.python.org/3.3/library/xml.etree.elementtree.html#xml.etree.ElementTree.ElementTree.write
        for prefix in self._namespaces:
            attrib_name = "xmlns"
            if prefix != "":
                attrib_name += ":" + prefix
            outel.attrib[attrib_name] = self._namespaces[prefix]

        return outel

    @property
    def command_line(self):
        return self._command_line

    @command_line.setter
    def command_line(self, value):
        self._command_line = _strcast(value)

    @property
    def dc(self):
        """The Dublin Core dictionary of key-value pairs for this document.  Typically, "type" is  "Hash List", or "Disk Image".  Keys should be strings not containing colons, values should be strings.  If this causes an issue for you, please report it as a bug."""
        return self._dc

    @property
    def creator_libraries(self):
        return self._creator_libraries

    
    @dc.setter
    def dc(self, value):
        _typecheck(value, dict)
        self._dc = value

    @property
    def files(self):
        """List of file objects directly attached to this DFXMLObject.  No setter for now."""
        return self._files

    @property
    def namespaces(self):
        raise AttributeError("The namespaces dictionary should not be directly accessed; instead, use .iter_namespaces().")

    @property
    def sources(self):
        return self._sources

    @sources.setter
    def sources(self, value):
        if not value is None:
            _typecheck(value, list)
        self._sources = value

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, value):
        self._version = _strcast(value)

    @property
    def volumes(self):
        """List of volume objects directly attached to this DFXMLObject.  No setter for now."""
        return self._volumes


class RegXMLObject(object):
    def __init__(self, *args, **kwargs):
        self.metadata = kwargs.get("metadata")
        self.creator = kwargs.get("creator")
        self.source = kwargs.get("source")
        self.version = kwargs.get("version")
        self._hives = []
        self._cells = []
        self._namespaces = dict()
        input_hives = kwargs.get("hives") or [] # In case kwargs["hives"] = None.
        input_cells = kwargs.get("cells") or []
        for hive in input_hives:
            self.append(hive)
        for cell in input_cells:
            self.append(cells)

    def __iter__(self):
        """Yields all HiveObjects, recursively their CellObjects, and the CellObjects directly attached to this RegXMLObject, in that order."""
        for h in self._hives:
            yield h
            for c in h:
                yield c
        for c in self._cells:
            yield c

    def append(self, value):
        if isinstance(value, HiveObject):
            self._hives.append(value)
        elif isinstance(value, CellObject):
            self._cells.append(value)
        else:
            _logger.debug("value = %r" % value)
            raise TypeError("Expecting a HiveObject or a CellObject.  Got instead this type: %r." % type(value))

    def print_regxml(self, output_fh=sys.stdout):
        """Serializes and prints the entire object, without constructing the whole tree."""
        regxml_wrapper = _ET_tostring(self.to_partial_Element())
        #_logger.debug("regxml_wrapper = %r." % regxml_wrapper)
        regxml_foot = "</regxml>"
        #Check for an empty element
        if regxml_wrapper.strip()[-3:] == " />":
            regxml_head = regxml_wrapper.strip()[:-3] + ">"
        elif regxml_wrapper.strip()[-2:] == "/>":
            regxml_head = regxml_wrapper.strip()[:-2] + ">"
        else:
            regxml_head = regxml_wrapper.strip()[:-len(regxml_foot)]

        output_fh.write(regxml_head)
        output_fh.write("\n")
        for hive in self._hives:
            hive.print_regxml(output_fh)
        output_fh.write(regxml_foot)
        output_fh.write("\n")

    def to_Element(self):
        outel = self.to_partial_Element()

        for hive in self._hives:
            tmpel = hive.to_Element()
            outel.append(tmpel)

        for cell in self._cells:
            tmpel = cell.to_Element()
            outel.append(tmpel)

        return outel

    def to_partial_Element(self):
        """
        Creates the wrapping RegXML element.  No hives, no cells.  Saves on creating an entire Element tree in memory.
        """
        outel = ET.Element("regxml")

        if self.version:
            outel.attrib["version"] = self.version

        return outel

    def to_regxml(self):
        """Serializes the entire RegXML document tree into a string.  Returns that string.  RAM-intensive.  Most will want to use print_regxml() instead."""
        return _ET_tostring(self.to_Element())


class VolumeObject(object):

    _all_properties = set([
      "annos",
      "allocated_only",
      "block_count",
      "block_size",
      "byte_runs",
      "first_block",
      "ftype",
      "ftype_str",
      "last_block",
      "partition_offset",
      "original_volume",
      "sector_size"
    ])

    _diff_attr_names = {
      "new":"delta:new_volume",
      "deleted":"delta:deleted_volume",
      "modified":"delta:modified_volume",
      "matched":"delta:matched"
    }

    #TODO There may be need in the future to compare the annotations as well.  It complicates make_differential_dfxml too much for now.
    _incomparable_properties = set([
      "annos"
    ])

    def __init__(self, *args, **kwargs):
        self._files = []
        self._annos = set()
        self._diffs = set()

        for prop in VolumeObject._all_properties:
            if prop in ["annos", "files"]:
                continue
            setattr(self, prop, kwargs.get(prop))

    def __iter__(self):
        """Yields all FileObjects directly attached to this VolumeObject."""
        for f in self._files:
            yield f

    def __repr__(self):
        parts = []
        for prop in VolumeObject._all_properties:
            #Skip outputting the files list.
            if prop == "files":
                continue
            val = getattr(self, prop)
            if not val is None:
                parts.append("%s=%r" % (prop, val))
        return "VolumeObject(" + ", ".join(parts) + ")"

    def append(self, value):
        _typecheck(value, FileObject)
        self._files.append(value)

    def compare_to_original(self):
        self._diffs = self.compare_to_other(self.original_volume, True)

    def compare_to_other(self, other, ignore_original=False):
        """Returns a set of all the properties found to differ."""
        _typecheck(other, VolumeObject)
        diffs = set()
        for prop in VolumeObject._all_properties:
            if prop in VolumeObject._incomparable_properties:
                continue
            if ignore_original and prop == "original_volume":
                continue

            #_logger.debug("getattr(self, %r) = %r" % (prop, getattr(self, prop)))
            #_logger.debug("getattr(other, %r) = %r" % (prop, getattr(other, prop)))

            #Allow file system type to be case-insensitive
            if prop == "ftype_str":
                o = getattr(other, prop)
                if o: o = o.lower()
                s = getattr(self, prop)
                if s: s = s.lower()
                if s != o:
                    diffs.add(prop)
            else:
                if getattr(self, prop) != getattr(other, prop):
                    diffs.add(prop)
        return diffs

    def populate_from_Element(self, e):
        global _warned_elements
        _typecheck(e, (ET.Element, ET.ElementTree))
        #_logger.debug("e = %r" % e)

        #Read differential annotations
        _read_differential_annotations(VolumeObject._diff_attr_names, e, self.annos)

        #Split into namespace and tagname
        (ns, tn) = _qsplit(e.tag)
        assert tn in ["volume", "original_volume"]

        #Look through direct-child elements to populate run array
        for ce in e.findall("./*"):
            #_logger.debug("ce = %r" % ce)
            (cns, ctn) = _qsplit(ce.tag)
            #_logger.debug("cns = %r" % cns)
            #_logger.debug("ctn = %r" % ctn)
            if ctn == "byte_runs":
                self.byte_runs = ByteRuns()
                self.byte_runs.populate_from_Element(ce)
            elif ctn == "original_volume":
                self.original_volume = VolumeObject()
                self.original_volume.populate_from_Element(ce)
            elif ctn in VolumeObject._all_properties:
                #_logger.debug("ce.text = %r" % ce.text)
                setattr(self, ctn, ce.text)
                #_logger.debug("getattr(self, %r) = %r" % (ctn, getattr(self, ctn)))
            else:
                if (cns, ctn) not in _warned_elements:
                    _warned_elements.add((cns, ctn))
                    _logger.warning("Unsure what to do with this element in a VolumeObject: %r" % ce)

    def print_dfxml(self, output_fh=sys.stdout):
        pe = self.to_partial_Element()
        dfxml_wrapper = _ET_tostring(pe)

        if len(pe) == 0 and len(self._files) == 0:
            output_fh.write(dfxml_wrapper)
            return

        dfxml_foot = "</volume>"

        #Deal with an empty element being printed as <elem/>
        if len(pe) == 0:
            replaced_dfxml_wrapper = dfxml_wrapper.replace(" />", ">")
            dfxml_head = replaced_dfxml_wrapper
        else:
            dfxml_head = dfxml_wrapper.strip()[:-len(dfxml_foot)]

        output_fh.write(dfxml_head)
        output_fh.write("\n")
        _logger.debug("Writing %d file objects for this volume." % len(self._files))
        for f in self._files:
            e = f.to_Element()
            output_fh.write(_ET_tostring(e))
            output_fh.write("\n")
        output_fh.write(dfxml_foot)
        output_fh.write("\n")

    def to_Element(self):
        outel = self.to_partial_Element()
        for f in self._files:
            tmpel = f.to_Element()
            outel.append(tmpel)
        return outel

    def to_partial_Element(self):
        """Returns the volume element with its properties, except for the child fileobjects.  Properties are appended in DFXML schema order."""
        outel = ET.Element("volume")

        annos_whittle_set = copy.deepcopy(self.annos)
        diffs_whittle_set = copy.deepcopy(self.diffs)

        #Add differential annotations
        for annodiff in VolumeObject._diff_attr_names:
            if annodiff in annos_whittle_set:
                outel.attrib[VolumeObject._diff_attr_names[annodiff]] = "1"
                annos_whittle_set.remove(annodiff)
        if len(annos_whittle_set) > 0:
            _logger.warning("Failed to export some differential annotations: %r." % annos_whittle_set)

        if self.byte_runs:
            outel.append(self.byte_runs.to_Element())

        def _append_el(prop, value):
            tmpel = ET.Element(prop)
            _keep = False
            if not value is None:
                tmpel.text = str(value)
                _keep = True
            if prop in self.diffs:
                tmpel.attrib["delta:changed_property"] = "1"
                diffs_whittle_set.remove(prop)
                _keep = True
            if _keep:
                outel.append(tmpel)

        def _append_str(prop):
            value = getattr(self, prop)
            _append_el(prop, value)

        def _append_bool(prop):
            value = getattr(self, prop)
            if not value is None:
                value = "1" if value else "0"
            _append_el(prop, value)

        for prop in [
          "partition_offset",
          "sector_size",
          "block_size",
          "ftype",
          "ftype_str",
          "block_count",
          "first_block",
          "last_block"
        ]:
            _append_str(prop)

        #Output the one Boolean property
        _append_bool("allocated_only")

        #Output the original volume's properties
        if not self.original_volume is None or "original_volume" in diffs_whittle_set:
            #Skip FileObject list, if any
            if self.original_volume is None:
                tmpel = ET.Element("delta:original_volume")
            else:
                tmpel = self.original_volume.to_partial_Element()
                tmpel.tag = "delta:original_volume"

            if "original_volume" in diffs_whittle_set:
                tmpel.attrib["delta:changed_property"] = "1"

            outel.append(tmpel)

        if len(diffs_whittle_set) > 0:
            _logger.warning("Did not annotate all of the differing properties of this volume.  Remaining properties:  %r." % diffs_whittle_set)

        return outel

    @property
    def allocated_only(self):
        return self._allocated_only

    @allocated_only.setter
    def allocated_only(self, val):
        self._allocated_only = _boolcast(val)

    @property
    def annos(self):
        """Set of differential annotations.  Expected members are the keys of this class's _diff_attr_names dictionary."""
        return self._annos

    @annos.setter
    def annos(self, val):
        _typecheck(val, set)
        self._annos = val

    @property
    def block_count(self):
        return self._block_count

    @block_count.setter
    def block_count(self, val):
        self._block_count = _intcast(val)

    @property
    def block_size(self):
        return self._block_size

    @block_size.setter
    def block_size(self, val):
        self._block_size = _intcast(val)

    @property
    def diffs(self):
        return self._diffs

    @property
    def first_block(self):
        return self._first_block

    @first_block.setter
    def first_block(self, val):
        self._first_block = _intcast(val)

    @property
    def ftype(self):
        return self._ftype

    @ftype.setter
    def ftype(self, val):
        self._ftype = _intcast(val)

    @property
    def ftype_str(self):
        return self._ftype_str

    @ftype_str.setter
    def ftype_str(self, val):
        self._ftype_str = _strcast(val)

    @property
    def last_block(self):
        return self._last_block

    @last_block.setter
    def last_block(self, val):
        self._last_block = _intcast(val)

    @property
    def original_volume(self):
        return self._original_volume

    @original_volume.setter
    def original_volume(self, val):
        if not val is None:
            _typecheck(val, VolumeObject)
        self._original_volume= val

    @property
    def partition_offset(self):
        return self._partition_offset

    @partition_offset.setter
    def partition_offset(self, val):
        self._partition_offset = _intcast(val)

    @property
    def sector_size(self):
        return self._sector_size

    @sector_size.setter
    def sector_size(self, val):
        self._sector_size = _intcast(val)

class HiveObject(object):
    def __init__(self, *args, **kwargs):
        self._cells = []

    def __iter__(self):
        """Yields all CellObjects directly attached to this VolumeObject."""
        for c in self._cells:
            yield c

    def append(self, value):
        _typecheck(value, CellObject)
        self._cells.append(value)

    def print_regxml(self, output_fh=sys.stdout):
        for cell in self._cells:
            output_fh.write(cell.to_regxml())
            output_fh.write("\n")

    def to_Element(self):
        outel = ET.Element("hive")
        for cell in self._cells:
            tmpel = cell.to_Element()
            outel.append(tmpel)
        return outel

class ByteRun(object):

    _all_properties = set([
      "img_offset",
      "fs_offset",
      "file_offset",
      "fill",
      "len"
    ])

    def __init__(self, *args, **kwargs):
        for prop in ByteRun._all_properties:
            setattr(self, prop, kwargs.get(prop))

    def __add__(self, other):
        """
        Joins two ByteRun objects into a single run if possible.  Returns a new object of the concatenation if successful, None if not.
        """
        _typecheck(other, ByteRun)
        #Don't glom fills of different values
        if self.fill != other.fill:
            return None

        if None in [self.len, other.len]:
            return None

        for prop in ["img_offset", "fs_offset", "file_offset"]:
            if None in [getattr(self, prop), getattr(other, prop)]:
                continue
            if getattr(self, prop) + self.len == getattr(other, prop):
                retval = copy.deepcopy(self)
                retval.len += other.len
                return retval
        return None

    def __eq__(self, other):
        #Check type
        if other is None:
            return False
        if not isinstance(other, ByteRun):
            if not _warned_byterun_badtypecomp:
                _logger.warning("A ByteRun comparison was called against a non-ByteRun object: " + repr(other) + ".")
                _warned_byterun_badtypecomp = True
            return False

        #Check values
        return \
          self.img_offset == other.img_offset and \
          self.fs_offset == other.fs_offset and \
          self.file_offset == other.file_offset and \
          self.fill == other.fill and \
          self.len == other.len

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        parts = []
        for prop in ByteRun._all_properties:
            val = getattr(self, prop)
            if not val is None:
                parts.append("%s=%r" % (prop, val))
        return "ByteRun(" + ", ".join(parts) + ")"

    def populate_from_Element(self, e):
        _typecheck(e, (ET.Element, ET.ElementTree))

        #Split into namespace and tagname
        (ns, tn) = _qsplit(e.tag)
        assert tn == "byte_run"

        copied_attrib = copy.deepcopy(e.attrib)

        #Populate run properties from element attributes
        for prop in ByteRun._all_properties:
            if prop in copied_attrib:
                val = copied_attrib.get(prop)
                if not val is None:
                    setattr(self, prop, val)
                del copied_attrib[prop]
        #Note remaining properties
        for prop in copied_attrib:
            if prop not in _warned_byterun_attribs:
                _warned_byterun_attribs.add(prop)
                _logger.warning("No instructions present for processing this attribute found on a byte run: %r." % prop)

    def to_Element(self):
        outel = ET.Element("byte_run")
        for prop in ByteRun._all_properties:
            val = getattr(self, prop)
            if not val is None:
                outel.attrib[prop] = str(val)
        return outel

    @property
    def file_offset(self):
        return self._file_offset

    @file_offset.setter
    def file_offset(self, val):
        self._file_offset = _intcast(val)

    @property
    def fill(self):
        """There is an implicit assumption that the fill character is encoded as UTF-8."""
        return self._fill

    @fill.setter
    def fill(self, val):
        self._fill = _bytecast(val)

    @property
    def fs_offset(self):
        return self._fs_offset

    @fs_offset.setter
    def fs_offset(self, val):
        self._fs_offset = _intcast(val)

    @property
    def img_offset(self):
        return self._img_offset

    @img_offset.setter
    def img_offset(self, val):
        self._img_offset = _intcast(val)

    @property
    def len(self):
        return self._len

    @len.setter
    def len(self, val):
        self._len = _intcast(val)

class ByteRuns(object):
    """
    A list-like object for ByteRun objects.
    """
    #Must define these methods to adhere to the list protocol:
    #__len__
    #__getitem__
    #__setitem__
    #__delitem__
    #__iter__
    #append
    #
    #Refs:
    #http://www.rafekettler.com/magicmethods.html
    #http://stackoverflow.com/a/8841520

    _facet_values = [None, "data", "inode", "name"]

    def __init__(self, run_list=None, **kwargs):
        self._facet = kwargs.get("facet")
        self._listdata = []
        if isinstance(run_list, list):
            for run in run_list:
                self.append(run)

    def __delitem__(self, key):
        del self._listdata[key]

    def __eq__(self, other):
        """Compares the byte run lists and the facet (allowing a null facet to match "data")."""
        #Check type
        if other is None:
            return False
        _typecheck(other, ByteRuns)

        if self.facet != other.facet:
            if set([self.facet, other.facet]) != set([None, "data"]):
                return False
        if len(self) != len(other):
            #_logger.debug("len(self) = %d" % len(self))
            #_logger.debug("len(other) = %d" % len(other))
            return False
        for (sbr_index, sbr) in enumerate(self):
            obr = other[sbr_index]
            #_logger.debug("sbr_index = %d" % sbr_index)
            #_logger.debug("sbr = %r" % sbr)
            #_logger.debug("obr = %r" % obr)
            if sbr != obr:
                return False
        return True

    def __getitem__(self, key):
        return self._listdata.__getitem__(key)

    def __iter__(self):
        return iter(self._listdata)

    def __len__(self):
        return self._listdata.__len__()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        parts = []
        for run in self:
            parts.append(repr(run))
        maybe_facet = ""
        if self.facet:
            maybe_facet = "facet=%r, " % self.facet
        return "ByteRuns(" + maybe_facet + "run_list=[" + ", ".join(parts) + "])"

    def __setitem__(self, key, value):
        _typecheck(value, ByteRun)
        self._listdata[key] = value

    def append(self, value):
        """
        Appends a ByteRun object to this container's list.
        """
        _typecheck(value, ByteRun)
        self._listdata.append(value)

    def glom(self, value):
        """
        Appends a ByteRun object to this container's list, after attempting to join the run with the last run already stored.
        """
        _typecheck(value, ByteRun)
        if len(self._listdata) == 0:
            self.append(value)
        else:
            last_run = self._listdata[-1]
            maybe_new_run = last_run + value
            if maybe_new_run is None:
                self.append(value)
            else:
                self._listdata[-1] = maybe_new_run
        
    def iter_contents(self, raw_image, buffer_size=1048576, sector_size=512, errlog=None, statlog=None):
        """
        Generator.  Yields contents, as byte strings one block at a time, given a backing raw image path.  Relies on The SleuthKit's img_cat, so contents can be extracted from any disk image type that TSK supports.
        @param buffer_size The maximum size of the byte strings yielded.
        @param sector_size The size of a disk sector in the raw image.  Required by img_cat.
        """
        if not isinstance(raw_image, str):
            raise TypeError("iter_contents needs the string path to the image file.  Received: %r." % raw_image)

        stderr_fh = None
        if not errlog is None:
            stderr_fh = open(errlog, "wb")

        status_fh = None
        if not statlog is None:
            status_fh = open(errlog, "wb")

        #The exit status of the last img_cat.
        last_status = None

        try:
            for run in self:
                if run.len is None:
                    raise AttributeError("Byte runs can't be extracted if a run length is undefined.")

                len_to_read = run.len

                #If we have a fill character, just pump out that character
                if not run.fill is None and len(run.fill) > 0:
                    while len_to_read > 0:
                        #This multiplication and slice should handle multi-byte fill characters, in case that ever comes up.
                        yield (run.fill * buffer_size)[:len_to_read]
                        len_to_read -= buffer_size
                    #Next byte run
                    continue

                if run.img_offset is None:
                    raise AttributeError("Byte runs can't be extracted if missing a fill character and image offset.")

                cmd = ["img_cat"]
                cmd.append("-b")
                cmd.append(str(sector_size))
                cmd.append("-s")
                cmd.append(str(run.img_offset//sector_size))
                cmd.append("-e")
                cmd.append(str( (run.img_offset + run.len)//sector_size))
                cmd.append(raw_image)
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=stderr_fh)

                #Do the buffered read
                while len_to_read > 0:
                    buffer_data = p.stdout.read(buffer_size)
                    yield_data = buffer_data[ : min(len_to_read, buffer_size)]
                    if len(yield_data) > 0:
                        yield yield_data
                    else:
                        #Let the subprocess terminate so we can see the exit status
                        p.wait()
                        last_status = p.returncode
                        if last_status != 0:
                            raise subprocess.CalledProcessError(last_status, " ".join(cmd), "img_cat failed.")
                    len_to_read -= buffer_size
        except Exception as e:
            #Cleanup in an exception
            if not stderr_fh is None:
                stderr_fh.close()

            if not status_fh is None:
                if isinstance(e, subprocess.CalledProcessError):
                    status_fh.write(e.returncode)
                else:
                    status_fh.write("1")
                status_fh.close()
            raise e

        #Cleanup when all's gone well.
        if not status_fh is None:
            if not last_status is None:
                status_fh.write(last_status)
            status_fh.close()
        if not stderr_fh is None:
            stderr_fh.close()

    def populate_from_Element(self, e):
        _typecheck(e, (ET.Element, ET.ElementTree))

        #Split into namespace and tagname
        (ns, tn) = _qsplit(e.tag)
        assert tn == "byte_runs"
 
        if "facet" in e.attrib:
            self.facet = e.attrib["facet"]

        #Look through direct-child elements to populate run array
        for ce in e.findall("./*"):
            (cns, ctn) = _qsplit(ce.tag)
            if ctn == "byte_run":
                nbr = ByteRun()
                nbr.populate_from_Element(ce)
                self.append(nbr)

    def to_Element(self):
        outel = ET.Element("byte_runs")
        for run in self:
            tmpel = run.to_Element()
            outel.append(tmpel)
        if self.facet:
            outel.attrib["facet"] = self.facet
        return outel

    @property
    def facet(self):
        """Expected to be null, "data", "inode", or "name".  See FileObject.data_brs, FileObject.inode_brs, and FileObject.name_brs."""
        return self._facet

    @facet.setter
    def facet(self, val):
        if not val is None:
            _typecheck(val, str)
        if val not in ByteRuns._facet_values:
            raise ValueError("A ByteRuns facet must be one of these: %r.  Received: %r." % (ByteRuns._facet_values, val))
        self._facet = val

re_precision = re.compile(r"(?P<num>\d+)(?P<unit>(|m|n)s|d)?")
class TimestampObject(object):
    """
    Encodes the "dftime" type.  Wraps around dfxml.dftime, closely enough that this might just get folded into that class.
    TimestampObjects implement a vs-null comparison workaround as in the SAS family of products:  Null, for ordering purposes, is considered to be a value less than negative infinity.
    """

    timestamp_name_list = ["mtime", "atime", "ctime", "crtime", "dtime", "bkup_time"]

    def __init__(self, *args, **kwargs):
        self.name = kwargs.get("name")
        self.prec = kwargs.get("prec")
        #_logger.debug("type(args) = %r" % type(args))
        #_logger.debug("args = %r" % (args,))
        if len(args) == 0:
            self.time = None
        elif len(args) == 1:
            self.time = args[0]
        else:
            raise ValueError("Unexpected arguments.  Whole args tuple: %r." % (args,))

        self._timestamp = None

    def __eq__(self, other):
        #Check type
        if other is None:
            return False
        _typecheck(other, TimestampObject)

        if self.name != other.name:
            return False
        if self.prec != other.prec:
            return False
        if self.time != other.time:
            return False
        return True

    def __ge__(self, other):
        """Note: The semantics here and in other ordering functions are that "Null" is a value less than negative infinity."""
        if other is None:
            return False
        else:
            self._comparison_sanity_check(other)
        return self.time.__ge__(other.time)

    def __gt__(self, other):
        """Note: The semantics here and in other ordering functions are that "Null" is a value less than negative infinity."""
        if other is None:
            return False
        else:
            self._comparison_sanity_check(other)
        return self.time.__gt__(other.time)

    def __le__(self, other):
        """Note: The semantics here and in other ordering functions are that "Null" is a value less than negative infinity."""
        if other is None:
            return True
        else:
            self._comparison_sanity_check(other)
        return self.time.__le__(other.time)

    def __lt__(self, other):
        """Note: The semantics here and in other ordering functions are that "Null" is a value less than negative infinity."""
        if other is None:
            return True
        else:
            self._comparison_sanity_check(other)
        return self.time.__lt__(other.time)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        parts = []
        if self.name:
            parts.append("name=%r" % self.name)
        if self.prec:
            parts.append("prec=%r" % (self.prec,))
        if self.time:
            parts.append("%r" % self.time)
        return "TimestampObject(" + ", ".join(parts) + ")"

    def __str__(self):
        if self.time:
            return str(self.time)
        else:
            return self.__repr__()

    def _comparison_sanity_check(self, other):
        if None in (self.time, other.time):
            raise ValueError("Can't compare TimestampObjects: %r, %r." % self, other)

    def populate_from_Element(self, e):
        _typecheck(e, (ET.Element, ET.ElementTree))
        if "prec" in e.attrib:
            self.prec = e.attrib["prec"]
        self.time = e.text
        (ns, tn) = _qsplit(e.tag)
        self.name = tn

    def to_Element(self):
        _typecheck(self.name, str)
        outel = ET.Element(self.name)
        if self.prec:
            outel.attrib["prec"] = "%d%s" % self.prec
        if self.time:
            outel.text = str(self.time)
        return outel

    @property
    def name(self):
        """The type of timestamp - modified (mtime), accessed (atime), etc."""
        return self._name

    @name.setter
    def name(self, value):
        if not value is None:
            if not value in TimestampObject.timestamp_name_list:
                raise ValueError("The timestamp name must be in this list: %r.  Received: %r." % (TimestampObject.timestamp_name_list, value))
        self._name = value

    @property
    def prec(self):
        """
        A pair, (resolution, unit); unit is a second (s), millisecond, nanosecond, or day (d).  The default unit is "s".  Can be passed as a string or a duple.
        """
        return self._prec

    @prec.setter
    def prec(self, value):
        if value is None:
            self._prec = None
            return self._prec
        elif isinstance(value, tuple) and \
          len(value) == 2 and \
          isinstance(value[0], int) and \
          isinstance(value[1], str):
            self._prec = value
            return self._prec
        
        m = re_precision.match(value)
        md = m.groupdict()
        tup = (int(md["num"]), md.get("unit") or "s")
        #_logger.debug("tup = %r" % (tup,))
        self._prec = tup

    @property
    def time(self):
        """
        The actual timestamp.  A DFXML.dftime object.  This class might be superfluous and end up collapsing into that...
        """
        return self._time

    @time.setter
    def time(self, value):
        if value is None:
            self._time = None
        else:
            checked_value = dfxml.dftime(value)
            #_logger.debug("checked_value.timestamp() = %r" % checked_value.timestamp())
            self._time = checked_value
            #Propagate timestamp value to other formats
            self._timestamp = self._time.timestamp()

    @property
    def timestamp(self):
        """A Unix floating-point timestamp, as time.mktime returns.  Currently, there is no setter for this property."""
        return self._timestamp


class FileObject(object):
    """
    This class provides property accesses, an XML serializer (ElementTree-based), and a deserializer.
    The properties interface is NOT function calls, but simple accesses.  That is, the old _fileobject_ style:
        assert isinstance(fi, dfxml.fileobject)
        fi.mtime()
    is now replaced with:
        assert isinstance(fi, Objects.FileObject)
        fi.mtime
    """

    _all_properties = set([
      "alloc",
      "alloc_inode",
      "alloc_name",
      "annos",
      "atime",
      "bkup_time",
      "byte_runs",
      "compressed",
      "crtime",
      "ctime",
      "data_brs",
      "dtime",
      "error",
      "filename",
      "filesize",
      "gid",
      "id",
      "inode",
      "inode_brs",
      "link_target",
      "libmagic",
      "md5",
      "meta_type",
      "mode",
      "mtime",
      "name_brs",
      "name_type",
      "nlink",
      "original_fileobject",
      "orphan",
      "parent_object",
      "partition",
      "seq",
      "sha1",
      "uid",
      "unalloc",
      "unused",
      "used"
    ])

    _br_facet_to_property = {
      "data":"data_brs",
      "inode":"inode_brs",
      "name":"name_brs"
    }

    #TODO There may be need in the future to compare the annotations as well.  It complicates make_differential_dfxml too much for now.
    _incomparable_properties = set([
      "annos",
      "byte_runs",
      "id",
      "unalloc",
      "unused"
    ])

    _diff_attr_names = {
      "new":"delta:new_file",
      "deleted":"delta:deleted_file",
      "renamed":"delta:renamed_file",
      "changed":"delta:changed_file",
      "modified":"delta:modified_file",
      "matched":"delta:matched"
    }

    def __init__(self, *args, **kwargs):
        #Prime all the properties
        for prop in FileObject._all_properties:
            if prop == "annos":
                continue
            setattr(self, prop, kwargs.get(prop))
        self._annos = set()
        self._diffs = set()

    def __eq__(self, other):
        if other is None:
            return False
        _typecheck(other, FileObject)
        for prop in FileObject._all_properties:
            if prop in FileObject._incomparable_properties:
                continue
            if getattr(self, prop) != getattr(other, prop):
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        parts = []

        for prop in sorted(FileObject._all_properties):
            #Save data byte runs for the end, as theirs lists can get really long.
            if prop not in ["byte_runs", "data_brs"]:
                value = getattr(self, prop)
                if not value is None:
                    parts.append("%s=%r" % (prop, value))

        if self.data_brs:
            parts.append("data_brs=%r" % self.byte_runs)

        return "FileObject(" + ", ".join(parts) + ")"

    def compare_to_original(self):
        self._diffs = self.compare_to_other(self.original_fileobject, True)

    def compare_to_other(self, other, ignore_original=False):
        _typecheck(other, FileObject)

        diffs = set()

        for propname in FileObject._all_properties:
            if propname in FileObject._incomparable_properties:
                continue
            if ignore_original and propname == "original_fileobject":
                continue
            oval = getattr(other, propname)
            sval = getattr(self, propname)
            if oval is None and sval is None:
                continue
            if oval != sval:
                #_logger.debug("propname, oval, sval: %r, %r, %r" % (propname, oval, sval))
                diffs.add(propname)

        return diffs

    def extract_facet(self, facet, image_path=None, buffer_size=1048576, partition_offset=None, sector_size=512, errlog=None, statlog=None, icat_threshold = 268435456):
        """
        Generator.  Extracts the facet with a SleuthKit tool, yielding chunks of the data.
        @param buffer_size The facet data is yielded in chunks of at most this parameter's size. Default 1MiB.
        @param partition_offset The offset of the file's containing partition, in bytes.  Needed for icat.  If not given, the FileObject's VolumeObject will be used.  If that's also absent, icat can't be used, and img_cat will instead be tried as a fallback (which means byte runs must be in the DFXML).
        @param icat_threshold icat incurs extensive, non-sequential IO overhead to walk the filesystem to reach the facet's byte runs.  img_cat can be called on each byte run reported in the DFXML file, but on fragmented files this incurs overhead in process spawning.  Facets larger than this threshold are extracted with icat.  Default 256MiB.  Force icat by setting this to -1; force img_cat with infinity (float("inf")).
        """

        _image_path = image_path
        if _image_path is None:
            raise ValueError("The backing image path must be supplied.")

        _partition_offset = partition_offset
        if _partition_offset is None:
            if self.volume_object:
                _partition_offset = self.volume_object.partition_offset

        #Try using icat; needs inode number and volume offset.  We're additionally requiring the filesize be known.
        #TODO The icat needs a little more experimentation.
        if False and facet == "content" and \
          not self.filesize is None and \
          self.filesize >= icat_threshold and \
          not self.inode is None and \
          not _partition_offset is None:
            _logger.debug("Extracting with icat: %r." % self)

            #Set up logging if desired
            stderr_fh = sys.stderr
            if not errlog is None:
                stderr_fh = open(errlog, "wb")

            status_fh = None
            if not statlog is None:
                status_fh = open(errlog, "w")

            #Set up icat process
            cmd = ["icat"]
            cmd.append("-b")
            cmd.append(str(sector_size))
            cmd.append("-o")
            cmd.append(str(self.volume_object.partition_offset//sector_size))
            if not self.volume_object.ftype_str is None:
                cmd.append("-f")
                cmd.append(self.volume_object.ftype_str)
            cmd.append(image_path)
            cmd.append(str(self.inode))
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=stderr_fh)

            #Do a buffered read
            len_to_read = self.filesize
            while len_to_read > 0:
                buffer_data = p.stdout.read(buffer_size)
                yield_data = buffer_data[ : min(len_to_read, buffer_size)]
                if len(yield_data) > 0:
                    yield yield_data
                else:
                    #Let the subprocess terminate so we can see the exit status
                    p.wait()
                    last_status = p.returncode

                    #Log the status if requested
                    if not status_fh is None:
                        status_fh.write(last_status)

                    #Act on a bad status
                    if last_status != 0:
                        raise subprocess.CalledProcessError(last_status, " ".join(cmd), "icat failed.")
                len_to_read -= buffer_size

            #Clean up file handles
            if status_fh: status_fh.close()
            if stderr_fh: stderr_fh.close()
            
        elif not self.byte_runs is None:
            for chunk in self.byte_runs.iter_contents(_image_path, buffer_size, sector_size, errlog, statlog):
                yield chunk

    def populate_from_Element(self, e):
        """Populates this FileObject's properties from an ElementTree Element.  The Element need not be retained."""
        global _warned_elements
        _typecheck(e, (ET.Element, ET.ElementTree))

        #_logger.debug("FileObject.populate_from_Element(%r)" % e)

        #Split into namespace and tagname
        (ns, tn) = _qsplit(e.tag)
        assert tn in ["fileobject", "original_fileobject", "parent_object"]

        #Map "delta:" attributes of <fileobject>s into the self.annos set
        #_logger.debug("self.annos, before: %r." % self.annos)
        _read_differential_annotations(FileObject._diff_attr_names, e, self.annos)
        #_logger.debug("self.annos, after: %r." % self.annos)

        #Look through direct-child elements for other properties
        for ce in e.findall("./*"):
            (cns, ctn) = _qsplit(ce.tag)
            #_logger.debug("Populating from child element: %r." % ce.tag)

            #Inherit any marked changes
            for attr in ce.attrib:
                #_logger.debug("Inspecting attr for diff. annos: %r." % attr)
                (ns, an) = _qsplit(attr)
                if an == "changed_property" and ns == dfxml.XMLNS_DELTA:
                    #_logger.debug("Identified changed property: %r." % ctn)
                    #TODO There may be a more elegant way of handling the hashes and any other attribute-dependent element-to-property mapping.  Probably involving XPath.
                    if ctn == "hashdigest":
                        if "type" not in ce.attrib:
                            raise AttributeError("Attribute 'type' not found.  Every hashdigest element should have a 'type' attribute to identify the hash type.")
                        self.diffs.add(ce.attrib["type"].lower())
                    elif ctn == "byte_runs":
                        facet = ce.attrib.get("facet")
                        prop = FileObject._br_facet_to_property.get(facet, "data_brs")
                        self.diffs.add(prop)
                    else:
                        self.diffs.add(ctn)

            if ctn == "byte_runs":
                #byte_runs might be for file contents, the inode/MFT entry, or the directory entry naming the file.  Use the facet attribute to determine which.  If facet is absent, assume they're data byte runs.
                if "facet" in ce.attrib:
                    if ce.attrib["facet"] not in FileObject._br_facet_to_property:
                        if not ce.attrib["facet"] in _warned_byterun_facets:
                            _warned_byterun_facets.add(ce.attrib["facet"])
                            _logger.warning("byte_runs facet %r was unexpected.  Will not interpret this element.")
                    else:
                        brs = ByteRuns()
                        brs.populate_from_Element(ce)
                        brs.facet = ce.attrib["facet"]
                        setattr(self, FileObject._br_facet_to_property[brs.facet], brs)
                else:
                    self.byte_runs = ByteRuns()
                    self.byte_runs.populate_from_Element(ce)
            elif ctn == "hashdigest":
                if ce.attrib["type"].lower() == "md5":
                    self.md5 = ce.text
                elif ce.attrib["type"].lower() == "sha1":
                    self.sha1 = ce.text
            elif ctn == "original_fileobject":
                self.original_fileobject = FileObject()
                self.original_fileobject.populate_from_Element(ce)
            elif ctn == "parent_object":
                self.parent_object = FileObject()
                self.parent_object.populate_from_Element(ce)
            elif ctn in ["atime", "bkup_time", "crtime", "ctime", "dtime", "mtime"]:
                setattr(self, ctn, TimestampObject())
                getattr(self, ctn).populate_from_Element(ce)
            elif ctn in FileObject._all_properties:
                setattr(self, ctn, ce.text)
            else:
                if (cns, ctn) not in _warned_elements:
                    _warned_elements.add((cns, ctn))
                    _logger.warning("Uncertain what to do with this element: %r" % ce)

    def populate_from_stat(self, s):
        """Populates FileObject fields from a stat() call."""
        import os
        _typecheck(s, os.stat_result)

        if platform.system() == "Windows":
            # On Windows, Python 2 reports 0L.  Treat this as absent information.
            # On Windows, Python 3 reports the "File ID" ( see "nFileIndexLow" remark at: https://msdn.microsoft.com/en-us/library/aa363788 ).  Record this as the inode number for now.  NOTE: in the future this may become a Windows-namespaced property "fileindex"; it may be prudent to later file a follow-on to Python Issue 32878 ( https://bugs.python.org/issue32878 ).
            if sys.version_info[0] >= 3:
                self.inode = s.st_ino
        else:
            self.inode = s.st_ino
        
        self.mode = s.st_mode
        self.nlink = s.st_nlink
        self.uid = s.st_uid
        self.gid = s.st_gid
        self.filesize = s.st_size
        #s.st_dev is ignored for now.

        if "st_mtime" in dir(s):
            self.mtime = s.st_mtime

        if "st_atime" in dir(s):
            self.atime = s.st_atime

        if "st_ctime" in dir(s):
            self.ctime = s.st_ctime

        if "st_birthtime" in dir(s):
            self.crtime = s.st_birthtime

    def to_Element(self):
        """Creates an ElementTree Element with elements in DFXML schema order."""
        outel = ET.Element("fileobject")

        annos_whittle_set = copy.deepcopy(self.annos)
        diffs_whittle_set = copy.deepcopy(self.diffs)

        for annodiff in FileObject._diff_attr_names:
            if annodiff in annos_whittle_set:
                outel.attrib[FileObject._diff_attr_names[annodiff]] = "1"
                annos_whittle_set.remove(annodiff)
        if len(annos_whittle_set) > 0:
            _logger.warning("Failed to export some differential annotations: %r." % annos_whittle_set)

        def _anno_change(el):
            if el.tag in self.diffs:
                el.attrib["delta:changed_property"] = "1"
                diffs_whittle_set.remove(el.tag)

        def _anno_hash(el):
            if el.attrib["type"] in self.diffs:
                el.attrib["delta:changed_property"] = "1"
                diffs_whittle_set.remove(el.attrib["type"])

        def _anno_byte_runs(el):
            if "facet" in el.attrib:
                prop = FileObject._br_facet_to_property[el.attrib["facet"]]
            else:
                prop = "data_brs"
            if prop in self.diffs:
                el.attrib["delta:changed_property"] = "1"
                #_logger.debug("diffs_whittle_set = %r." % diffs_whittle_set)
                diffs_whittle_set.remove(prop)

        #Recall that Element text must be a string
        def _append_str(name, value):
            """Note that empty elements should be created if the element was removed."""
            if not value is None or name in diffs_whittle_set:
                tmpel = ET.Element(name)
                if not value is None:
                    tmpel.text = str(value)
                _anno_change(tmpel)
                outel.append(tmpel)

        def _append_time(name, value):
            """Note that empty elements should be created if the element was removed."""
            if not value is None or name in diffs_whittle_set:
                if not value is None and value.time:
                    tmpel = value.to_Element()
                else:
                    tmpel = ET.Element(name)
                _anno_change(tmpel)
                outel.append(tmpel)

        def _append_bool(name, value):
            """Note that empty elements should be created if the element was removed."""
            if not value is None or name in diffs_whittle_set:
                tmpel = ET.Element(name)
                if not value is None:
                    tmpel.text = str(1 if value else 0)
                _anno_change(tmpel)
                outel.append(tmpel)

        _using_facets = False
        def _append_byte_runs(name, value):
            """The complicated part here is setting the "data" facet on the byte runs, because we assume that no facet definitions means that for this file, there's only the one byte_runs list for data."""
            #_logger.debug("_append_byte_runs(%r, %r)" % (name, value))
            if value or name in diffs_whittle_set:
                if value:
                    tmpel = value.to_Element()
                    if "facet" in tmpel.attrib:
                        _using_facets = True
                else:
                    tmpel = ET.Element("byte_runs")
                    propname_to_facet = {
                      "data_brs": "data",
                      "inode_brs": "inode",
                      "name_brs": "name"
                    }
                    if name in propname_to_facet:
                        _using_facets = True
                        tmpel.attrib["facet"] = propname_to_facet[name]
                    elif _using_facets:
                        tmpel.attrib["facet"] = propname_to_facet["data_brs"]
                _anno_byte_runs(tmpel)
                outel.append(tmpel)

        def _append_object(name, value, namespace_prefix=None):
            """name must be the name of a property that has a to_Element() method.  namespace_prefix will be prepended as-is to the element tag."""
            obj = value
            if obj or name in diffs_whittle_set:
                if obj:
                    tmpel = obj.to_Element()
                else:
                    tmpel = ET.Element(name)
                #Set the tag name here for properties like parent_object, a FileObject without being wholly a FileObject.
                if namespace_prefix:
                    tmpel.tag = namespace_prefix + name
                else:
                    tmpel.tag = name
                _anno_change(tmpel)
                outel.append(tmpel)

        def _append_hash(name, value):
            if not value is None or name in diffs_whittle_set:
                tmpel = ET.Element("hashdigest")
                tmpel.attrib["type"] = name
                if not value is None:
                    tmpel.text = value
                _anno_hash(tmpel)
                outel.append(tmpel)

        #The parent object is a one-off.  Duplicating the whole parent is wasteful, so create a shadow object that just outputs the important bits.
        if not self.parent_object is None:
            parent_object_shadow = FileObject()
            parent_object_shadow.inode = self.parent_object.inode
            _append_object("parent_object", parent_object_shadow)

        _append_str("filename", self.filename)
        _append_str("error", self.error)
        _append_str("partition", self.partition)
        _append_str("id", self.id)
        _append_str("name_type", self.name_type)
        _append_str("filesize", self.filesize)
        #TODO Define a better flag for if we're going to output <alloc> elements.
        if self.alloc_name is None and self.alloc_inode is None:
            _append_bool("alloc", self.alloc)
        else:
            _append_bool("alloc_inode", self.alloc_inode)
            _append_bool("alloc_name", self.alloc_name)
        _append_bool("used", self.used)
        _append_bool("orphan", self.orphan)
        _append_bool("compressed", self.compressed)
        _append_str("inode", self.inode)
        _append_str("meta_type", self.meta_type)
        _append_str("mode", self.mode)
        _append_str("nlink", self.nlink)
        _append_str("uid", self.uid)
        _append_str("gid", self.gid)
        _append_time("mtime", self.mtime)
        _append_time("ctime", self.ctime)
        _append_time("atime", self.atime)
        _append_time("crtime", self.crtime)
        _append_str("seq", self.seq)
        _append_time("dtime", self.dtime)
        _append_time("bkup_time", self.bkup_time)
        _append_str("link_target", self.link_target)
        _append_str("libmagic", self.libmagic)
        _append_byte_runs("inode_brs", self.inode_brs)
        _append_byte_runs("name_brs", self.name_brs)
        _append_byte_runs("data_brs", self.data_brs)
        _append_hash("md5", self.md5)
        _append_hash("sha1", self.sha1)
        _append_object("original_fileobject", self.original_fileobject, "delta:")

        if len(diffs_whittle_set) > 0:
            _logger.warning("Did not annotate all of the differing properties of this file.  Remaining properties:  %r." % diffs_whittle_set)

        return outel

    def to_dfxml(self):
        return _ET_tostring(self.to_Element())

    @property
    def alloc(self):
        """Note that setting .alloc will affect the value of .unalloc, and vice versa.  The last one to set wins."""
        global _nagged_alloc
        if not _nagged_alloc:
            _logger.warning("The FileObject.alloc property is deprecated.  Use .alloc_inode and/or .alloc_name instead.  .alloc is proxied as True if alloc_inode and alloc_name are both True.")
            _nagged_alloc = True
        if self.alloc_inode and self.alloc_name:
            return True
        else:
            return self._alloc

    @alloc.setter
    def alloc(self, val):
        self._alloc = _boolcast(val)
        if not self._alloc is None:
            self._unalloc = not self._alloc

    @property
    def alloc_inode(self):
        return self._alloc_inode

    @alloc_inode.setter
    def alloc_inode(self, val):
        self._alloc_inode = _boolcast(val)

    @property
    def alloc_name(self):
        return self._alloc_name

    @alloc_name.setter
    def alloc_name(self, val):
        self._alloc_name = _boolcast(val)

    @property
    def annos(self):
        """Set of differential annotations.  Expected members are the keys of this class's _diff_attr_names dictionary."""
        return self._annos

    @annos.setter
    def annos(self, val):
        _typecheck(val, set)
        self._annos = val

    @property
    def atime(self):
        return self._atime

    @atime.setter
    def atime(self, val):
        if val is None:
            self._atime = None
        elif isinstance(val, TimestampObject):
            self._atime = val
        else:
            checked_val = TimestampObject(val, name="atime")
            self._atime = checked_val

    @property
    def bkup_time(self):
        return self._bkup_time

    @bkup_time.setter
    def bkup_time(self, val):
        if val is None:
            self._bkup_time = None
        elif isinstance(val, TimestampObject):
            self._bkup_time = val
        else:
            checked_val = TimestampObject(val, name="bkup_time")
            self._bkup_time = checked_val

    @property
    def byte_runs(self):
        """This property is now a synonym for the data byte runs (.data_brs)."""
        return self.data_brs

    @byte_runs.setter
    def byte_runs(self, val):
        self.data_brs = val

    @property
    def compressed(self):
        return self._compressed

    @compressed.setter
    def compressed(self, val):
        self._compressed = _boolcast(val)

    @property
    def ctime(self):
        return self._ctime

    @ctime.setter
    def ctime(self, val):
        if val is None:
            self._ctime = None
        elif isinstance(val, TimestampObject):
            self._ctime = val
        else:
            checked_val = TimestampObject(val, name="ctime")
            self._ctime = checked_val

    @property
    def crtime(self):
        return self._crtime

    @crtime.setter
    def crtime(self, val):
        if val is None:
            self._crtime = None
        elif isinstance(val, TimestampObject):
            self._crtime = val
        else:
            checked_val = TimestampObject(val, name="crtime")
            self._crtime = checked_val

    @property
    def data_brs(self):
        """The byte runs that store the file's content."""
        return self._data_brs

    @data_brs.setter
    def data_brs(self, val):
        if not val is None:
            _typecheck(val, ByteRuns)
        self._data_brs = val

    @property
    def diffs(self):
        """This property intentionally has no setter.  To populate, call compare_to_original() after assigning an original_fileobject."""
        return self._diffs

    @property
    def dtime(self):
        return self._dtime

    @dtime.setter
    def dtime(self, val):
        if val is None:
            self._dtime = None
        elif isinstance(val, TimestampObject):
            self._dtime = val
        else:
            checked_val = TimestampObject(val, name="dtime")
            self._dtime = checked_val

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, val):
        self._error = _strcast(val)

    @property
    def filesize(self):
        return self._filesize

    @filesize.setter
    def filesize(self, val):
        self._filesize = _intcast(val)

    @property
    def gid(self):
        return self._gid

    @gid.setter
    def gid(self, val):
        self._gid = _strcast(val)

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, val):
        self._id = _intcast(val)

    @property
    def inode(self):
        return self._inode

    @inode.setter
    def inode(self, val):
        self._inode = _intcast(val)

    @property
    def libmagic(self):
        return self._libmagic

    @libmagic.setter
    def libmagic(self, val):
        self._libmagic = _strcast(val)

    @property
    def inode_brs(self):
        """The byte run(s) that represents the file's metadata object (the inode or the MFT entry).  In file systems that do not distinguish between inode and directory entry, e.g. FAT, .inode_brs should be equivalent to .name_brs, if both fields are present."""
        return self._inode_brs

    @inode_brs.setter
    def inode_brs(self, val):
        if not val is None:
            _typecheck(val, ByteRuns)
        self._inode_brs = val

    @property
    def meta_type(self):
        return self._meta_type

    @meta_type.setter
    def meta_type(self, val):
        self._meta_type = _intcast(val)

    @property
    def mode(self):
        """The security mode is represented in the FileObject as a base-10 integer.  It is also serialized as a decimal integer."""
        return self._mode

    @mode.setter
    def mode(self, val):
        self._mode = _intcast(val)

    @property
    def mtime(self):
        return self._mtime

    @mtime.setter
    def mtime(self, val):
        if val is None:
            self._mtime = None
        elif isinstance(val, TimestampObject):
            self._mtime = val
        else:
            checked_val = TimestampObject(val, name="mtime")
            self._mtime = checked_val

    @property
    def name_brs(self):
        """The byte run(s) that represents the file's name object (the directory entry).  In file systems that do not distinguish between inode and directory entry, e.g. FAT, .inode_brs should be equivalent to .name_brs, if both fields are present."""
        return self._name_brs

    @name_brs.setter
    def name_brs(self, val):
        if not val is None:
            _typecheck(val, ByteRuns)
        self._name_brs = val

    @property
    def name_type(self):
        return self._name_type

    @name_type.setter
    def name_type(self, val):
        if val is None:
            self._name_type = val
        else:
            cast_val = _strcast(val)
            if cast_val not in ["-", "p", "c", "d", "b", "r", "l", "s", "h", "w", "v"]:
                raise ValueError("Unexpected name_type received: %r (casted to %r)." % (val, cast_val))
            self._name_type = cast_val

    @property
    def nlink(self):
        return self._nlink

    @nlink.setter
    def nlink(self, val):
        self._nlink = _intcast(val)

    @property
    def orphan(self):
        return self._orphan

    @orphan.setter
    def orphan(self, val):
        self._orphan = _boolcast(val)

    @property
    def original_fileobject(self):
        return self._original_fileobject

    @original_fileobject.setter
    def original_fileobject(self, val):
        if not val is None:
            _typecheck(val, FileObject)
        self._original_fileobject = val

    @property
    def partition(self):
        return self._partition

    @partition.setter
    def partition(self, val):
        self._partition = _intcast(val)

    @property
    def parent_object(self):
        """This object is an extremely sparse FileObject, containing just identifying information.  Alternately, it can be an entire object reference to the parent Object, though uniqueness should be checked."""
        return self._parent_object

    @parent_object.setter
    def parent_object(self, val):
        if not val is None:
            _typecheck(val, FileObject)
        self._parent_object = val

    @property
    def seq(self):
        return self._seq

    @seq.setter
    def seq(self, val):
        self._seq = _intcast(val)

    @property
    def uid(self):
        return self._uid

    @uid.setter
    def uid(self, val):
        self._uid = _strcast(val)

    @property
    def unalloc(self):
        """Note that setting .unalloc will affect the value of .alloc, and vice versa.  The last one to set wins."""
        return self._unalloc

    @unalloc.setter
    def unalloc(self, val):
        self._unalloc = _boolcast(val)
        if not self._unalloc is None:
            self._alloc = not self._unalloc

    @property
    def unused(self):
        return self._used

    @unused.setter
    def unused(self, val):
        self._unused = _intcast(val)
        if not self._unused is None:
            self._used = not self._unused

    @property
    def used(self):
        return self._used

    @used.setter
    def used(self, val):
        self._used = _intcast(val)
        if not self._used is None:
            self._unused = not self._used

    @property
    def volume_object(self):
        """Reference to the containing volume object.  Not meant to be propagated with __repr__ or to_Element()."""
        return self._volume_object

    @volume_object.setter
    def volume_object(self, val):
        if not val is None:
            _typecheck(val, VolumeObject)
        self._volume_object = val


class CellObject(object):

    _all_properties = set([
      "alloc",
      "annos",
      "byte_runs",
      "cellpath",
      "mtime",
      "name",
      "name_type",
      "original_cellobject",
      "parent_object",
      "root"
    ])

    _diff_attr_names = {
      "new":"delta:new_cell",
      "deleted":"delta:deleted_cell",
      "changed":"delta:changed_cell",
      "modified":"delta:modified_cell",
      "matched":"delta:matched"
    }

    #TODO There may be need in the future to compare the annotations as well.
    _incomparable_properties = set([
      "annos"
    ])

    def __init__(self, *args, **kwargs):
        #These properties must be assigned first for sanity check dependencies
        self.name_type = kwargs.get("name_type")

        for prop in CellObject._all_properties:
            if prop == "annos":
                setattr(self, prop, kwargs.get(prop, set()))
            else:
                setattr(self, prop, kwargs.get(prop))

        self._diffs = set()

    def __eq__(self, other):
        if other is None:
            return False
        _typecheck(other, CellObject)
        for prop in CellObject._all_properties:
            if prop in CellObject._incomparable_properties:
                continue
            if getattr(self, prop) != getattr(other, prop):
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        parts = []

        for prop in sorted(list(CellObject._all_properties)):
            if not getattr(self, prop) is None:
                parts.append("%s=%r" % (prop, getattr(self, prop)))

        return "CellObject(" + ", ".join(parts) + ")"

    def compare_to_original(self):
        self._diffs = self.compare_to_other(self.original_cellobject, True)

    def compare_to_other(self, other, ignore_original=False):
        _typecheck(other, CellObject)

        diffs = set()

        for propname in CellObject._all_properties:
            if propname in CellObject._incomparable_properties:
                continue
            if ignore_original and propname == "original_cellobject":
                continue
            oval = getattr(other, propname)
            sval = getattr(self, propname)
            if oval is None and sval is None:
                continue
            if oval != sval:
                #_logger.debug("propname, oval, sval: %r, %r, %r" % (propname, oval, sval))
                diffs.add(propname)

        return diffs

    def populate_from_Element(self, e):
        """Populates this CellObject's properties from an ElementTree Element.  The Element need not be retained."""
        global _warned_elements
        _typecheck(e, (ET.Element, ET.ElementTree))

        _read_differential_annotations(CellObject._diff_attr_names, e, self.annos)

        #Split into namespace and tagname
        (ns, tn) = _qsplit(e.tag)
        assert tn in ["cellobject", "original_cellobject", "parent_object"]

        if e.attrib.get("root"):
            self.root = e.attrib["root"]

        #Look through direct-child elements for other properties
        for ce in e.findall("./*"):
            (cns, ctn) = _qsplit(ce.tag)
            if ctn == "alloc":
                self.alloc = ce.text
            elif ctn == "byte_runs":
                self.byte_runs = ByteRuns()
                self.byte_runs.populate_from_Element(ce)
            elif ctn == "cellpath":
                self.cellpath = ce.text
            elif ctn == "mtime":
                self.mtime = TimestampObject()
                self.mtime.populate_from_Element(ce)
            elif ctn == "name":
                self.name = ce.text
            elif ctn == "name_type":
                self.name_type = ce.text
            elif ctn == "original_cellobject":
                self.original_cellobject = CellObject()
                self.original_cellobject.populate_from_Element(ce)
            elif ctn == "parent_object":
                self.parent_object = CellObject()
                self.parent_object.populate_from_Element(ce)
            else:
                if (cns, ctn) not in _warned_elements:
                    _warned_elements.add((cns, ctn))
                    _logger.warning("Uncertain what to do with this element: %r" % ce)

        self.sanity_check()

    def sanity_check(self):
        if self.name_type and self.name_type != "k":
            if self.mtime:
                _logger.info("Error occurred sanity-checking this CellObject: %r." % self)
                raise ValueError("A Registry Key (node) is the only kind of CellObject that can have a timestamp.")
            if self.root:
                _logger.info("Error occurred sanity-checking this CellObject: %r." % self)
                raise ValueError("A Registry Key (node) is the only kind of CellObject that can have the 'root' attribute.")

    def to_Element(self):
        self.sanity_check()

        outel = ET.Element("cellobject")

        annos_whittle_set = copy.deepcopy(self.annos)
        diffs_whittle_set = copy.deepcopy(self.diffs)

        for annodiff in CellObject._diff_attr_names:
            if annodiff in annos_whittle_set:
                outel.attrib[CellObject._diff_attr_names[annodiff]] = "1"
                annos_whittle_set.remove(annodiff)
        if len(annos_whittle_set) > 0:
            _logger.warning("Failed to export some differential annotations: %r." % annos_whittle_set)

        def _anno_change(el):
            if el.tag in self.diffs:
                el.attrib["delta:changed_property"] = "1"
                diffs_whittle_set.remove(el.tag)

        #Recall that Element text must be a string
        def _append_str(name, value):
            if not value is None or name in diffs_whittle_set:
                tmpel = ET.Element(name)
                if not value is None:
                    tmpel.text = str(value)
                _anno_change(tmpel)
                outel.append(tmpel)

        def _append_object(name, value):
            if not value is None or name in diffs_whittle_set:
                if value is None:
                    tmpel = ET.Element(name)
                else:
                    tmpel = value.to_Element()
                _anno_change(tmpel)
                outel.append(tmpel)

        #TODO root should be an element too.  Revise schema.
        if self.root:
            outel.attrib["root"] = str(self.root)

        _append_str("cellpath", self.cellpath)
        _append_str("name", self.name)
        _append_str("name_type", self.name_type)
        _append_str("alloc", self.alloc)
        _append_object("mtime", self.mtime)
        _append_object("byte_runs", self.byte_runs)
        _append_object("original_cellobject", self.original_cellobject)

        if len(diffs_whittle_set) > 0:
            _logger.warning("Did not annotate all of the differing properties of this file.  Remaining properties:  %r." % diffs_whittle_set)

        return outel

    def to_regxml(self):
        return _ET_tostring(self.to_Element())

    @property
    def alloc(self):
        return self._alloc

    @alloc.setter
    def alloc(self, val):
        self._alloc = _boolcast(val)

    @property
    def annos(self):
        """Set of differential annotations.  Expected members are the keys of this class's _diff_attr_names dictionary."""
        return self._annos

    @annos.setter
    def annos(self, val):
        _typecheck(val, set)
        self._annos = val

    @property
    def byte_runs(self):
        return self._byte_runs

    @byte_runs.setter
    def byte_runs(self, val):
        if not val is None:
            _typecheck(val, ByteRuns)
        self._byte_runs = val

    @property
    def cellpath(self):
        return self._cellpath

    @cellpath.setter
    def cellpath(self, val):
        if not val is None:
            _typecheck(val, str)
        self._cellpath = val

    @property
    def diffs(self):
        return self._diffs

    @diffs.setter
    def diffs(self, value):
        _typecheck(value, set)
        self._diffs = value

    @property
    def mtime(self):
        return self._mtime

    @mtime.setter
    def mtime(self, val):
        if val is None:
            self._mtime = None
        elif isinstance(val, TimestampObject):
            self._mtime = val
        else:
            self._mtime = TimestampObject(val, name="mtime")
            self.sanity_check()

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, val):
        if not val is None:
            _typecheck(val, str)
        self._name = val

    @property
    def name_type(self):
        return self._name_type

    @name_type.setter
    def name_type(self, val):
        if not val is None:
            assert val in ["k", "v"]
        self._name_type = val

    @property
    def original_cellobject(self):
        return self._original_cellobject

    @original_cellobject.setter
    def original_cellobject(self, val):
        if not val is None:
            _typecheck(val, CellObject)
        self._original_cellobject = val

    @property
    def parent_object(self):
        """This object is an extremely sparse CellObject, containing just identifying information.  Alternately, it can be an entire object reference to the parent Object, though uniqueness should be checked."""
        return self._parent_object

    @parent_object.setter
    def parent_object(self, val):
        if not val is None:
            _typecheck(val, CellObject)
        self._parent_object = val

    @property
    def root(self):
        return self._root

    @root.setter
    def root(self, val):
        self._root = _boolcast(val)


def iterparse(filename, events=("start","end"), dfxmlobject=None):
    """
    Generator.  Yields a stream of populated DFXMLObjects, VolumeObjects and FileObjects, paired with an event type ("start" or "end").  The DFXMLObject and VolumeObjects do NOT have their child lists populated with this method - that is left to the calling program.
    The event type interface is meant to match the interface of ElementTree's iterparse; this is simply for familiarity's sake.  DFXMLObjects and VolumeObjects are yielded with "start" when the stream of VolumeObject or FileObjects begins - that is, they are yielded after being fully constructed up to the potentially-lengthy child object stream.  FileObjects are yielded only with "end".
    @param filename: A string
    @param events: Events.  Optional.  A tuple of strings, containing "start" and/or "end".
    @param dfxmlobject: A DFXMLObject document.  Optional.  A DFXMLObject is created and yielded in the object stream if this argument is not supplied.
    """

    #The DFXML stream file handle.
    fh = None
    subp = None
    subp_command = ["fiwalk", "-x", filename]
    if filename.endswith("xml"):
        fh = open(filename, "rb")
    else:
        subp = subprocess.Popen(subp_command, stdout=subprocess.PIPE)
        fh = subp.stdout

    _events = set()
    for e in events:
        if not e in ("start","end"):
            raise ValueError("Unexpected event type: %r.  Expecting 'start', 'end'." % e)
        _events.add(e)

    dobj = dfxmlobject or DFXMLObject()

    #The only way to efficiently populate VolumeObjects is to populate the object when the stream has hit its first FileObject.
    vobj = None

    #It doesn't seem ElementTree allows fetching parents of Elements that are incomplete (just hit the "start" event).  So, build a volume Element when we've hit "<volume ... >", glomming all elements until the first fileobject is hit.
    #Likewise with the Element for the DFXMLObject.
    dfxml_proxy = None
    volume_proxy = None

    #State machine, used to track when the first fileobject of a volume is encountered.
    READING_START = 0
    READING_PRESTREAM = 1 #DFXML metadata, pre-Object stream
    READING_VOLUMES = 2
    READING_FILES = 3
    READING_POSTSTREAM = 4 #DFXML metadata, post-Object stream (typically the <rusage> element)
    _state = READING_START

    for (ETevent, elem) in ET.iterparse(fh, events=("start-ns", "start", "end")):
        #View the object event stream in debug mode
        #_logger.debug("(event, elem) = (%r, %r)" % (ETevent, elem))
        #if ETevent in ("start", "end"):
        #    _logger.debug("_ET_tostring(elem) = %r" % _ET_tostring(elem))

        #Track namespaces
        if ETevent == "start-ns":
            dobj.add_namespace(*elem)
            continue

        #Split tag name into namespace and local name
        (ns, ln) = _qsplit(elem.tag)

        if ETevent == "start":
            if ln == "dfxml":
                if _state != READING_START:
                    raise ValueError("Encountered a <dfxml> element, but the parser isn't in its start state.  Recursive <dfxml> declarations aren't supported at this time.")
                dfxml_proxy = ET.Element(elem.tag)
                for k in elem.attrib:
                    #Note that xmlns declarations don't appear in elem.attrib.
                    dfxml_proxy.attrib[k] = elem.attrib[k] 
                _state = READING_PRESTREAM
            elif ln == "volume":
                if _state == READING_PRESTREAM:
                    #Cut; yield DFXMLObject now.
                    dobj.populate_from_Element(dfxml_proxy)
                    if "start" in _events:
                        yield ("start", dobj)
                #Start populating a new Volume proxy.
                volume_proxy = ET.Element(elem.tag)
                for k in elem.attrib:
                    volume_proxy.attrib[k] = elem.attrib[k] 
                _state = READING_VOLUMES
            elif ln == "fileobject":
                if _state == READING_PRESTREAM:
                    #Cut; yield DFXMLObject now.
                    dobj.populate_from_Element(dfxml_proxy)
                    if "start" in _events:
                        yield ("start", dobj)
                elif _state == READING_VOLUMES:
                    #_logger.debug("Encountered a fileobject while reading volume properties.  Yielding volume now.")
                    #Cut; yield VolumeObject now.
                    if volume_proxy is not None:
                        vobj = VolumeObject()
                        vobj.populate_from_Element(volume_proxy)
                        if "start" in _events:
                            yield ("start", vobj)
                        #Reset
                        volume_proxy.clear()
                        volume_proxy = None
                _state = READING_FILES
        elif ETevent == "end":
            if ln == "fileobject":
                if _state in (READING_PRESTREAM, READING_POSTSTREAM):
                    #This particular branch can be reached if there are trailing fileobject elements after the volume element.  This would happen if a tool needed to represent files (likely reassembled fragments) found outside all the partitions.
                    #More frequently, we hit this point when there are no volume groupings.
                    vobj = None
                fi = FileObject()
                fi.populate_from_Element(elem)
                fi.volume_object = vobj
                #_logger.debug("fi = %r" % fi)
                if "end" in _events:
                    yield ("end", fi)
                #Reset
                elem.clear()
            elif elem.tag == "dfxml":
                if "end" in _events:
                    yield ("end", dobj)
            elif elem.tag == "volume":
                if "end" in _events:
                    yield ("end", vobj)
                _state = READING_POSTSTREAM
            elif _state == READING_VOLUMES:
                #This is a volume property; glom onto the proxy.
                if volume_proxy is not None:
                    volume_proxy.append(elem)
            elif _state == READING_PRESTREAM:
                if ln in ["metadata", "creator", "source"]:
                    #This is a direct child of the DFXML document property; glom onto the proxy.
                    if dfxml_proxy is not None:
                        dfxml_proxy.append(elem)

    #If we called Fiwalk, double-check that it exited successfully.
    if not subp is None:
        _logger.debug("Calling wait() to let the Fiwalk subprocess terminate...") #Just reading from subp.stdout doesn't let the process terminate; it only finishes working.
        subp.wait()
        if subp.returncode != 0:
            e = subprocess.CalledProcessError("There was an error running Fiwalk.")
            e.returncode = subp.returncode
            e.cmd = subp_command
            raise e
        _logger.debug("...Done.")

def parse(filename):
    """Returns a DFXMLObject populated from the contents of the (string) filename argument."""
    retval = None
    appender = None
    for (event, obj) in iterparse(filename):
        if event == "start":
            if isinstance(obj, DFXMLObject):
                retval = obj
                appender = obj
            elif isinstance(obj, VolumeObject):
                retval.append(obj)
                appender = obj
        elif event == "end":
            if isinstance(obj, VolumeObject):
                appender = retval
            elif isinstance(obj, FileObject):
                appender.append(obj)
    return retval

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    
    logging.basicConfig(level=logging.DEBUG)
    #Run unit tests

    assert _intcast(-1) == -1
    assert _intcast("-1") == -1
    assert _qsplit("{http://www.w3.org/2001/XMLSchema}all") == ("http://www.w3.org/2001/XMLSchema","all")
    assert _qsplit("http://www.w3.org/2001/XMLSchema}all") == (None, "http://www.w3.org/2001/XMLSchema}all")


    fi = FileObject()

    #Check property setting
    fi.mtime = "1999-12-31T23:59:59Z"
    _logger.debug("fi = %r" % fi)

    #Check bad property setting
    failed = None
    try:
        fi.mtime = "Not a timestamp"
        failed = False
    except:
        failed = True
    _logger.debug("fi = %r" % fi)
    _logger.debug("failed = %r" % failed)
    assert failed

    t0 = TimestampObject(prec="100ns", name="mtime")
    _logger.debug("t0 = %r" % t0)
    assert t0.prec[0] == 100
    assert t0.prec[1] == "ns"
    t1 = TimestampObject("2009-01-23T01:23:45Z", prec="2", name="atime")
    _logger.debug("t1 = %r" % t1)
    assert t1.prec[0] == 2
    assert t1.prec[1] == "s"

    print("Unit tests passed.")