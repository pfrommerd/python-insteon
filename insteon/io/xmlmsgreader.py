from xml.dom import minidom

from .message import Direction,DataType
from .message_def import FieldDef,MsgDef

def read_default_xml():
    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        import pkg_resources
        filepath = pkg_resources.resource_filename(__name__,'msg_definitions.xml')
        if not filepath:
            return None
        return read_xml(filepath)

def read_xml(filename):
    xmldoc = minidom.parse(filename)
    return process_xmltree(xmldoc)

# Returns a map of name to msgdef
def process_xmltree(xmldoc):
    results = {}

    elems = xmldoc.getElementsByTagName('msg')
    for m in elems:
        d = process_msgdef_elem(m)
        results[d.name] = d

    return results

# Returns a message
def process_msgdef_elem(elem):
    msg_name = elem.attributes['name'].value
    msg_length = int(elem.attributes['length'].value)
    msg_direction = Direction.FROM_MODEM \
                        if elem.attributes['direction'].value == 'FROM_MODEM' else \
                            Direction.TO_MODEM
    msg_def = MsgDef(msg_name, msg_direction)

    headers = elem.getElementsByTagName('header')
    if len(headers) > 1:
        raise ValueError('No header for message!')
    header = headers[0]
    for n in filter(lambda x: isinstance(x,minidom.Element),
                    header.childNodes):
        process_field_elem(n, msg_def)

    # Set the header length
    msg_def.header_length = msg_def.length

    for n in filter(
            lambda x: isinstance(x,minidom.Element) and x.tagName != 'header',
            elem.childNodes):
        process_field_elem(n, msg_def)

    if msg_length != msg_def.length:
        raise ValueError('Msg length and configured length not the same: {} vs {}'.format(msg_length,msg_def.length))
        
    return msg_def

def process_field_elem(field_elem, msgdef):
    name = None
    value = None
    filter_ = None

    if 'name' in field_elem.attributes:
        name = field_elem.attributes['name'].value

    if field_elem.firstChild and field_elem.firstChild.nodeValue:
        value = bytes.fromhex(field_elem.firstChild.nodeValue[2:])[0]

    if 'filter' in field_elem.attributes:
        filter_type = field_elem.attributes['filter'].value
        if filter_type == 'equals_default':
            filter_ = lambda x: x == value
        elif filter_type.startswith('bitset'):
            bit = int(filter_type.split(':')[1])
            filter_ = lambda x: x & (1 << bit) > 0
        elif filter_type.startswith('bitunset'):
            bit = int(filter_type.split(':')[1])
            filter_ = lambda x: x & (1 << bit) == 0

    typename = field_elem.tagName
    if typename == "byte":
        msgdef.append(FieldDef(msgdef.length, 1, DataType.BYTE,
                                name, value, filter_))
    elif typename == "int":
        msgdef.append(FieldDef(msgdef.length, 4, DataType.INT,
                                name, value, filter_))
    elif typename == "float":
        msgdef.append(FieldDef(msgdef.length, 4, DataType.FLOAT,
                                name, value, filter_))
    elif typename == "address":
        msgdef.append(FieldDef(msgdef.length, 3, DataType.ADDRESS,
                                name, value, filter_))
    else:
        raise ValueError('Unrecognized field type {}'.format(typename))
