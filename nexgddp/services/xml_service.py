"""XML SERVICE"""

import logging
from nexgddp.errors import XMLParserError
import xml.etree.ElementTree as ET


class XMLService(object):
    """."""

    @staticmethod
    def get_fields(xml):
        logging.info('Parsing XML fields')
        fields = {}
        try:
            root = ET.fromstring(xml)
            for cd in root.findall('{http://www.opengis.net/wcs/2.0}CoverageDescription'):
                for rt in cd.findall('{http://www.opengis.net/gmlcov/1.0}rangeType'):
                    for dr in rt.findall('{http://www.opengis.net/swe/2.0}DataRecord'):
                        for field in dr.findall('{http://www.opengis.net/swe/2.0}field'):
                            field_name = field.get('name')
                            for field_spec in field.findall('{http://www.opengis.net/swe/2.0}Quantity'):
                                for field_spec_uom in field_spec.findall('{http://www.opengis.net/swe/2.0}uom'):
                                    fields[field_name] = {
                                        'type': 'Quantity',
                                        'uom': field_spec_uom.get('code')
                                    }
        except Exception as e:
            raise XMLParserError(message=str(e))
        return fields
