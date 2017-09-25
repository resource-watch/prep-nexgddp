"""XML SERVICE"""

import logging
from nexgddp.errors import XMLParserError
import xml.etree.ElementTree as ET


class XMLService(object):
    """."""

    @staticmethod
    def get_fields(xml):
        logging.info('[XMLService] Parsing XML fields')
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
                                        'type': 'number',
                                        'uom': field_spec_uom.get('code')
                                    }
            fields['year'] = {'type': 'date'}
        except Exception as e:
            raise XMLParserError(message=str(e))
        return fields

    @staticmethod
    def get_domain(xml):
        logging.info('[XMLService] Parsing XML domain info')
        domain = {}
        try:
            root = ET.fromstring(xml)
            for cd in root.findall('{http://www.opengis.net/wcs/2.0}CoverageDescription'):
                for bb in cd.findall('{http://www.opengis.net/gml/3.2}boundedBy'):
                    for envelope in bb:
                        for corner in envelope:
                            domain_tag = corner.tag.rsplit('}')[-1]
                            domain_attributes = corner.text.replace('"', '').rsplit(' ')
                            domain[domain_tag] = domain_attributes

        except Exception as e:
            raise XMLParserError(message=str(e))
        return domain
