from __future__ import absolute_import
from __future__ import division

# patches unittest.TestCase to be python3 compatible
import logging
import unittest

from deprecated import accela_dataflow
from deprecated.dataflow_utils import sort_dict


class TestAccelaDataflow(unittest.TestCase):
    opening_permit = {
                "parcels": [{
                    "parcelNumber": "0076L00128000000",
                    "councilDistrict": "1",
                    "id": "0076L00128000000",
                    "isPrimary": "N",
                    "landValue": 0.0,
                    "improvedValue": 0.0,
                    "gisSequenceNumber": 0,
                    "exemptionValue": 0.0,
                    "parcelArea": 0.0,
                    "parcel": "0076L0012",
                    "status": {
                        "value": "A",
                        "text": "Active"
                    }
                }],
                "addresses": [{
                    "streetStart": 901,
                    "id": 1010909872,
                    "refAddressId": 929549287,
                    "serviceProviderCode": "PITTSBURGH_PA",
                    "city": "PITTSBURGH",
                    "postalCode": "15212",
                    "streetAddress": "901 GRAND AVE",
                    "isPrimary": "Y",
                    "streetName": "GRAND",
                    "county": "ALLEGHENY",
                    "recordId": {
                        "id": "PITTSBURGH_PA-20CAP-00000-0054M",
                        "trackingId": 0,
                        "serviceProviderCode": "PITTSBURGH_PA",
                        "value": "20CAP-00000-0054M"
                    },
                    "status": {
                        "value": "A",
                        "text": "Active"
                    },
                    "state": {
                        "value": "PA",
                        "text": "PA"
                    },
                    "streetSuffix": {
                        "value": "AVE",
                        "text": "AVE"
                    }
                }],
                "customForms": [{
                    "0-20 LIN FT": None,
                    "Incomplete 30+ Days": None,
                    "Bond Number": None,
                    "51-100 SQ YDS": None,
                    "LIN FT - IF OVER 20+": None,
                    "Days - 15 Day Increment": "30",
                    "id": "DPW_OPEN-APPLICATION.cREQUIREMENTS",
                    "SQ YDS - IF OVER 100+": None,
                    "4-50 SQ YDS": "2",
                    "100+ SQ YDS": None,
                    "1-3 SQ YDS": None,
                    "20+ LIN FT": None
                }, {
                    "Valid Date (From)": "2020-05-04",
                    "To 3": None,
                    "To 2": None,
                    "From": "HENLEY ST",
                    "Location 3": None,
                    "From 3": None,
                    "From 2": None,
                    "Restoration Date": "2020-07-03",
                    "Issue Date": "2020-04-30",
                    "To": "ELRENO ST",
                    "id": "DPW_OPEN-PERMIT.cINFORMATION",
                    "Valid Date (To)": "2020-06-03",
                    "Location": "901 & 903  GRAND AVE",
                    "Location 2": None
                }, {
                    "Fee Waive Authority": None,
                    "Fees Waived?": "No",
                    "id": "DPW_OPEN-FEE.cINFORMATION",
                    "Reason": None
                }],
                "professionals": [{
                    "id": "UT011-UTILITY",
                    "fullName": "  ",
                    "city": "PITTSBURGH",
                    "postalCode": "15221",
                    "licenseNumber": "UT011",
                    "referenceLicenseId": "13699",
                    "isPrimary": "Y",
                    "updateOnUI": False,
                    "phone1": "412-244-2535",
                    "addressLine1": "1201 PITT STREET",
                    "businessName": "PEOPLES NATURAL GAS COMPANY",
                    "recordId": {
                        "id": "PITTSBURGH_PA-20CAP-00000-0054M",
                        "trackingId": 0,
                        "serviceProviderCode": "PITTSBURGH_PA",
                        "value": "20CAP-00000-0054M"
                    },
                    "licenseType": {
                        "value": "UTILITY",
                        "text": "UTILITY"
                    },
                    "state": {
                        "value": "PA",
                        "text": "PA"
                    }
                }],
                "contacts": [{
                    "id": "9013208051",
                    "isPrimary": "Y",
                    "phone1": "412-244-2535",
                    "startDate": "2020-04-30 09:17:09",
                    "organizationName": "PEOPLES NATURAL GAS COMPANY",
                    "recordId": {
                        "id": "PITTSBURGH_PA-20CAP-00000-0054M",
                        "trackingId": 0,
                        "serviceProviderCode": "PITTSBURGH_PA",
                        "value": "20CAP-00000-0054M"
                    },
                    "status": {
                        "value": "A",
                        "text": "Active"
                    },
                    "type": {
                        "value": "Applicant",
                        "text": "Applicant"
                    },
                    "address": {
                        "city": "PITTSBURGH",
                        "postalCode": "15221",
                        "addressLine1": "1201 PITT STREET",
                        "state": {
                            "value": "PA",
                            "text": "PA"
                        }
                    }
                }],
                "id": "PITTSBURGH_PA-20CAP-00000-0054M",
                "type": {
                    "module": "PublicWorks",
                    "value": "PublicWorks/Opening/NA/NA",
                    "type": "Opening",
                    "group": "PublicWorks",
                    "category": "NA",
                    "subType": "NA",
                    "alias": "Opening Permit",
                    "text": "Opening Permit",
                    "id": "PublicWorks-Opening-NA-NA"
                },
                "description": "NEW TAP (10076261)  #20200772829 STREET 2-( 6 FT X 8 FT)  (901 & 903 GRAND AVE)",
                "module": "PublicWorks",
                "status": {
                    "value": "Closed",
                    "text": "Closed"
                },
                "statusDate": "2020-10-29 00:00:00",
                "recordClass": "COMPLETE",
                "initiatedProduct": "AV360",
                "serviceProviderCode": "PITTSBURGH_PA",
                "closedDate": "2020-10-29 00:00:00",
                "reportedDate": "2020-04-30 00:00:00",
                "createdBy": "PRIMART",
                "updateDate": "2020-10-29 07:20:29",
                "totalJobCost": 0.0,
                "customId": "DPW2006625",
                "reportedChannel": {
                    "value": "Call Center",
                    "text": "Call Center"
                },
                "undistributedCost": 0.0,
                "closedByDepartment": "PITTSBURGH_PA/PW/INSP/NA/NA/NA/NA",
                "actualProductionUnit": 0.0,
                "estimatedProductionUnit": 0.0,
                "openedDate": "2020-04-30 00:00:00",
                "jobValue": 0.0,
                "trackingId": 208647127679,
                "closedByUser": "BOTTLED",
                "value": "20CAP-00000-0054M",
                "totalFee": 640.0,
                "totalPay": 640.0,
                "balance": 0.0,
                "booking": False,
                "infraction": False,
                "misdemeanor": False,
                "offenseWitnessed": False,
                "defendantSignature": False,
                "publicOwned": False
            }

    obstruction_permit = {
                            "parcels": [{
                                "parcelNumber": "0029N00305000000",
                                "exemptionValue": 0.0,
                                "gisSequenceNumber": 0,
                                "parcel": "0029N0030",
                                "parcelArea": 0.0,
                                "improvedValue": 0.0,
                                "landValue": 0.0,
                                "councilDistrict": "3",
                                "isPrimary": "N",
                                "id": "0029N00305000000",
                                "status": {
                                    "text": "Inactive"
                                }
                            }],
                            "contacts": [{
                                "phone2": "412-292-665",
                                "phone1": "412-255-5400",
                                "firstName": "ADAM",
                                "lastName": "HINDS",
                                "organizationName": "TURNER CONSTRUCTION",
                                "isPrimary": "Y",
                                "id": "9010009436",
                                "recordId": {
                                    "id": "PITTSBURGH_PA-20CAP-00000-003DS",
                                    "trackingId": 0,
                                    "serviceProviderCode": "PITTSBURGH_PA",
                                    "value": "20CAP-00000-003DS"
                                },
                                "status": {
                                    "value": "A",
                                    "text": "Active"
                                },
                                "type": {
                                    "value": "Applicant",
                                    "text": "Applicant"
                                },
                                "address": {
                                    "postalCode": "15222",
                                    "city": "PITTSBURGH",
                                    "addressLine1": "925 LIBERTY AVE",
                                    "state": {
                                        "value": "PA",
                                        "text": "PA"
                                    }
                                }
                            }],
                            "addresses": [{
                                "refAddressId": 929561819,
                                "streetStart": 3025,
                                "id": 1010651228,
                                "postalCode": "15203",
                                "city": "PITTSBURGH",
                                "isPrimary": "Y",
                                "serviceProviderCode": "PITTSBURGH_PA",
                                "streetAddress": "3025 E CARSON ST",
                                "county": "ALLEGHENY",
                                "streetPrefix": "E",
                                "streetName": "CARSON",
                                "recordId": {
                                    "id": "PITTSBURGH_PA-20CAP-00000-003DS",
                                    "trackingId": 0,
                                    "serviceProviderCode": "PITTSBURGH_PA",
                                    "value": "20CAP-00000-003DS"
                                },
                                "status": {
                                    "value": "A",
                                    "text": "Active"
                                },
                                "state": {
                                    "value": "PA",
                                    "text": "PA"
                                },
                                "direction": {
                                    "value": "E",
                                    "text": "E"
                                },
                                "streetSuffix": {
                                    "value": "ST",
                                    "text": "ST"
                                }
                            }],
                            "customForms": [{
                                "Date (To)": "2020-12-31",
                                "Hours (Monday-Friday)": "CONTINUOUS",
                                "Hours (Saturday-Sunday-Holidays)": "CONTINUOUS",
                                "id": "DPW_OBSTR-APPLICATION.cREQUIREMENTS",
                                "Date (From)": "2020-03-01"
                            }, {
                                "From 2": None,
                                "Valid Date (From)": None,
                                "Comment 2": None,
                                "Comment 1": None,
                                "From 1": None,
                                "Issue Date": "2020-02-28",
                                "To 2": None,
                                "To 1": None,
                                "id": "DPW_OBSTR-PERMIT.cINFORMATION",
                                "Valid Date (To)": None,
                                "Location 1": None,
                                "Location 2": None
                            }, {
                                "Coordinate with other area construction or activities": "CHECKED",
                                "Maintain a minimum of one lane at all times": None,
                                "Flagperson required during working hours to assist with traffic control": None,
                                "Maintain access for emergency vehicles and local traffic as needed": None,
                                "Contact affected residents / businesses / property owners": "CHECKED",
                                "Traffic control in accordance with PennDOT Publication 212 and 213 standard plans": "CHECKED",
                                "Backfill or plate all excavations to traffic-worthy conditions during non-working hours": None,
                                "Maintain a clear and safe pedestrian path around work site at all times": "CHECKED",
                                "Maintain a minimum of one lane in each direction at all times": "CHECKED",
                                "Traffic control per attached plan": None,
                                "Traffic control in accordance with project plans and specifications": None,
                                "Hard road closure": None,
                                "Notify PAT operation at 412.566.5321 at least 48 hours in advance": None,
                                "Post \"No Parking\" signs in advance as needed": "CHECKED",
                                "Off-duty police officer required during working hours to assist with traffic control": None,
                                "id": "DPW_OBSTR-CONDITIONS",
                                "Other.": "WHEN IMPLEMENTING CURB CUT - WORK HOURS 9AM TO 3PM",
                                "Other": None
                            }],
                            "customTables": [{
                                "id": "DPW_TRAFFIC-STREET.cCLOSURE",
                                "rows": [{
                                    "fields": {
                                        "Comment": None,
                                        "Street": "3025 E CARSON ST",
                                        "From": "HOT METAL ST",
                                        "To": "SARAH ST"
                                    },
                                    "id": "1"
                                }]
                            }],
                            "id": "PITTSBURGH_PA-20CAP-00000-003DS",
                            "type": {
                                "module": "PublicWorks",
                                "value": "PublicWorks/Traffic Obstruction/NA/NA",
                                "type": "Traffic Obstruction",
                                "text": "Traffic Obstruction Permit",
                                "subType": "NA",
                                "category": "NA",
                                "group": "PublicWorks",
                                "alias": "Traffic Obstruction Permit",
                                "id": "PublicWorks-Traffic.cObstruction-NA-NA"
                            },
                            "description": "TEMPORARY CURB CUT DURING CONSTRUCTION -- REPLACE BACK TO EXISTING AFTER PROJECT SI FINISHED",
                            "module": "PublicWorks",
                            "totalJobCost": 0.0,
                            "initiatedProduct": "AV360",
                            "reportedDate": "2020-02-28 00:00:00",
                            "createdBy": "ROBIND",
                            "undistributedCost": 0.0,
                            "statusDate": "2020-02-28 07:36:20",
                            "recordClass": "COMPLETE",
                            "estimatedProductionUnit": 0.0,
                            "actualProductionUnit": 0.0,
                            "serviceProviderCode": "PITTSBURGH_PA",
                            "customId": "DPW2004367",
                            "updateDate": "2020-10-28 22:09:30",
                            "reportedChannel": {
                                "value": "Call Center",
                                "text": "Call Center"
                            },
                            "openedDate": "2020-02-28 00:00:00",
                            "trackingId": 208696034136,
                            "jobValue": 0.0,
                            "value": "20CAP-00000-003DS",
                            "totalFee": 0.0,
                            "totalPay": 0.0,
                            "balance": 0.0,
                            "booking": False,
                            "infraction": False,
                            "misdemeanor": False,
                            "offenseWitnessed": False,
                            "defendantSignature": False,
                            "publicOwned": False
                        }

    exclude_fields = [
        'module',
        'serviceProviderCode',
        'undistributedCost',
        'totalJobCost',
        'recordClass',
        'reportedChannel',
        'closedByDepartment',
        'estimatedProductionUnit',
        'actualProductionUnit',
        'createdByCloning',
        'closedByUser',
        'trackingId',
        'initiatedProduct',
        'createdBy',
        'value',
        'balance',
        'booking',
        'infraction',
        'misdemeanor',
        'offenseWitnessed',
        'defendantSignature',
        'parcels',
        'id',
        'statusDate',
        'jobValue',
        'reportedDate'
    ]

    ff = accela_dataflow.FilterFields(exclude_fields)
    pnf = accela_dataflow.ParseNestedFields()

    def test_parse_fields_opening_permit(self):
        expected = {
                    "customId": "DPW2006625",
                    "type": "Opening Permit",
                    "openedDate": "2020-04-30 00:00:00",
                    "closedDate": "2020-10-29 00:00:00",
                    "from_date": "2020-05-04",
                    "to_date": "2020-06-03",
                    "restoration_date": "2020-07-03",
                    "description": "NEW TAP (10076261)  #20200772829 STREET 2-( 6 FT X 8 FT)  (901 & 903 GRAND AVE)",
                    "address": "901 GRAND AVE PITTSBURGH PA 15212",
                    "street_or_location": "901 & 903  GRAND AVE",
                    "from_street": "HENLEY ST",
                    "to_street": "ELRENO ST",
                    "business_name": "PEOPLES NATURAL GAS COMPANY",
                    "license_type": "UTILITY",
                    "updateDate": "2020-10-29 07:20:29",
                    "totalFee": 640,
                    "totalPay": 640,
                    "publicOwned": False,
                    "status": "Closed"
                }
        filtered_opening_permit = next(self.ff.process(self.opening_permit))
        parsed = next(self.pnf.process(filtered_opening_permit))
        self.assertEqual(sort_dict(expected), sort_dict(parsed))

    # TODO: test obstruction permit, obscure real names in test

    def test_parse_fields_obstruction_permit(self):
        expected = {
                    "customId": "DPW2004367",
                    "type": "Traffic Obstruction Permit",
                    "openedDate": "2020-02-28 00:00:00",
                    "closedDate": None,
                    "from_date": "2020-03-01",
                    "to_date": "2020-12-31",
                    "restoration_date": None,
                    "description": "TEMPORARY CURB CUT DURING CONSTRUCTION -- REPLACE BACK TO EXISTING AFTER PROJECT "
                                   "SI FINISHED",
                    "address": "3025 E CARSON ST PITTSBURGH PA 15203",
                    "street_or_location": "3025 E CARSON ST",
                    "from_street": "HOT METAL ST",
                    "to_street": "SARAH ST",
                    "business_name": "TURNER CONSTRUCTION",
                    "license_type": None,
                    "updateDate": "2020-10-28 22:09:30",
                    "totalFee": 0.0,
                    "totalPay": 0.0,
                    "publicOwned": False,
                    "status": None
                  }
        filtered_obstruction_permit = next(self.ff.process(self.obstruction_permit))
        parsed = next(self.pnf.process(filtered_obstruction_permit))
        self.assertEqual(sort_dict(expected), sort_dict(parsed))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
