from __future__ import absolute_import
from __future__ import division
# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import
import unittest
import pandas as pd

from dataflow_utils import dataflow_utils


class TestDataflowUtils(unittest.TestCase):

    def test_columns_camel_to_snake_case(self):
        datum = {'exampleColumn': 'foo', 'anotherExample': 'bar'}
        expected = {'example_column': 'foo', 'another_example': 'bar'}
        # ccsc = ColumnsCamelToSnakeCase()
        ccsc = dataflow_utils.ColumnsCamelToSnakeCase()
        self.assertEqual(next(ccsc.process(datum)), expected)

    def test_columns_to_lower_case(self):
        datum = {'Example_Column': 'foo', 'anotherExample': 'bar', 'With a Space': 'foo'}
        expected = {'example_column': 'foo', 'anotherexample': 'bar', 'with a space': 'foo'}
        clc = dataflow_utils.ColumnsToLowerCase()
        self.assertEqual(next(clc.process(datum)), expected)

    def test_change_data_types(self):
        datum = {'count': '1', 'zip': 15213}
        type_changes = [("count", 'int'), ("zip", 'str')]
        expected = {'count': 1, 'zip': '15213'}
        cdt = dataflow_utils.ChangeDataTypes(type_changes)
        self.assertEqual(next(cdt.process(datum)), expected)

    def test_standardize_dep_names(self):
        datum = pd.DataFrame(['firdptmt', 'Unassigned', 'innovation & performance',
                              'parks & rec', 'policedpmt', 'CPRB', 'TBD', 'public safty'],
                             columns=['Department'])
        regex = [(r'(?i)^inno.*$', 'Innovation'),
                 (r'(?i)^TBD*$', 'Undetermined Dept/BRM'),
                 (r'(?i)^public saf.*$', 'Public Safety'),
                 (r'(?i)^Unassigned$', 'Undetermined Dept/BRM'),
                 (r'(?i)^park.*$', 'Parks'),
                 (r'(?i)^fir.*$', 'Fire'),
                 (r'(?i)^police.*(?<!board\.)$', 'Police'),
                 (r'(?i)^CPRB.*$', 'Police Review Board')]
        expected = pd.DataFrame(['Fire', 'Undetermined Dept/BRM', 'Innovation', 'Parks', 'Police',
                                 'Police Review Board', 'Undetermined Dept/BRM', 'Public Safety'],
                                columns=['Department'])
        sdn = dataflow_utils.StandardizeDepNames(regex)
        output = next(sdn.process(datum))
        self.assertTrue(expected.equals(output))

    def test_swap_field_names(self):
        datum = {'exampleColumn': 'foo', 'anotherExample': 'bar'}
        name_changes = [('exampleColumn', 'newExampleColumn'), ('anotherExample', 'newAnotherExample')]
        expected = {'newExampleColumn': 'foo', 'newAnotherExample': 'bar'}
        sfn = dataflow_utils.SwapFieldNames(name_changes)
        self.assertEqual(next(sfn.process(datum)), expected)

    def test_get_date_string(self):
        datum = {'unix_timestamp': 1602619169}
        date_column_names = [('unix_timestamp', 'string_timestamp')]
        expected = {'unix_timestamp': 1602619169, 'string_timestamp': '2020-10-13 15:59:29 EDT'}
        gds = dataflow_utils.GetDateStrings(date_column_names)
        self.assertEqual(next(gds.process(datum)), expected)

    def test_filter_fields(self):
        datum = {'city': 'pittsburgh', 'state': 'pa'}
        relevant_fields = 'state'
        expected = {'city': 'pittsburgh'}
        ff = dataflow_utils.FilterFields(relevant_fields)
        self.assertEqual(next(ff.process(datum)), expected)

    def test_filter_fields_exclude(self):
        datum = {'city': 'pittsburgh', 'state': 'pa'}
        relevant_fields = 'state'
        expected = {'state': 'pa'}
        ff = dataflow_utils.FilterFields(relevant_fields, exclude_relevant_fields=False)
        self.assertEqual(next(ff.process(datum)), expected)

    def test_anonymize_vip_names(self):
        providers = {
                    "Verizon": {
                                "assignedto": ["MARTY LAMAR", "CHRIS HORNSTEIN", "ANUPAMA JAIN"],
                                "assignedto_lastname": ["LAMAR", "HORNSTEIN", "JAIN"],
                                "wirelessnumber": ["412-506-2224", "412-123-7631", "412-636-8839"]
                    },
                    "Sprint": {
                               "assignedto": ["COUNCILMAN KRAUS"],
                               "assignedto_lastname": ["KRAUS"],
                               "wirelessnumber": ["412-937-1234"]
                    },
                    "AT&T": {
                             "assignedto": ["JANET MANUEL", "DARRYL JONES", "RONALD ROMANO"],
                             "assignedto_lastname": ["MANUEL", "JONES", "ROMANO"],
                             "wirelessnumber": ["412-182- 9184", "412-771-1833", "412-123-9903"]
                    }
        }

        result = {
                     "Verizon": {
                                 "assignedto": ['By Request', 'By Request', 'By Request'],
                                 "assignedto_lastname": ['By Request', 'By Request', 'By Request'],
                                 "wirelessnumber": ["xxx-xxx-xx24", "xxx-xxx-xx31", "xxx-xxx-xx39"]
                     },
                     "Sprint": {
                                 "assignedto": ["By Request"],
                                 "assignedto_lastname": ["By Request"],
                                 "wirelessnumber": ["xxx-xxx-xx34"]
                     },
                     "AT&T": {
                               "assignedto": ['By Request', 'By Request', 'By Request'],
                               "assignedto_lastname": ['By Request', 'By Request', 'By Request'],
                               "wirelessnumber": ["xxx-xxx-xx84", "xxx-xxx-xx33", "xxx-xxx-xx03"]
                     }
         }

        for provider in providers.keys():
            datum = pd.DataFrame(providers.get(provider, {}))
            expected = pd.DataFrame(result.get(provider, {}))

            vip = dataflow_utils.AnonymizeVIPNames(provider)
            output = next(vip.process(datum))
            output = output.drop(columns=["VIP"])
            self.assertTrue(expected.equals(output))
            

if __name__ == '__main__':
    unittest.main()
