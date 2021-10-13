import unittest
import datetime

from projecttemplate.infra import query_builder


class TestQueryBuilder(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.columns_to_select = [
            "P254G6_VEH_DC.CODE_VERSION",
            "DC_PIECES_RECHANGE.CODE_PR",
            "P254G6_VEH_DC.BOITE_DE_VITESSE",
        ]

        cls.today = datetime.date(2019, 2, 1)
        cls.nb_days = 2
        cls.from_table = "BRC07.P254G6_VEH_DC"
        cls.join_tables = {"BRC07.DC_PIECES_RECHANGE": "VIN"}
        cls.sampling = {"DATE_ECOM": cls.today.strftime("%d/%m/%y")}

    def test_build_download_query(self):
        """[infra] check query according to a statement."""
        query = query_builder.build_download_query(
            self.from_table,
            self.columns_to_select,
            self.join_tables,
            self.nb_days,
            "DATE_ECOM",
            "IN",
            self.sampling,
        )
        expected = "(SELECT P254G6_VEH_DC.CODE_VERSION, DC_PIECES_RECHANGE.CODE_PR, P254G6_VEH_DC.BOITE_DE_VITESSE FROM BRC07.P254G6_VEH_DC LEFT JOIN BRC07.DC_PIECES_RECHANGE ON BRC07.DC_PIECES_RECHANGE.VIN = BRC07.P254G6_VEH_DC.VIN  WHERE DATE_ECOM >= TO_DATE('01/02/19', 'dd/mm/yy') - 2)"
        self.assertEqual(query, expected)

    def test_build_from_statement(self):
        """[infra] build from a statement in two tables"""
        actual = query_builder.build_from_statement(self.from_table, self.join_tables)
        expected = "BRC07.P254G6_VEH_DC LEFT JOIN BRC07.DC_PIECES_RECHANGE ON BRC07.DC_PIECES_RECHANGE.VIN = BRC07.P254G6_VEH_DC.VIN "
        self.assertEqual(actual, expected)
