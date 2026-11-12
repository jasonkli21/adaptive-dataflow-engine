import unittest

from ade_client import QueryError, _rpc
from pyarrow.flight import FlightCancelledError


class FlightStatus(unittest.TestCase):
    def test_pyarrow_distinguishes_deadline_from_explicit_cancel(self):
        for message, code in (
            ("Flight cancelled call, with message: Timeout expired", "DEADLINE_EXCEEDED"),
            ("Flight cancelled call, with message: Cancelled", "CANCELLED"),
        ):
            def call():
                raise FlightCancelledError(message)

            with self.subTest(message=message), self.assertRaises(QueryError) as result:
                _rpc(call)
            self.assertEqual(result.exception.code, code)
