import unittest

from integrity.hmac_verifier import (
    sign_event,
    verify_event,
)


TEST_SECRET = "carbonhive-test-secret-2026"


class TestHMACIntegrity(unittest.TestCase):

    def setUp(self):
        self.event = {
            "event_id": "test-event-001",
            "equipment_id": "compressor-001",
            "electricity_kwh": 8.5,
            "temperature_c": 72.4,
            "is_synthetic": True,
        }

    def test_valid_signature_is_accepted(self):
        signed_event = sign_event(
            self.event,
            TEST_SECRET,
        )

        self.assertTrue(
            verify_event(signed_event, TEST_SECRET)
        )

    def test_modified_event_is_rejected(self):
        signed_event = sign_event(
            self.event,
            TEST_SECRET,
        )

        signed_event["electricity_kwh"] = 99.9

        self.assertFalse(
            verify_event(signed_event, TEST_SECRET)
        )

    def test_wrong_secret_is_rejected(self):
        signed_event = sign_event(
            self.event,
            TEST_SECRET,
        )

        self.assertFalse(
            verify_event(
                signed_event,
                "incorrect-test-secret",
            )
        )

    def test_missing_signature_is_rejected(self):
        self.assertFalse(
            verify_event(self.event, TEST_SECRET)
        )

    def test_signature_is_not_part_of_signed_payload(self):
        signed_event = sign_event(
            self.event,
            TEST_SECRET,
        )

        self.assertNotIn(
            "hmac_sha256",
            self.event,
        )

        self.assertIn(
            "hmac_sha256",
            signed_event,
        )

        self.assertTrue(
            verify_event(signed_event, TEST_SECRET)
        )

    def test_signing_does_not_modify_original_event(self):
        original_event = self.event.copy()

        sign_event(self.event, TEST_SECRET)

        self.assertEqual(
            self.event,
            original_event,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
