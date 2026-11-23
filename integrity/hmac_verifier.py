import hashlib
import hmac
import json

from typing import Any, Mapping


SIGNATURE_FIELD = "hmac_sha256"


def canonical_event_bytes(event: Mapping[str, Any]) -> bytes:
    """
    Convert an event into a deterministic byte representation.

    The signature field is excluded so that an event can be
    verified using the same canonical representation used to sign it.
    """

    unsigned_event = {
        key: value
        for key, value in event.items()
        if key != SIGNATURE_FIELD
    }

    canonical_json = json.dumps(
        unsigned_event,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )

    return canonical_json.encode("utf-8")


def sign_event(
    event: Mapping[str, Any],
    secret: str,
) -> dict[str, Any]:
    """
    Return a copy of an event containing its HMAC-SHA256 signature.

    The secret must be shared securely between the signer
    and the verifier.
    """

    if not isinstance(secret, str) or not secret:
        raise ValueError("The HMAC secret must be a non-empty string.")

    signed_event = {
        key: value
        for key, value in event.items()
        if key != SIGNATURE_FIELD
    }

    message = canonical_event_bytes(signed_event)

    signature = hmac.new(
        secret.encode("utf-8"),
        message,
        hashlib.sha256,
    ).hexdigest()

    signed_event[SIGNATURE_FIELD] = signature

    return signed_event


def verify_event(
    event: Mapping[str, Any],
    secret: str,
) -> bool:
    """
    Verify an event's HMAC-SHA256 signature.

    Returns False if the signature is missing, incorrect,
    or the event cannot be canonically serialized.
    """

    if not isinstance(secret, str) or not secret:
        raise ValueError("The HMAC secret must be a non-empty string.")

    supplied_signature = event.get(SIGNATURE_FIELD)

    if not isinstance(supplied_signature, str):
        return False

    try:
        message = canonical_event_bytes(event)

        expected_signature = hmac.new(
            secret.encode("utf-8"),
            message,
            hashlib.sha256,
        ).hexdigest()

    except (TypeError, ValueError):
        return False

    return hmac.compare_digest(
        supplied_signature,
        expected_signature,
    )
