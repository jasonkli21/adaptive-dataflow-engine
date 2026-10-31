"""Strict predicates for source-bound Phase17 review-remediation refusals."""


def check_refusal(label, case, trace, stdout):
    """Require a bounded refusal before source assignment and business output."""
    if case.get("exit") != 1:
        raise ValueError(f"missing refusal {label}")
    if not any(record.get("type") == "failure" for record in trace):
        raise ValueError(f"refusal has no structured failure {label}")
    if any(
        record.get("type") == "admitted" or record.get("point") == "source_assigned"
        for record in trace
    ):
        raise ValueError(f"refusal reached Kafka assignment {label}")
    if stdout:
        raise ValueError(f"refusal emitted business output {label}")
