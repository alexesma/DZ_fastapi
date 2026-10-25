import pytest
from pydantic import ValidationError

from dz_fastapi.api.print_forms import PrintFormInput


def test_print_form_rejects_blocks_outside_label():
    with pytest.raises(ValidationError, match="правый край"):
        PrintFormInput(
            width_mm=58,
            height_mm=40,
            blocks=[{
                "id": "oem", "kind": "text", "field": "oem_number",
                "x": 54, "y": 1, "width": 10, "height": 5,
            }],
        )


def test_print_form_rejects_duplicate_block_ids():
    block = {
        "id": "oem", "kind": "text", "field": "oem_number",
        "x": 1, "y": 1, "width": 20, "height": 5,
    }
    with pytest.raises(ValidationError, match="уникальными"):
        PrintFormInput(width_mm=58, height_mm=40, blocks=[block, block])
