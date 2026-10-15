from typing import Literal, Optional

from pydantic import BaseModel, Field


class CustomerPricelistJobCreate(BaseModel):
    action: Literal["build", "send"] = "build"
    pricelist_id: Optional[int] = Field(default=None, gt=0)
