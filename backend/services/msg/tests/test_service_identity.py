"""8B_00 骨架自检：服务身份与产品清单可导入。"""

from bms_msg import PRODUCT_KEY, SERVICE_NAME


def test_service_identity() -> None:
    assert SERVICE_NAME == "msg"
    assert PRODUCT_KEY == "msg"


def test_service_records_are_product_group() -> None:
    from services.module_registry import MSG_SERVICE_RECORDS

    assert MSG_SERVICE_RECORDS
    assert all(record.product_key == PRODUCT_KEY for record in MSG_SERVICE_RECORDS)
