# export_ui.py
import traceback

import dagster as dg


def run_export(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    try:
        # Import trong hàm để lỗi import cũng xuất hiện khi chạy step.
        from .export_customers import export_customer

        output_path, row_count = export_customer()

    except Exception:
        context.log.error(
            "Export CUSTOMER thất bại:\n"
            + traceback.format_exc()
        )
        raise  # Giữ nguyên exception và traceback gốc.

    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "csv_path": dg.MetadataValue.path(str(output_path)),
        }
    )
