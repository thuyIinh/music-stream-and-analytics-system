from datetime import datetime

def start_run(cur, pipeline_name: str) -> int:
    """Tạo một bản ghi mới trong bảng etl_runs và trả về run_id."""
    cur.execute(
        """
        INSERT INTO etl_runs (pipeline_name, started_at, status)
        VALUES (%s, NOW(), 'running') RETURNING run_id
        """,
        (pipeline_name,)
    )
    return cur.fetchone()[0]

def log_detail(cur, run_id: int, stage: str, table_name: str, started_at: datetime,
               rows_in: int = 0, rows_out: int = 0, rows_error: int = 0,
               status: str = "success", message: str = None):
    """Ghi chi tiết quá trình xử lý của từng bảng vào etl_run_details."""
    cur.execute(
        """
        INSERT INTO etl_run_details (
            run_id, stage, table_name, started_at, finished_at, 
            rows_in, rows_out, rows_error, status, message
        )
        VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s)
        """,
        (run_id, stage, table_name, started_at, rows_in, rows_out, rows_error, status, message)
    )

def finish_run(cur, run_id: int, rows_loaded: int, status="success", error=None):
    """Cập nhật trạng thái hoàn thành cho toàn bộ pipeline trong etl_runs."""
    cur.execute(
        """
        UPDATE etl_runs
        SET finished_at = NOW(),
            duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at)),
            status = %s,
            rows_loaded = %s,
            error_message = %s
        WHERE run_id = %s
        """,
        (status, rows_loaded, error, run_id)
    )