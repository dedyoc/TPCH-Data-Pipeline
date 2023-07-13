from dagster import materialize, build_op_context
from etl_pipeline.assets import my_first_asset


def test_my_first_asset():
    result = materialize(assets=[my_first_asset])
    assert result.success

    context = build_op_context()
    assert my_first_asset(context) == 1
