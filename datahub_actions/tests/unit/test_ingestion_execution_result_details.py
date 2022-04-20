from datahub_actions.events.transformers.get_ingestion_execution_result_details import (
    get_recipe_from_execution_input,
)


def test_ingestion_execution_result_details():

    assert get_recipe_from_execution_input(
        {
            "task": "RUN_INGEST",
            "arguments": [
                {
                    "key": "recipe",
                    "value": '{"source":{"type":"mysql","config":{"host_port":"HOST_PORT","database":"DATABASE","username":"USERNAME","password":"PASSWORD","include_tables":true,"include_views":true,"profiling":{"enabled":false}}},"sink":{"type":"datahub-rest","config":{"server":"https://HOST/gms","token":"TOKEN"}}}',
                },
                {"key": "version", "value": "0.8.26.3"},
            ],
        }
    ) == {
        "source": {
            "type": "mysql",
            "config": {
                "host_port": "HOST_PORT",
                "database": "DATABASE",
                "username": "USERNAME",
                "password": "PASSWORD",
                "include_tables": True,
                "include_views": True,
                "profiling": {"enabled": False},
            },
        },
        "sink": {
            "config": {"server": "https://HOST/gms", "token": "TOKEN"},
            "type": "datahub-rest",
        },
    }
