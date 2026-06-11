from airflow_provider_avito._version import __version__


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-avito",
        "name": "Avito",
        "description": "Airflow provider for Avito CPA — collect call statistics",
        "versions": [__version__],
        "integrations": [
            {
                "integration-name": "Avito",
                "external-doc-url": "https://developers.avito.ru/api-catalog/cpa/documentation",
                "tags": ["service"],
            },
        ],
        "operators": [
            {
                "integration-name": "Avito",
                "python-modules": ["airflow_provider_avito.operators.calls"],
            },
        ],
        "hooks": [
            {
                "integration-name": "Avito",
                "python-modules": ["airflow_provider_avito.hooks.avito"],
            },
        ],
    }
