import base64
import json
import os

if __name__ == "__main__":
    PATH = os.path.join("/root", ".basedosdados", "credentials", "prod.json")  # type: ignore

    BD_DBT_SERVICE_ACCOUNT = os.getenv("BD_DBT_SERVICE_ACCOUNT")

    if BD_DBT_SERVICE_ACCOUNT is None:
        raise Exception("BD_DBT_SERVICE_ACCOUNT not defined")

    if len(BD_DBT_SERVICE_ACCOUNT) == 0:
        raise Exception("BD_DBT_SERVICE_ACCOUNT env var is empty")

    with open(PATH, "w", encoding="utf-8") as f:
        json.dump(
            json.loads(
                base64.b64decode(BD_DBT_SERVICE_ACCOUNT).decode("utf-8")
            ),
            f,
            ensure_ascii=False,
        )
