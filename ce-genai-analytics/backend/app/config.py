import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # =========================
    # Database
    # =========================
    DB_DRIVER = os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}")
    DB_SERVER = os.getenv("DB_SERVER", "localhost\\sqlexpress")
    DB_NAME = os.getenv("DB_NAME", "CEAnalytics")
    DB_TRUSTED = os.getenv("DB_TRUSTED", "yes")

    # Optional (if you ever switch to SQL auth)
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    # =========================
    # OpenAI
    # =========================
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    def get_odbc_connection_string(self) -> str:
        # Windows Auth (Trusted Connection)
        if self.DB_TRUSTED.lower() == "yes":
            return (
                f"DRIVER={self.DB_DRIVER};"
                f"SERVER={self.DB_SERVER};"
                f"DATABASE={self.DB_NAME};"
                f"Trusted_Connection=yes;"
            )

        # SQL Authentication (optional)
        return (
            f"DRIVER={self.DB_DRIVER};"
            f"SERVER={self.DB_SERVER};"
            f"DATABASE={self.DB_NAME};"
            f"UID={self.DB_USER};"
            f"PWD={self.DB_PASSWORD};"
        )


settings = Settings()
