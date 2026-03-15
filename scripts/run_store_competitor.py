#!/usr/bin/env python3
from build_ors_matrices import ORSMatrixBuilder

import os
from pathlib import Path

from dotenv import load_dotenv


def main() -> None:
    load_dotenv()
    api_key = os.getenv("ORS_API_KEY")
    if not api_key:
        raise SystemExit("Errore: imposta ORS_API_KEY nel file .env o nelle variabili d'ambiente")

    ORSMatrixBuilder(api_key=api_key, output_dir=Path("output")).build_store_competitor()


if __name__ == "__main__":
    main()
